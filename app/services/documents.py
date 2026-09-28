"""Document use-cases: submit, update, read, list, crosswalk lookup, retry."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bson import ObjectId
from pymongo.errors import DuplicateKeyError, PyMongoError

from app.config import Settings
from app.domain import (
    ACTIVE_STATUSES,
    ResultSource,
    Status,
    build_result,
    content_hash,
    failed_stage,
    fresh_stages,
    skipped_stages,
    utcnow,
)
from app.errors import Conflict, NotFound, RateLimited
from app.models import DocumentCreate, SubmitOutcome
from app.repository import DocumentRepository, reset_pipeline_fields
from app.services.content_cache import CachedResult, ContentCache
from app.services.locks import LockState, RedisLock
from app.services.rate_limiter import Acquire, ActiveJobLimiter

logger = logging.getLogger(__name__)

# Bound on CAS retries when concurrent writers keep bumping content_version.
_MAX_CAS_ATTEMPTS = 5
# How long a same-ref submission waits for a concurrent creator (~ the lock TTL).
_REF_WAIT_SECONDS = 0.05
_MAX_REF_WAIT_ROUNDS = 100


@dataclass
class SubmitResult:
    doc: dict[str, Any]
    outcome: SubmitOutcome
    served_from_cache: bool


@dataclass
class _ContentChange:
    doc: dict[str, Any]
    changed: bool
    served_from_cache: bool


class DocumentService:
    def __init__(
        self,
        repo: DocumentRepository,
        limiter: ActiveJobLimiter,
        cache: ContentCache,
        locks: RedisLock,
        settings: Settings,
    ):
        self.repo = repo
        self.limiter = limiter
        self.cache = cache
        self.locks = locks
        self.settings = settings

    # ----- reads -----------------------------------------------------------

    async def get(self, doc_id: str, user_id: str) -> dict[str, Any]:
        oid = _parse_object_id(doc_id)
        doc = await self.repo.get_owned(oid, user_id) if oid else None
        if doc is None:
            raise NotFound("document not found")
        return doc

    async def get_by_ref(self, client_doc_ref: str, user_id: str) -> dict[str, Any]:
        doc = await self.repo.get_by_ref(user_id, client_doc_ref)
        if doc is None:
            raise NotFound("document not found")
        return doc

    async def list(
        self, user_id: str, status: Status | None, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int]:
        return await self.repo.list_for_user(user_id, status, (page - 1) * page_size, page_size)

    # ----- submit ----------------------------------------------------------

    async def submit(self, data: DocumentCreate) -> SubmitResult:
        hash_ = content_hash(data.content)
        if data.client_doc_ref is None:
            return await self._create(data, hash_)

        # Same-ref submissions may arrive concurrently (partner retries). Creation is serialized per
        # (user, ref) so that only one request takes a rate-limit slot and inserts; the others wait,
        # then find the document and take the repeat-submission path. The unique index remains the
        # final arbiter if Redis is unavailable.
        lock_key = f"lock:ref:{data.user_id}:{data.client_doc_ref}"
        for _ in range(_MAX_REF_WAIT_ROUNDS):
            existing = await self.repo.get_by_ref(data.user_id, data.client_doc_ref)
            if existing is not None:
                return await self._resubmit(existing, data, hash_)
            state, token = await self.locks.acquire(lock_key)
            if state is LockState.HELD_ELSEWHERE:
                await asyncio.sleep(_REF_WAIT_SECONDS)
                continue
            try:
                return await self._create(data, hash_)
            except DuplicateKeyError:
                logger.info("concurrent submission for same ref", extra={"client_doc_ref": data.client_doc_ref})
            finally:
                if state is LockState.ACQUIRED:
                    await self.locks.release(lock_key, token)
        raise Conflict("too many concurrent submissions for this client_doc_ref", code="contention")

    async def _create(self, data: DocumentCreate, hash_: str) -> SubmitResult:
        now = utcnow()
        doc_id = ObjectId()
        doc: dict[str, Any] = {
            "_id": doc_id,
            "user_id": data.user_id,
            "title": data.title,
            "content": data.content,
            "content_hash": hash_,
            "content_version": 1,
            "draft": None,
            "result": None,
            "lease": None,
            "created_at": now,
            "updated_at": now,
        }
        if data.client_doc_ref is not None:
            doc["client_doc_ref"] = data.client_doc_ref
            doc["ref_version"] = data.ref_version

        cached = await self.cache.lookup(hash_)
        if cached is not None:
            doc |= _completed_from_cache(cached, version=1, now=now)
            await self.repo.insert(doc)
            logger.info("document served from content cache", extra={"document_id": str(doc_id)})
            return SubmitResult(doc, SubmitOutcome.CREATED, served_from_cache=True)

        slot = await self.limiter.acquire(data.user_id, doc_id, 1)
        if slot is Acquire.DENIED:
            raise RateLimited(f"user already has {self.settings.max_active_docs_per_user} documents in the pipeline")
        doc |= {"status": Status.QUEUED.value, "stages": fresh_stages(), "next_run_at": now}
        try:
            await self.repo.insert(doc)
        except PyMongoError:
            if slot is Acquire.ADDED:
                await self.limiter.release(data.user_id, doc_id, 1)
            raise
        logger.info("document queued", extra={"document_id": str(doc_id), "user_id": data.user_id})
        return SubmitResult(doc, SubmitOutcome.CREATED, served_from_cache=False)

    async def _resubmit(self, existing: dict[str, Any], data: DocumentCreate, hash_: str) -> SubmitResult:
        """Repeat submission of a known client_doc_ref (see README "Crosswalk")."""
        _check_ref_version(existing, data.ref_version, hash_)
        if hash_ == existing["content_hash"]:
            doc = existing
            stored_ref_version = existing.get("ref_version")
            if data.ref_version is not None and (stored_ref_version is None or stored_ref_version < data.ref_version):
                doc = await self.repo.raise_ref_version(existing["_id"], data.ref_version, utcnow()) or existing
            return SubmitResult(doc, SubmitOutcome.UNCHANGED, served_from_cache=False)

        change = await self._change_content(
            existing, data.content, hash_, title=data.title, ref_version=data.ref_version
        )
        outcome = SubmitOutcome.NEW_VERSION if change.changed else SubmitOutcome.UNCHANGED
        return SubmitResult(change.doc, outcome, change.served_from_cache)

    # ----- update ----------------------------------------------------------

    async def update_content(
        self, doc_id: str, user_id: str, content: str, expected_version: int | None
    ) -> dict[str, Any]:
        doc = await self.get(doc_id, user_id)
        change = await self._change_content(doc, content, content_hash(content), expected_version=expected_version)
        return change.doc

    async def _change_content(
        self,
        doc: dict[str, Any],
        content: str,
        hash_: str,
        *,
        title: str | None = None,
        ref_version: int | None = None,
        expected_version: int | None = None,
    ) -> _ContentChange:
        """Replace content in place: bump content_version and restart the pipeline, atomically.

        The write is a compare-and-set on the content_version we read. Losing the race means
        someone else changed the content first: with `expected_version` that is a 409, otherwise
        we re-read and re-apply (last writer wins, but every writer's version is well defined).
        """
        user_id, doc_id = doc["user_id"], doc["_id"]
        for _ in range(_MAX_CAS_ATTEMPTS):
            if expected_version is not None and doc["content_version"] != expected_version:
                raise Conflict(
                    f"content_version is {doc['content_version']}, expected {expected_version}",
                    code="version_conflict",
                )
            if ref_version is not None:
                _check_ref_version(doc, ref_version, hash_)
            if hash_ == doc["content_hash"]:
                return _ContentChange(doc, changed=False, served_from_cache=False)

            now = utcnow()
            version = doc["content_version"]
            new_version = version + 1
            was_active = doc["status"] in ACTIVE_STATUSES
            fields = reset_pipeline_fields(now) | {"content": content, "content_hash": hash_}
            if title is not None:
                fields["title"] = title
            extra_filter = None
            if ref_version is not None:
                fields["ref_version"] = ref_version
                extra_filter = {"$or": [{"ref_version": None}, {"ref_version": {"$lt": ref_version}}]}

            cached = await self.cache.lookup(hash_)
            slot: Acquire | None = None
            if cached is not None:
                fields |= _completed_from_cache(cached, version=new_version, now=now)
            else:
                # An active run hands its slot to the new version; an idle document needs a fresh one.
                slot = await self.limiter.acquire(
                    user_id, doc_id, new_version, prev_version=version if was_active else None
                )
                if slot is Acquire.DENIED:
                    raise RateLimited(
                        f"user already has {self.settings.max_active_docs_per_user} documents in the pipeline"
                    )

            updated = await self.repo.replace_content(doc_id, user_id, version, fields, extra_filter)
            if updated is not None:
                if cached is not None and was_active:
                    await self.limiter.release(user_id, doc_id, version)
                logger.info(
                    "document content replaced",
                    extra={"document_id": str(doc_id), "content_version": new_version, "from_cache": bool(cached)},
                )
                return _ContentChange(updated, changed=True, served_from_cache=cached is not None)

            # Lost the CAS. Re-read; give back a slot we took unless the winner now owns that same member.
            fresh = await self.repo.get_owned(doc_id, user_id)
            if slot is Acquire.ADDED and (fresh is None or fresh["content_version"] != new_version):
                await self.limiter.release(user_id, doc_id, new_version)
            if fresh is None:
                raise NotFound("document not found")
            doc = fresh
        raise Conflict("document is being modified concurrently, retry", code="contention")

    # ----- retry -----------------------------------------------------------

    async def retry(self, doc_id: str, user_id: str) -> dict[str, Any]:
        doc = await self.get(doc_id, user_id)
        stage = failed_stage(doc)
        if stage is None:
            raise Conflict("only failed documents can be retried", code="not_failed")
        version = doc["content_version"]
        slot = await self.limiter.acquire(user_id, doc["_id"], version)
        if slot is Acquire.DENIED:
            raise RateLimited(f"user already has {self.settings.max_active_docs_per_user} documents in the pipeline")
        updated = await self.repo.reset_failed_stage(doc["_id"], user_id, version, stage, utcnow())
        if updated is None:
            if slot is Acquire.ADDED:
                await self.limiter.release(user_id, doc["_id"], version)
            raise Conflict("document changed while retrying, re-read it", code="not_failed")
        logger.info("document retry requested", extra={"document_id": doc_id, "stage": stage.value})
        return updated


def _parse_object_id(value: str) -> ObjectId | None:
    # Malformed ids are reported exactly like missing ones.
    return ObjectId(value) if ObjectId.is_valid(value) else None


def _check_ref_version(doc: dict[str, Any], ref_version: int | None, hash_: str) -> None:
    stored = doc.get("ref_version")
    if ref_version is None or stored is None:
        return
    if ref_version < stored:
        raise Conflict(f"ref_version {ref_version} is older than stored ref_version {stored}", code="stale_ref_version")
    if ref_version == stored and hash_ != doc["content_hash"]:
        raise Conflict(
            f"ref_version {ref_version} already recorded with different content", code="ref_version_conflict"
        )


def _completed_from_cache(cached: CachedResult, *, version: int, now: datetime) -> dict[str, Any]:
    return {
        "status": Status.COMPLETED.value,
        "stages": skipped_stages(),
        "next_run_at": None,
        "result": build_result(
            version=version,
            hash_=cached.content_hash,
            summary=cached.summary,
            tags=cached.tags,
            source=ResultSource.CACHE,
            now=now,
        ),
    }
