"""All MongoDB access for documents.

Every write made on behalf of a pipeline run is a compare-and-set filtered on
`content_version` (and, for workers, the lease token). A PATCH bumps
`content_version` and clears the lease in one atomic update, so any write
still in flight for the previous version matches zero documents and is dropped.
"""

from datetime import datetime, timedelta
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from app.domain import (
    ACTIVE_STATUSES,
    Stage,
    StageState,
    Status,
    fresh_stages,
    stage_info,
)

INDEXES = [
    # Listing, newest first (no status filter).
    IndexModel([("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)], name="user_created"),
    # Listing with a status filter, and the Mongo fallback count for the rate limiter.
    IndexModel(
        [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_status_created",
    ),
    # Crosswalk: GET /documents/by-ref/{ref}. Unique per owner; documents without a ref omit the field
    # entirely and are not indexed. ($exists rather than $type: the planner only proves an equality
    # predicate satisfies $exists, so a $type partial filter would never be selected.)
    IndexModel(
        [("user_id", ASCENDING), ("client_doc_ref", ASCENDING)],
        name="user_client_doc_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$exists": True}},
    ),
    # Content cache second tier: find an already-published result by content hash.
    IndexModel(
        [("result.content_hash", ASCENDING)],
        name="result_content_hash",
        partialFilterExpression={"result.content_hash": {"$exists": True}},
    ),
    # Worker claim query.
    IndexModel([("status", ASCENDING), ("next_run_at", ASCENDING)], name="claim"),
]


class DocumentRepository:
    def __init__(self, collection: AsyncCollection):
        self.col = collection

    async def ensure_indexes(self) -> None:
        await self.col.create_indexes(INDEXES)

    # ----- reads -----------------------------------------------------------

    async def get_owned(self, doc_id: ObjectId, user_id: str) -> dict[str, Any] | None:
        # Owner is part of the filter: another user's document and a missing one are indistinguishable.
        return await self.col.find_one({"_id": doc_id, "user_id": user_id})

    async def get_by_ref(self, user_id: str, client_doc_ref: str) -> dict[str, Any] | None:
        return await self.col.find_one({"user_id": user_id, "client_doc_ref": client_doc_ref})

    async def list_for_user(
        self, user_id: str, status: Status | None, skip: int, limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        query: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["status"] = status.value
        projection = {"content": 0, "draft": 0, "lease": 0}
        cursor = self.col.find(query, projection).sort([("created_at", DESCENDING), ("_id", DESCENDING)])
        items = await cursor.skip(skip).limit(limit).to_list(length=None)
        total = await self.col.count_documents(query)
        return items, total

    async def count_active(self, user_id: str, exclude_id: ObjectId | None = None) -> int:
        query: dict[str, Any] = {"user_id": user_id, "status": {"$in": [s.value for s in ACTIVE_STATUSES]}}
        if exclude_id is not None:
            query["_id"] = {"$ne": exclude_id}
        return await self.col.count_documents(query)

    async def find_result_by_hash(self, hash_: str) -> dict[str, Any] | None:
        doc = await self.col.find_one({"result.content_hash": hash_}, {"result": 1})
        return doc["result"] if doc else None

    # ----- API-side writes -------------------------------------------------

    async def insert(self, doc: dict[str, Any]) -> None:
        await self.col.insert_one(doc)

    async def replace_content(
        self,
        doc_id: ObjectId,
        user_id: str,
        expected_version: int,
        set_fields: dict[str, Any],
        extra_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """CAS on content_version. Increments it and applies `set_fields` (which reset pipeline state)."""
        return await self.col.find_one_and_update(
            {"_id": doc_id, "user_id": user_id, "content_version": expected_version, **(extra_filter or {})},
            {"$set": set_fields, "$inc": {"content_version": 1}},
            return_document=ReturnDocument.AFTER,
        )

    async def raise_ref_version(self, doc_id: ObjectId, ref_version: int, now: datetime) -> dict[str, Any] | None:
        return await self.col.find_one_and_update(
            {"_id": doc_id},
            {"$max": {"ref_version": ref_version}, "$set": {"updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )

    async def reset_failed_stage(
        self, doc_id: ObjectId, user_id: str, version: int, stage: Stage, now: datetime
    ) -> dict[str, Any] | None:
        """Manual retry: re-queue from the failed stage only, keeping earlier stage output."""
        new_status = Status.QUEUED if stage is Stage.PROCESSING else Status.ENRICHING
        return await self.col.find_one_and_update(
            {"_id": doc_id, "user_id": user_id, "content_version": version, "status": Status.FAILED.value},
            {
                "$set": {
                    "status": new_status.value,
                    f"stages.{stage.value}": stage_info(),
                    "lease": None,
                    "next_run_at": now,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )

    # ----- worker-side writes ---------------------------------------------

    async def claim_next(self, owner: str, token: str, now: datetime, lease_seconds: int) -> dict[str, Any] | None:
        """Atomically lease one runnable job. Two workers can never both win the same document."""
        return await self.col.find_one_and_update(
            {
                "status": {"$in": [s.value for s in ACTIVE_STATUSES]},
                "next_run_at": {"$lte": now},
                "$or": [{"lease": None}, {"lease.expires_at": {"$lt": now}}],
            },
            {"$set": {"lease": {"owner": owner, "token": token, "expires_at": now + timedelta(seconds=lease_seconds)}}},
            sort=[("next_run_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    def _fence(doc_id: ObjectId, version: int, token: str) -> dict[str, Any]:
        return {"_id": doc_id, "content_version": version, "lease.token": token}

    async def start_stage(
        self, doc_id: ObjectId, version: int, token: str, stage: Stage, now: datetime
    ) -> dict[str, Any] | None:
        status = Status.PROCESSING if stage is Stage.PROCESSING else Status.ENRICHING
        return await self.col.find_one_and_update(
            self._fence(doc_id, version, token),
            {
                "$set": {
                    "status": status.value,
                    f"stages.{stage.value}.state": StageState.RUNNING.value,
                    f"stages.{stage.value}.started_at": now,
                    f"stages.{stage.value}.error": None,
                    "updated_at": now,
                },
                "$inc": {f"stages.{stage.value}.attempts": 1},
            },
            return_document=ReturnDocument.AFTER,
        )

    async def complete_processing(
        self, doc_id: ObjectId, version: int, token: str, hash_: str, summary: str, now: datetime
    ) -> bool:
        res = await self.col.update_one(
            self._fence(doc_id, version, token),
            {
                "$set": {
                    "status": Status.ENRICHING.value,
                    "stages.processing.state": StageState.SUCCEEDED.value,
                    "stages.processing.finished_at": now,
                    "stages.enriching": stage_info(),
                    "draft": {"content_version": version, "content_hash": hash_, "summary": summary},
                    # Release the lease so enrichment can be picked up by any worker.
                    "lease": None,
                    "next_run_at": now,
                    "updated_at": now,
                }
            },
        )
        return res.modified_count == 1

    async def complete_enriching(
        self, doc_id: ObjectId, version: int, token: str, result: dict[str, Any], now: datetime
    ) -> bool:
        # `result` is replaced as one sub-document in the same write that marks the document completed,
        # and only if the draft it was derived from still belongs to this content_version.
        fence = self._fence(doc_id, version, token) | {"draft.content_version": version}
        res = await self.col.update_one(
            fence,
            {
                "$set": {
                    "status": Status.COMPLETED.value,
                    "stages.enriching.state": StageState.SUCCEEDED.value,
                    "stages.enriching.finished_at": now,
                    "result": result,
                    "lease": None,
                    "next_run_at": None,
                    "updated_at": now,
                }
            },
        )
        return res.modified_count == 1

    async def schedule_retry(
        self, doc_id: ObjectId, version: int, token: str, stage: Stage, error: str, run_at: datetime, now: datetime
    ) -> bool:
        res = await self.col.update_one(
            self._fence(doc_id, version, token),
            {
                "$set": {
                    f"stages.{stage.value}.state": StageState.RETRY_SCHEDULED.value,
                    f"stages.{stage.value}.error": error,
                    f"stages.{stage.value}.finished_at": now,
                    "lease": None,
                    "next_run_at": run_at,
                    "updated_at": now,
                }
            },
        )
        return res.modified_count == 1

    async def fail_stage(
        self, doc_id: ObjectId, version: int, token: str, stage: Stage, error: str, now: datetime
    ) -> bool:
        res = await self.col.update_one(
            self._fence(doc_id, version, token),
            {
                "$set": {
                    "status": Status.FAILED.value,
                    f"stages.{stage.value}.state": StageState.FAILED.value,
                    f"stages.{stage.value}.error": error,
                    f"stages.{stage.value}.finished_at": now,
                    "lease": None,
                    "next_run_at": None,
                    "updated_at": now,
                }
            },
        )
        return res.modified_count == 1


def reset_pipeline_fields(now: datetime) -> dict[str, Any]:
    """Fields a content change sets alongside the content_version increment."""
    return {
        "status": Status.QUEUED.value,
        "stages": fresh_stages(),
        "draft": None,
        "lease": None,
        "next_run_at": now,
        "updated_at": now,
    }
