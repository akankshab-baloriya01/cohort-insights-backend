"""Domain vocabulary and pure helpers shared by the API and the worker.

Document shape in MongoDB (collection `documents`):

    _id              ObjectId
    user_id          str                  owner; every read is filtered by it
    title            str
    content          str                  current content
    content_hash     str                  sha256(content)
    content_version  int                  1 on insert, +1 on every content change (the fencing token)
    client_doc_ref   str | absent         partner reference, unique per user when present
    ref_version      int | absent         partner-supplied ordering for repeat submissions
    status           Status               top-level pipeline position
    stages           {processing: StageInfo, enriching: StageInfo}   always about content_version
    draft            {content_version, content_hash, summary} | null  stage-1 output of the current run
    result           {content_version, content_hash, summary, tags, completed_at, source} | null
                     the last *published* result; written only as one whole sub-document
    lease            {owner, token, expires_at} | null   worker claim
    next_run_at      datetime | null      when a worker may pick the job up (retry backoff)
    created_at, updated_at
"""

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


ACTIVE_STATUSES = (Status.QUEUED, Status.PROCESSING, Status.ENRICHING)


class Stage(StrEnum):
    PROCESSING = "processing"
    ENRICHING = "enriching"


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"  # result served from the content cache


class ResultSource(StrEnum):
    PIPELINE = "pipeline"
    CACHE = "cache"


def utcnow() -> datetime:
    return datetime.now(UTC)


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def stage_info(state: StageState = StageState.PENDING) -> dict[str, Any]:
    return {"state": state.value, "attempts": 0, "started_at": None, "finished_at": None, "error": None}


def fresh_stages() -> dict[str, Any]:
    return {Stage.PROCESSING.value: stage_info(), Stage.ENRICHING.value: stage_info()}


def skipped_stages() -> dict[str, Any]:
    return {
        Stage.PROCESSING.value: stage_info(StageState.SKIPPED),
        Stage.ENRICHING.value: stage_info(StageState.SKIPPED),
    }


def build_result(
    *, version: int, hash_: str, summary: str, tags: list[str], source: ResultSource, now: datetime
) -> dict[str, Any]:
    """The only constructor for `result`. It is always written as a whole, never field by field."""
    return {
        "content_version": version,
        "content_hash": hash_,
        "summary": summary,
        "tags": tags,
        "completed_at": now,
        "source": source.value,
    }


def failed_stage(doc: dict[str, Any]) -> Stage | None:
    if doc["status"] != Status.FAILED:
        return None
    for stage in Stage:
        if doc["stages"][stage.value]["state"] == StageState.FAILED:
            return stage
    return None
