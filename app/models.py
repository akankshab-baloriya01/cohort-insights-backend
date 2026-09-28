"""Pydantic request/response models."""

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from app.domain import ResultSource, Stage, StageState, Status, failed_stage

USER_ID_PATTERN = r"^[A-Za-z0-9._@-]{1,64}$"
# No "/" so the ref is always a single path segment in /documents/by-ref/{ref}.
CLIENT_DOC_REF_PATTERN = r"^[A-Za-z0-9._:-]{1,128}$"
MAX_TITLE_CHARS = 300
MAX_CONTENT_CHARS = 100_000

UserId = Annotated[str, StringConstraints(pattern=USER_ID_PATTERN)]
ClientDocRef = Annotated[str, StringConstraints(pattern=CLIENT_DOC_REF_PATTERN)]
Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_TITLE_CHARS)]
Content = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=MAX_CONTENT_CHARS)]


def _reject_control_chars(value: str) -> str:
    if any(ord(c) < 32 and c not in "\n\r\t" for c in value):
        raise ValueError("must not contain control characters")
    return value


# ----- requests --------------------------------------------------------------


class DocumentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId
    title: Title
    content: Content
    client_doc_ref: ClientDocRef | None = None
    ref_version: int | None = Field(
        default=None,
        ge=0,
        description="Optional partner-side revision of client_doc_ref. When supplied, an older or conflicting "
        "revision of an already-known ref is rejected with 409 instead of overwriting newer content.",
    )

    _no_ctrl = field_validator("title", "content")(_reject_control_chars)

    @field_validator("ref_version")
    @classmethod
    def _ref_version_needs_ref(cls, value: int | None, info: Any) -> int | None:
        if value is not None and info.data.get("client_doc_ref") is None:
            raise ValueError("ref_version requires client_doc_ref")
        return value


class DocumentUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    content: Content
    expected_version: int | None = Field(
        default=None,
        ge=1,
        description="Optimistic concurrency: apply only if content_version still equals this, else 409.",
    )

    _no_ctrl = field_validator("content")(_reject_control_chars)


# ----- responses -------------------------------------------------------------


class SubmitOutcome(StrEnum):
    CREATED = "created"
    UNCHANGED = "unchanged"  # repeat of a known client_doc_ref with identical content
    NEW_VERSION = "new_version"  # repeat of a known client_doc_ref with different content


class SubmitResponse(BaseModel):
    document_id: str
    status: Status
    content_version: int
    outcome: SubmitOutcome
    served_from_cache: bool


class StageView(BaseModel):
    state: StageState
    attempts: int
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None


class ResultView(BaseModel):
    content_version: int = Field(description="The content_version this summary AND these tags were derived from.")
    content_hash: str
    is_current: bool = Field(description="content_version == document.content_version")
    summary: str
    tags: list[str]
    completed_at: datetime
    source: ResultSource


class DocumentView(BaseModel):
    document_id: str
    user_id: str
    title: str
    content: str
    content_version: int
    content_hash: str
    client_doc_ref: str | None
    ref_version: int | None
    status: Status
    failed_stage: Stage | None
    stages: dict[Stage, StageView]
    result: ResultView | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "DocumentView":
        return cls(
            document_id=str(doc["_id"]),
            user_id=doc["user_id"],
            title=doc["title"],
            content=doc["content"],
            content_version=doc["content_version"],
            content_hash=doc["content_hash"],
            client_doc_ref=doc.get("client_doc_ref"),
            ref_version=doc.get("ref_version"),
            status=doc["status"],
            failed_stage=failed_stage(doc),
            stages={Stage(k): StageView(**v) for k, v in doc["stages"].items()},
            result=_result_view(doc),
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
        )


class DocumentListItem(BaseModel):
    document_id: str
    title: str
    status: Status
    failed_stage: Stage | None
    content_version: int
    client_doc_ref: str | None
    result_is_current: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_doc(cls, doc: dict[str, Any]) -> "DocumentListItem":
        result = doc.get("result")
        return cls(
            document_id=str(doc["_id"]),
            title=doc["title"],
            status=doc["status"],
            failed_stage=failed_stage(doc),
            content_version=doc["content_version"],
            client_doc_ref=doc.get("client_doc_ref"),
            result_is_current=bool(result) and result["content_version"] == doc["content_version"],
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
        )


class DocumentPage(BaseModel):
    items: list[DocumentListItem]
    page: int
    page_size: int
    total: int
    has_next: bool


class HealthResponse(BaseModel):
    status: str
    checks: dict[str, str]


def _result_view(doc: dict[str, Any]) -> ResultView | None:
    result = doc.get("result")
    if not result:
        return None
    return ResultView(
        content_version=result["content_version"],
        content_hash=result["content_hash"],
        is_current=result["content_version"] == doc["content_version"],
        summary=result["summary"],
        tags=result["tags"],
        completed_at=result["completed_at"],
        source=result["source"],
    )
