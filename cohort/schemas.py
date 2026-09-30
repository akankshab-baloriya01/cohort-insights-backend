from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

DocumentStatus = Literal["queued", "processing", "enriching", "completed", "failed"]
StageState = Literal["pending", "running", "completed", "failed"]


class CreateDocument(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1, max_length=100_000)
    client_doc_ref: Optional[str] = Field(default=None, min_length=1, max_length=128)

    @field_validator("user_id", "title", "content")
    @classmethod
    def not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class UpdateDocument(BaseModel):
    content: str = Field(min_length=1, max_length=100_000)
    expected_version: Optional[int] = Field(default=None, ge=1)

    @field_validator("content")
    @classmethod
    def not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class CreateDocumentResponse(BaseModel):
    document_id: str
    status: str


class Stage(BaseModel):
    state: StageState
    content_version: Optional[int] = None
    attempts: int = 0
    error: Optional[str] = None


class ProcessingStage(Stage):
    summary: Optional[str] = None


class EnrichingStage(Stage):
    tags: Optional[list[str]] = None


class Stages(BaseModel):
    processing: ProcessingStage
    enriching: EnrichingStage


class ListDocumentResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    content: str
    client_doc_ref: Optional[str] = None
    status: DocumentStatus
    content_version: int
    stages: Stages
    summary: Optional[str] = None
    tags: Optional[list[str]] = None
    created_at: datetime
    updated_at: datetime
