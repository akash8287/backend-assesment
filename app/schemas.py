"""Request and response models. Unknown fields are rejected."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.constants import CLIENT_REF_PATTERN, USER_ID_PATTERN


class DocumentCreate(BaseModel):
    """Body for POST /documents. client_doc_ref is optional and unique when present."""

    model_config = ConfigDict(extra="forbid")

    user_id: str = Field(pattern=USER_ID_PATTERN)
    title: str = Field(min_length=1, max_length=200)
    content: str = Field(min_length=1, max_length=100_000)
    client_doc_ref: str | None = Field(default=None, pattern=CLIENT_REF_PATTERN)

    @field_validator("title")
    @classmethod
    def strip_title(cls, value: str) -> str:
        """Store the title without surrounding whitespace."""
        stripped = value.strip()
        if not stripped:
            raise ValueError("title must not be blank")
        return stripped

    @field_validator("content")
    @classmethod
    def content_not_blank(cls, value: str) -> str:
        """Reject whitespace-only content without changing the bytes that are hashed."""
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class DocumentPatch(BaseModel):
    """Body for PATCH. expected_content_version makes the update conditional."""

    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1, max_length=100_000)
    expected_content_version: int | None = Field(default=None, ge=1)

    @field_validator("content")
    @classmethod
    def content_not_blank(cls, value: str) -> str:
        """Reject whitespace-only content without changing the bytes that are hashed."""
        if not value.strip():
            raise ValueError("content must not be blank")
        return value


class StageView(BaseModel):
    """One pipeline stage as the caller is allowed to see it for the current version."""

    state: Literal["pending", "running", "succeeded", "failed"]
    attempts: int


class StagesView(BaseModel):
    """Processing and enriching progress for the current content version."""

    processing: StageView
    enriching: StageView


class SummaryView(BaseModel):
    """Summary stamped with the content version and hash it was built from."""

    text: str
    content_version: int
    content_hash: str


class TagsView(BaseModel):
    """Tags stamped with the content they came from and the summary version they used."""

    values: list[str]
    content_version: int
    content_hash: str
    summary_content_version: int


class DocumentResponse(BaseModel):
    """Public document. summary and tags are null when they do not match this content."""

    document_id: str
    user_id: str
    title: str
    content: str
    content_hash: str
    content_version: int
    client_doc_ref: str | None
    status: Literal["queued", "processing", "enriching", "completed", "failed"]
    failed_stage: Literal["processing", "enriching"] | None
    stages: StagesView
    summary: SummaryView | None
    tags: TagsView | None
    results_current: bool
    created_at: datetime
    updated_at: datetime


class DocumentListResponse(BaseModel):
    """One page of documents plus the total for the same filter."""

    items: list[DocumentResponse]
    page: int
    page_size: int
    total: int
