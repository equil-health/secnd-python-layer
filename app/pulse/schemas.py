"""Pydantic schemas for Pulse API."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


# ── Preferences ──────────────────────────────────────────────────

class PulsePreferenceUpdate(BaseModel):
    specialty: str = Field(..., max_length=100)
    topics: list[str] = Field(default_factory=list)
    mesh_terms: list[str] | None = None
    frequency: str = Field(default="weekly", pattern="^(daily|weekly)$")
    is_enabled: bool = True
    enabled_journals: list[str] | None = None


class PulsePreferenceResponse(BaseModel):
    id: UUID
    user_id: UUID
    specialty: str
    topics: list[str]
    mesh_terms: list[str] | None
    frequency: str
    is_enabled: bool
    enabled_journals: list[str] | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── Articles ─────────────────────────────────────────────────────

class PulseArticleResponse(BaseModel):
    id: UUID
    title: str
    authors: list[str] | None
    journal: str | None
    doi: str | None
    pmid: str | None
    published_date: datetime | None
    abstract: str | None
    article_url: str | None
    tldr: str | None
    evidence_grade: str | None
    relevance_score: float | None
    source: str | None

    model_config = {"from_attributes": True}


# ── Digests ──────────────────────────────────────────────────────

class PulseDigestSummary(BaseModel):
    id: UUID
    status: str
    article_count: int
    specialty_used: str | None
    topics_used: list[str] | None
    date_range_start: datetime | None
    date_range_end: datetime | None
    generated_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


class PulseDigestDetail(PulseDigestSummary):
    error_message: str | None
    articles: list[PulseArticleResponse]


# ── Reference data ───────────────────────────────────────────────

class JournalInfo(BaseModel):
    key: str
    name: str
    strategy: str


class SpecialtyInfo(BaseModel):
    name: str
    mesh_terms: list[str]
