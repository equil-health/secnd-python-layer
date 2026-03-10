"""Pydantic schemas for Breaking pipeline API requests and responses."""

from datetime import date, datetime
from pydantic import BaseModel, Field


# ── Request schemas ─────────────────────────────────────────────────

class BreakingPreferencesUpdate(BaseModel):
    specialties: list[str] = Field(..., min_length=1, max_length=3)


# ── Response schemas ────────────────────────────────────────────────

class HeadlineResponse(BaseModel):
    id: str
    date: date
    specialty: str
    title: str
    url: str
    source: str | None = None
    snippet: str | None = None
    urgency_tier: str = "NEW"
    urgency_reason: str | None = None
    rank_score: int = 50
    rank_position: int = 0
    research_topic: str | None = None
    published_at: str | None = None
    # OpenAlex fields
    is_verified: bool = False
    citation_count: int | None = None
    quality_tier: str | None = None
    is_retracted: bool = False

    model_config = {"from_attributes": True}


class TrialStatusResponse(BaseModel):
    free_reports_used: int = 0
    limit: int = 4
    trial_ends_at: datetime | None = None
    reports_reset_date: date | None = None
    tier: str | None = None  # None = trial, else subscription tier


class BreakingFeedResponse(BaseModel):
    date: str
    headlines: dict[str, list[HeadlineResponse]]  # {specialty: [headline, ...]}
    alert_count: int = 0
    trial_status: TrialStatusResponse | None = None


class DeepResearchResponse(BaseModel):
    case_id: str | None = None
    blocked: bool = False
    reason: str | None = None
    message: str | None = None
    reports_remaining: int | None = None
    upgrade_options: list[dict] | None = None


class PreferencesResponse(BaseModel):
    doctor_id: str
    specialties: list[str]
    breaking_enabled: bool = True
    trial_started_at: datetime | None = None
    trial_ends_at: datetime | None = None
    free_reports_used: int = 0
    free_reports_limit: int = 4

    model_config = {"from_attributes": True}
