"""Pydantic schemas for Breaking pipeline API requests and responses."""

from datetime import date, datetime
from pydantic import BaseModel, Field, field_validator


# ── Known specialties (v7.0 — extended from 10 to 17) ──────────────

KNOWN_SPECIALTIES = {
    "Cardiology", "Nephrology", "Oncology", "Neurology", "Hepatology",
    "Pulmonology", "Endocrinology", "Gastroenterology", "General Medicine",
    "Rheumatology", "Dermatology", "Emergency Medicine", "Hematology",
    "Infectious Disease", "Ophthalmology", "Pediatrics", "Psychiatry",
}


# ── Request schemas ─────────────────────────────────────────────────

class BreakingPreferencesUpdate(BaseModel):
    specialties: list[str] = Field(..., min_length=1, max_length=1)


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


# ── v7.0 Topic schemas ─────────────────────────────────────────────

class TopicEntry(BaseModel):
    """One declared topic with its Gemini-generated queries."""
    topic_text: str
    generated_queries: list[str]


class TopicSaveRequest(BaseModel):
    """Request body for POST /api/breaking/topics.

    specialty_topics maps each specialty to a list of free-text topic strings.
    The doctor supplies only the text; Gemini generates the search queries.
    """
    specialty_topics: dict[str, list[str]] = Field(
        description="Map of specialty -> list of free-text topic strings (max 3 per specialty)"
    )

    @field_validator("specialty_topics")
    @classmethod
    def validate_topics(cls, v):
        for specialty, topics in v.items():
            if specialty not in KNOWN_SPECIALTIES:
                raise ValueError(f"Unknown specialty: {specialty}")
            if len(topics) > 3:
                raise ValueError(
                    f"{specialty}: maximum 3 topics allowed, got {len(topics)}"
                )
            for t in topics:
                if not (3 <= len(t) <= 200):
                    raise ValueError(
                        f"Topic '{t[:40]}...' must be 3-200 characters"
                    )
        return v


class TopicSaveResponse(BaseModel):
    """Response from POST /api/breaking/topics."""
    status: str
    specialty_topics: dict[str, list[TopicEntry]]
    queries_generated: int
    message: str


class TopicGetResponse(BaseModel):
    """Response from GET /api/breaking/topics."""
    specialty_topics: dict[str, list[TopicEntry]]
