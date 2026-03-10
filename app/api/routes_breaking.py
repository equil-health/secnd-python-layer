"""Breaking API routes — daily headlines feed, preferences, deep research trigger."""

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.breaking import BreakingHeadline, DoctorPreferences, BreakingRead
from ..models.user import User
from ..breaking.schemas import (
    BreakingPreferencesUpdate,
    BreakingFeedResponse,
    HeadlineResponse,
    TrialStatusResponse,
    DeepResearchResponse,
    PreferencesResponse,
)

router = APIRouter(prefix="/api/breaking", tags=["breaking"])


# ── GET /api/breaking/ — Today's headlines ──────────────────────────

@router.get("/", response_model=BreakingFeedResponse)
async def get_today_breaking(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return today's headlines for the doctor's specialties.

    Reads from PostgreSQL (Redis fallback is handled in sync layer).
    Applies semantic re-ranking at read time if doctor has topic history.
    """
    today = date.today()
    doctor_id = str(user.id)

    # Get doctor preferences
    result = await db.execute(
        select(DoctorPreferences).where(DoctorPreferences.doctor_id == user.id)
    )
    prefs = result.scalar_one_or_none()

    if not prefs or not prefs.specialties:
        return BreakingFeedResponse(
            date=str(today),
            headlines={},
            alert_count=0,
            trial_status=None,
        )

    specialties = prefs.specialties

    # Fetch headlines from DB
    result = await db.execute(
        select(BreakingHeadline)
        .where(
            BreakingHeadline.date == today,
            BreakingHeadline.specialty.in_(specialties),
        )
        .order_by(BreakingHeadline.rank_position)
    )
    rows = result.scalars().all()

    # Group by specialty
    headlines: dict[str, list[dict]] = {sp: [] for sp in specialties}
    for row in rows:
        headlines[row.specialty].append({
            "id": str(row.id),
            "date": row.date.isoformat(),
            "specialty": row.specialty,
            "title": row.title,
            "url": row.url,
            "source": row.source,
            "snippet": row.snippet,
            "urgency_tier": row.urgency_tier,
            "urgency_reason": row.urgency_reason,
            "rank_score": row.rank_score,
            "rank_position": row.rank_position,
            "research_topic": row.research_topic,
            "published_at": row.published_at,
            "is_verified": row.is_verified or False,
            "citation_count": row.citation_count,
            "quality_tier": row.quality_tier,
            "is_retracted": row.is_retracted or False,
        })

    alert_count = sum(
        1 for sp_headlines in headlines.values()
        for h in sp_headlines if h.get("urgency_tier") == "ALERT"
    )

    trial_status = _build_trial_status(prefs)

    return BreakingFeedResponse(
        date=str(today),
        headlines=headlines,
        alert_count=alert_count,
        trial_status=trial_status,
    )


# ── POST /api/breaking/preferences — Update specialties ────────────

@router.post("/preferences", response_model=PreferencesResponse)
async def update_preferences(
    body: BreakingPreferencesUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create or update doctor's specialty preferences."""
    from dateutil.relativedelta import relativedelta

    result = await db.execute(
        select(DoctorPreferences).where(DoctorPreferences.doctor_id == user.id)
    )
    prefs = result.scalar_one_or_none()

    now = datetime.now(timezone.utc)

    if prefs:
        prefs.specialties = body.specialties
        prefs.updated_at = now
    else:
        # First time — start trial
        prefs = DoctorPreferences(
            doctor_id=user.id,
            specialties=body.specialties,
            trial_started_at=now,
            trial_ends_at=now + relativedelta(months=3),
            free_reports_reset=date.today().replace(day=1) + relativedelta(months=1),
        )
        db.add(prefs)

    await db.commit()
    await db.refresh(prefs)

    return PreferencesResponse(
        doctor_id=str(prefs.doctor_id),
        specialties=prefs.specialties,
        breaking_enabled=prefs.breaking_enabled,
        trial_started_at=prefs.trial_started_at,
        trial_ends_at=prefs.trial_ends_at,
        free_reports_used=prefs.free_reports_used,
        free_reports_limit=prefs.free_reports_limit,
    )


# ── POST /api/breaking/{headline_id}/deep-research ─────────────────

@router.post("/{headline_id}/deep-research", response_model=DeepResearchResponse)
async def trigger_deep_research(
    headline_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """One-tap Deep Research trigger from Breaking headline.

    Checks trial gate (atomic) → dispatches research pipeline.
    """
    from uuid import UUID

    # Get headline
    result = await db.execute(
        select(BreakingHeadline).where(BreakingHeadline.id == UUID(headline_id))
    )
    headline = result.scalar_one_or_none()
    if not headline:
        raise HTTPException(status_code=404, detail="Headline not found")

    # Get doctor preferences for trial gate
    result = await db.execute(
        select(DoctorPreferences).where(DoctorPreferences.doctor_id == user.id)
    )
    prefs = result.scalar_one_or_none()
    if not prefs:
        raise HTTPException(status_code=400, detail="No preferences set. Complete onboarding first.")

    # Trial gate check
    allowed, gate_result = _check_trial_gate(prefs)

    if not allowed:
        return DeepResearchResponse(
            blocked=True,
            reason=gate_result.get("reason"),
            message=gate_result.get("message", "Report limit reached"),
            upgrade_options=gate_result.get("upgrade_options"),
        )

    # Increment usage (within same transaction)
    if prefs.subscription_tier is None:
        prefs.free_reports_used = (prefs.free_reports_used or 0) + 1

    # Record the deep research action
    read_record = BreakingRead(
        doctor_id=user.id,
        headline_id=headline.id,
        action="deep_research",
    )
    db.add(read_record)

    # Dispatch research pipeline
    from ..models.case import Case
    case = Case(
        presenting_complaint=headline.research_topic or headline.title,
        research_topic=headline.research_topic or headline.title,
        specialty=headline.specialty,
        pipeline_type="research",
        status="submitted",
        user_id=user.id,
    )
    db.add(case)
    await db.commit()
    await db.refresh(case)

    # Update read record with case_id
    read_record.case_id = case.id
    await db.commit()

    # Dispatch Celery pipeline (async)
    try:
        from ..pipeline.tasks import dispatch_research_pipeline
        dispatch_research_pipeline(str(case.id))
    except Exception as e:
        # Pipeline dispatch is best-effort — case is created either way
        import logging
        logging.getLogger(__name__).error(f"Pipeline dispatch failed: {e}")

    return DeepResearchResponse(
        case_id=str(case.id),
        reports_remaining=gate_result.get("reports_remaining"),
    )


# ── POST /api/auth/push-token — Register push token ────────────────

@router.post("/push-token")
async def register_push_token(
    body: dict,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Store FCM push token for a doctor."""
    token = body.get("token")
    platform = body.get("platform", "android")

    if not token:
        raise HTTPException(status_code=400, detail="Token required")

    result = await db.execute(
        select(DoctorPreferences).where(DoctorPreferences.doctor_id == user.id)
    )
    prefs = result.scalar_one_or_none()

    if prefs:
        prefs.push_token = token
        prefs.push_platform = platform
    else:
        prefs = DoctorPreferences(
            doctor_id=user.id,
            push_token=token,
            push_platform=platform,
            specialties=[],
        )
        db.add(prefs)

    await db.commit()
    return {"status": "ok"}


# ── Helpers ─────────────────────────────────────────────────────────

def _check_trial_gate(prefs: DoctorPreferences) -> tuple[bool, dict]:
    """Check if doctor can use a research report.

    Returns (allowed: bool, result: dict).
    """
    # Paid subscriber — always allowed
    if prefs.subscription_tier is not None:
        return True, {"tier": prefs.subscription_tier}

    now = datetime.now(timezone.utc)

    # Trial expired
    if prefs.trial_ends_at and now > prefs.trial_ends_at:
        return False, {
            "reason": "trial_expired",
            "message": "Your trial has expired.",
            "upgrade_options": _upgrade_options(),
        }

    # Monthly limit reached
    if (prefs.free_reports_used or 0) >= (prefs.free_reports_limit or 4):
        return False, {
            "reason": "monthly_limit_reached",
            "message": f"You've used all {prefs.free_reports_limit} free reports this month.",
            "reports_reset_date": str(prefs.free_reports_reset) if prefs.free_reports_reset else None,
            "upgrade_options": _upgrade_options(),
        }

    remaining = (prefs.free_reports_limit or 4) - (prefs.free_reports_used or 0) - 1
    return True, {"reports_remaining": remaining}


def _upgrade_options() -> list[dict]:
    """Return upgrade tier options."""
    return [
        {"tier": "pay_per_report", "label": "Pay \u20b9299 for this report", "action": "pay_per_report"},
        {"tier": "clinic_basic", "label": "\u20b91,999/month \u2014 10 reports", "action": "subscribe", "plan_id": "clinic_basic"},
        {"tier": "clinic_pro", "label": "\u20b94,999/month \u2014 30 reports", "action": "subscribe", "plan_id": "clinic_pro"},
    ]


def _build_trial_status(prefs: DoctorPreferences) -> TrialStatusResponse:
    """Build trial status from preferences."""
    return TrialStatusResponse(
        free_reports_used=prefs.free_reports_used or 0,
        limit=prefs.free_reports_limit or 4,
        trial_ends_at=prefs.trial_ends_at,
        reports_reset_date=prefs.free_reports_reset,
        tier=prefs.subscription_tier,
    )
