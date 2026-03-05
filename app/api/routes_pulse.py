"""Pulse API routes — preferences CRUD + digest retrieval."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.pulse import PulsePreference, PulseDigest, PulseArticle
from ..models.user import User
from ..pulse.schemas import (
    PulsePreferenceUpdate, PulsePreferenceResponse,
    PulseDigestSummary, PulseDigestDetail,
    JournalInfo, SpecialtyInfo,
)

router = APIRouter(prefix="/api/pulse", tags=["pulse"])


# ── Preferences ──────────────────────────────────────────────────

@router.get("/preferences", response_model=PulsePreferenceResponse | None)
async def get_preferences(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(PulsePreference).where(PulsePreference.user_id == user.id)
    )
    pref = result.scalar_one_or_none()
    return pref


@router.put("/preferences", response_model=PulsePreferenceResponse)
async def upsert_preferences(
    body: PulsePreferenceUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(PulsePreference).where(PulsePreference.user_id == user.id)
    )
    pref = result.scalar_one_or_none()

    if pref:
        pref.specialty = body.specialty
        pref.topics = body.topics
        pref.mesh_terms = body.mesh_terms
        pref.frequency = body.frequency
        pref.is_enabled = body.is_enabled
        pref.enabled_journals = body.enabled_journals
    else:
        pref = PulsePreference(
            user_id=user.id,
            specialty=body.specialty,
            topics=body.topics,
            mesh_terms=body.mesh_terms,
            frequency=body.frequency,
            is_enabled=body.is_enabled,
            enabled_journals=body.enabled_journals,
        )
        db.add(pref)

    await db.commit()
    await db.refresh(pref)
    return pref


# ── Digests ──────────────────────────────────────────────────────

@router.get("/digests", response_model=list[PulseDigestSummary])
async def list_digests(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    offset = (page - 1) * per_page
    result = await db.execute(
        select(PulseDigest)
        .where(PulseDigest.user_id == user.id)
        .order_by(desc(PulseDigest.created_at))
        .offset(offset)
        .limit(per_page)
    )
    return result.scalars().all()


@router.get("/digests/latest", response_model=PulseDigestDetail | None)
async def get_latest_digest(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(PulseDigest)
        .options(selectinload(PulseDigest.articles))
        .where(PulseDigest.user_id == user.id, PulseDigest.status == "complete")
        .order_by(desc(PulseDigest.created_at))
        .limit(1)
    )
    digest = result.scalar_one_or_none()
    return digest


@router.get("/digests/{digest_id}", response_model=PulseDigestDetail)
async def get_digest(
    digest_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    result = await db.execute(
        select(PulseDigest)
        .options(selectinload(PulseDigest.articles))
        .where(PulseDigest.id == digest_id, PulseDigest.user_id == user.id)
    )
    digest = result.scalar_one_or_none()
    if not digest:
        raise HTTPException(status_code=404, detail="Digest not found")
    return digest


@router.post("/digests/generate", status_code=202)
async def trigger_digest(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Manually trigger a Pulse digest generation (202 Accepted)."""
    if not settings.PULSE_ENABLED:
        raise HTTPException(status_code=503, detail="Pulse feature is currently disabled")

    # Check user has preferences
    result = await db.execute(
        select(PulsePreference).where(PulsePreference.user_id == user.id)
    )
    pref = result.scalar_one_or_none()
    if not pref:
        raise HTTPException(status_code=400, detail="Set your Pulse preferences first")

    from ..pulse.tasks import generate_pulse_digest
    generate_pulse_digest.apply_async(args=[str(user.id)], kwargs={"skip_cache": True})

    return {"status": "accepted", "message": "Digest generation started"}


# ── Reference data ───────────────────────────────────────────────

@router.get("/journals", response_model=list[JournalInfo])
async def list_journals(user: User = Depends(get_current_user)):
    from ..pulse.journal_registry import JOURNAL_REGISTRY
    return [
        JournalInfo(key=key, name=info["name"], strategy=info["strategy"])
        for key, info in JOURNAL_REGISTRY.items()
    ]


@router.get("/specialties", response_model=list[SpecialtyInfo])
async def list_specialties(user: User = Depends(get_current_user)):
    from ..pulse.scanner import SPECIALTY_MESH
    return [
        SpecialtyInfo(name=name, mesh_terms=terms)
        for name, terms in sorted(SPECIALTY_MESH.items())
    ]
