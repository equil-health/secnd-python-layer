"""Research pipeline API route."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.case import Case
from ..models.report import PipelineRun
from ..models.user import User
from ..auth.security import get_current_user, check_report_limit
from ..models.schemas import ResearchSubmit, ResearchConfirm, CaseResponse

router = APIRouter(prefix="/api/research", tags=["research"])


def _validate_topic(topic: str, specialty: str = "") -> dict | None:
    """4-layer domain validation for research topics.

    Layer 1 — Static blocklist (fast, zero cost)
    Layer 2 — pgvector semantic fast-pass (sub-10ms)
    Layer 3 — Gemini classifier (only when L1 flags and L2 doesn't resolve)
    Layer 4 — Disambiguation payload (409 response)

    Returns ``None`` when the topic is safe to proceed.
    Otherwise returns a disambiguation payload (dict) for a 409 response.
    """
    from ..pipeline.domain_validator import (
        check_known_ambiguity,
        check_pgvector_fast_pass,
        validate_medical_domain,
    )

    # L1: Static blocklist — instant check
    ambiguity = check_known_ambiguity(topic)
    if ambiguity is None:
        return None  # no ambiguous terms found → proceed

    # L2: pgvector semantic fast-pass — check if topic is close to known
    # medical concepts (cheap, avoids Gemini call for clearly medical topics)
    if check_pgvector_fast_pass(topic):
        return None  # semantically close to a medical topic → proceed

    # L3: Gemini classifier — expensive but accurate
    classification = validate_medical_domain(topic, specialty)

    # High-confidence medical → let it through
    if classification.get("is_medical") and classification.get("confidence", 0) >= 0.8:
        return None

    # L4: Disambiguation UX — return payload for frontend
    return {
        "disambiguation_needed": True,
        "ambiguous_term": ambiguity["term"],
        "medical_meaning": ambiguity["medical_meaning"],
        "non_medical_meaning": ambiguity["non_medical_meaning"],
        "medical_interpretation": classification.get("medical_interpretation", ""),
        "non_medical_interpretation": classification.get("non_medical_interpretation", ""),
        "confidence": classification.get("confidence", 0),
        "reasoning": classification.get("reasoning", ""),
        "original_topic": topic,
    }


@router.post("", status_code=201, response_model=CaseResponse)
async def submit_research(
    body: ResearchSubmit,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """POST /api/research — Submit a research topic for STORM analysis.

    If specialty or research_intent is provided, dispatches the enhanced
    10-step v2 pipeline. Otherwise uses the original 4-step v1 pipeline.

    Returns 409 if the topic is ambiguous and needs user confirmation.
    """
    check_report_limit(user)

    # Domain validation gate
    disambiguation = _validate_topic(body.research_topic, body.specialty or "")
    if disambiguation is not None:
        return JSONResponse(status_code=409, content=disambiguation)

    use_v2 = bool(body.specialty or body.research_intent)

    case = Case(
        presenting_complaint=body.research_topic[:100],
        pipeline_type="research",
        research_topic=body.research_topic,
        raw_case_text=body.additional_context,
        specialty=body.specialty,
        research_intent=body.research_intent,
        status="processing",
        user_id=user.id,
    )
    db.add(case)
    await db.flush()

    total_steps = 10 if use_v2 else 4

    pipeline_run = PipelineRun(
        case_id=case.id,
        status="queued",
        total_steps=total_steps,
        steps=[
            {"step": 1, "label": "Research topic accepted", "status": "done"},
        ],
    )
    db.add(pipeline_run)
    await db.commit()
    await db.refresh(case)

    # Increment reports used
    if user.is_demo:
        user.reports_used = (user.reports_used or 0) + 1

    if use_v2:
        from ..pipeline.tasks import dispatch_research_pipeline_v2
        dispatch_research_pipeline_v2(str(case.id))
    else:
        from ..pipeline.tasks import dispatch_research_pipeline
        dispatch_research_pipeline(str(case.id))

    return case


@router.post("/confirm", status_code=201, response_model=CaseResponse)
async def confirm_research(
    body: ResearchConfirm,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """POST /api/research/confirm — Submit a disambiguated research topic.

    Called after the user confirms the medical interpretation from the
    disambiguation card.  Skips validation (user already confirmed).
    """
    check_report_limit(user)

    use_v2 = bool(body.specialty or body.research_intent)

    # Audit trail: record original topic before disambiguation
    audit_context = f"[Disambiguated from: {body.original_topic}]"

    case = Case(
        presenting_complaint=body.confirmed_topic[:100],
        pipeline_type="research",
        research_topic=body.confirmed_topic,
        raw_case_text=audit_context,
        specialty=body.specialty,
        research_intent=body.research_intent,
        status="processing",
        user_id=user.id,
    )
    db.add(case)
    await db.flush()

    total_steps = 10 if use_v2 else 4

    pipeline_run = PipelineRun(
        case_id=case.id,
        status="queued",
        total_steps=total_steps,
        steps=[
            {"step": 1, "label": "Research topic accepted", "status": "done"},
        ],
    )
    db.add(pipeline_run)
    await db.commit()
    await db.refresh(case)

    # Increment reports used
    if user.is_demo:
        user.reports_used = (user.reports_used or 0) + 1
        await db.commit()

    if use_v2:
        from ..pipeline.tasks import dispatch_research_pipeline_v2
        dispatch_research_pipeline_v2(str(case.id))
    else:
        from ..pipeline.tasks import dispatch_research_pipeline
        dispatch_research_pipeline(str(case.id))

    return case
