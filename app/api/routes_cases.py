"""Case API routes — per spec section 4."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.case import Case
from ..models.report import PipelineRun
from ..models.user import User
from ..auth.security import get_current_user, check_report_limit
from ..models.schemas import (
    CaseSubmitStructured,
    CaseSubmitFreeText,
    CaseResponse,
    PipelineStatus,
    PipelineStep,
    CaseListResponse,
    CaseListItem,
)

router = APIRouter(prefix="/api/cases", tags=["cases"])


@router.post("", status_code=201, response_model=CaseResponse)
async def submit_case(
    body: CaseSubmitStructured,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """POST /api/cases — Submit a structured case for analysis."""
    check_report_limit(user)

    mode = body.mode or "standard"
    case = Case(
        patient_age=body.patient_age,
        patient_sex=body.patient_sex,
        patient_ethnicity=body.patient_ethnicity,
        presenting_complaint=body.presenting_complaint,
        medical_history=body.medical_history,
        medications=body.medications,
        physical_exam=body.physical_exam,
        lab_results=[lab.model_dump() for lab in body.lab_results] if body.lab_results else None,
        imaging_reports=body.imaging_reports,
        referring_diagnosis=body.referring_diagnosis,
        specific_question=body.specific_question,
        diagnosis_mode=mode,
        status="processing",
        user_id=user.id,
    )
    db.add(case)
    await db.flush()

    # Create pipeline run
    pipeline_run = PipelineRun(
        case_id=case.id,
        status="queued",
        total_steps=12,
        steps=[
            {"step": 1, "label": "Case accepted", "status": "done"},
        ],
    )
    db.add(pipeline_run)
    await db.commit()
    await db.refresh(case)

    # Increment reports used
    if user.is_demo:
        user.reports_used = (user.reports_used or 0) + 1
        await db.commit()

    # Dispatch pipeline (Celery)
    from ..pipeline.tasks import dispatch_pipeline
    dispatch_pipeline(str(case.id))

    return case


@router.post("/parse")
async def parse_free_text(body: CaseSubmitFreeText, user: User = Depends(get_current_user)):
    """POST /api/cases/parse — Parse free-text case into structured fields.

    Uses Gemini to extract structured data from pasted clinical text.
    """
    from ..pipeline.gemini import call_gemini

    prompt = f"""Parse this clinical case into structured fields. Return JSON only:
{{
    "patient_age": number or null,
    "patient_sex": "male"/"female"/"other" or null,
    "patient_ethnicity": string or null,
    "presenting_complaint": string,
    "medical_history": string or null,
    "medications": string or null,
    "physical_exam": string or null,
    "lab_results": [{{"name": "AST", "value": 128, "unit": "U/L", "flag": "H"}}] or null,
    "imaging_reports": string or null,
    "referring_diagnosis": string or null,
    "specific_question": string or null
}}

CLINICAL TEXT:
{body.raw_text}"""

    import json
    import re

    raw = call_gemini(prompt, max_tokens=2048, temperature=0.1)
    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {"presenting_complaint": body.raw_text}

    return {
        "parsed": parsed,
        "confidence": 0.85,
        "unparsed_sections": [],
    }


@router.get("/{case_id}")
async def get_case(case_id: UUID, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    """GET /api/cases/{id} — Get case with pipeline status."""
    result = await db.execute(select(Case).where(Case.id == case_id))
    case = result.scalar_one_or_none()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    result = await db.execute(
        select(PipelineRun).where(PipelineRun.case_id == case_id)
    )
    pipeline_run = result.scalar_one_or_none()

    pipeline_status = None
    if pipeline_run:
        steps = [PipelineStep(**s) for s in (pipeline_run.steps or [])]
        pipeline_status = PipelineStatus(
            case_id=case.id,
            status=pipeline_run.status,
            current_step=pipeline_run.current_step,
            total_steps=pipeline_run.total_steps,
            steps=steps,
            started_at=pipeline_run.started_at,
            completed_at=pipeline_run.completed_at,
        )

    return {
        "case": CaseResponse.model_validate(case),
        "pipeline": pipeline_status,
    }


@router.get("", response_model=CaseListResponse)
async def list_cases(
    page: int = 1,
    per_page: int = 20,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """GET /api/cases — List cases with pagination. Users see own; admins see all."""
    offset = (page - 1) * per_page

    base_filter = True if user.role == "admin" else (Case.user_id == user.id)

    # Total count
    count_result = await db.execute(select(func.count(Case.id)).where(base_filter))
    total = count_result.scalar()

    # Fetch page
    result = await db.execute(
        select(Case)
        .where(base_filter)
        .order_by(Case.created_at.desc())
        .offset(offset)
        .limit(per_page)
    )
    cases = result.scalars().all()

    return CaseListResponse(
        cases=[
            CaseListItem(
                id=c.id,
                status=c.status,
                diagnosis_mode=c.diagnosis_mode or "standard",
                presenting_complaint=(c.presenting_complaint or "")[:100],
                primary_diagnosis=None,
                created_at=c.created_at,
            )
            for c in cases
        ],
        total=total,
        page=page,
        per_page=per_page,
    )
