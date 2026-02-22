"""Research pipeline API route."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.case import Case
from ..models.report import PipelineRun
from ..models.schemas import ResearchSubmit, CaseResponse

router = APIRouter(prefix="/api/research", tags=["research"])


@router.post("", status_code=201, response_model=CaseResponse)
async def submit_research(body: ResearchSubmit, db: AsyncSession = Depends(get_db)):
    """POST /api/research — Submit a research topic for STORM analysis."""
    case = Case(
        presenting_complaint=body.research_topic[:100],
        pipeline_type="research",
        research_topic=body.research_topic,
        raw_case_text=body.additional_context,
        status="processing",
    )
    db.add(case)
    await db.flush()

    pipeline_run = PipelineRun(
        case_id=case.id,
        status="queued",
        total_steps=4,
        steps=[
            {"step": 1, "label": "Research topic accepted", "status": "done"},
        ],
    )
    db.add(pipeline_run)
    await db.commit()
    await db.refresh(case)

    from ..pipeline.tasks import dispatch_research_pipeline
    dispatch_research_pipeline(str(case.id))

    return case
