"""Report API routes — per spec section 4.4-4.6."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db.database import get_db
from ..models.report import Report, FollowUp
from ..models.case import Case
from ..models.schemas import (
    ReportResponse,
    EvidenceClaim,
    Reference,
    FollowUpRequest,
    FollowUpResponse,
)

router = APIRouter(prefix="/api/cases", tags=["reports"])


@router.get("/{case_id}/report", response_model=ReportResponse)
async def get_report(case_id: UUID, db: AsyncSession = Depends(get_db)):
    """GET /api/cases/{id}/report — Get the compiled report."""
    result = await db.execute(
        select(Report).where(Report.case_id == case_id)
    )
    report = result.scalar_one_or_none()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    # Look up the case to check pipeline type
    case_result = await db.execute(
        select(Case).where(Case.id == case_id)
    )
    case = case_result.scalar_one_or_none()
    pipeline_type = case.pipeline_type if case else "diagnosis"
    diagnosis_mode = case.diagnosis_mode if case else "standard"

    if pipeline_type == "research":
        # Research pipeline: no MedGemma, no evidence claims
        references = [Reference(**r) for r in (report.references or [])]
        return ReportResponse(
            case_id=report.case_id,
            pipeline_type="research",
            diagnosis_mode="standard",
            research_topic=case.research_topic if case else None,
            executive_summary=report.executive_summary,
            medgemma_analysis=None,
            evidence_claims=[],
            storm_article=report.storm_article_clean,
            references=references,
            primary_diagnosis=None,
            total_sources=report.total_sources or 0,
            hallucination_issues=0,
            report_html=report.report_html,
            pdf_url=f"/api/cases/{case_id}/report/pdf" if report.report_markdown else None,
            docx_url=f"/api/cases/{case_id}/report/docx" if report.report_markdown else None,
            created_at=report.created_at,
        )

    # Diagnosis pipeline (original logic)
    evidence_claims = []
    for claim_data in (report.evidence_results or []):
        evidence_claims.append(EvidenceClaim(
            claim=claim_data.get("claim", ""),
            verdict=claim_data.get("verdict", "UNKNOWN"),
            evidence=claim_data.get("evidence", ""),
            references=claim_data.get("references", []),
        ))

    references = [Reference(**r) for r in (report.references or [])]

    hallucination_issues = 0
    if report.hallucination_check:
        hallucination_issues = len(report.hallucination_check.get("issues", []))

    return ReportResponse(
        case_id=report.case_id,
        pipeline_type="diagnosis",
        diagnosis_mode=diagnosis_mode or "standard",
        executive_summary=report.executive_summary,
        medgemma_analysis=report.medgemma_clean or "",
        evidence_claims=evidence_claims,
        storm_article=report.storm_article_clean,
        references=references,
        primary_diagnosis=report.primary_diagnosis,
        total_sources=report.total_sources or 0,
        hallucination_issues=hallucination_issues,
        verification_stats=report.verification_stats,
        report_html=report.report_html,
        pdf_url=f"/api/cases/{case_id}/report/pdf" if report.report_markdown else None,
        docx_url=f"/api/cases/{case_id}/report/docx" if report.report_markdown else None,
        created_at=report.created_at,
    )


@router.get("/{case_id}/report/html")
async def get_report_html(case_id: UUID, db: AsyncSession = Depends(get_db)):
    """GET /api/cases/{id}/report/html — Styled standalone HTML."""
    result = await db.execute(
        select(Report).where(Report.case_id == case_id)
    )
    report = result.scalar_one_or_none()
    if not report or not report.report_html:
        raise HTTPException(status_code=404, detail="Report not found")

    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>Second Opinion Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 900px; margin: 0 auto; padding: 2rem; line-height: 1.6; }}
h1 {{ color: #1a365d; }} h2 {{ color: #2d3748; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.5rem; }}
blockquote {{ border-left: 4px solid #e53e3e; padding: 1rem; background: #fff5f5; }}
</style>
</head><body>{report.report_html}</body></html>"""

    return Response(content=html, media_type="text/html")


@router.get("/{case_id}/report/pdf")
async def get_report_pdf(case_id: UUID, db: AsyncSession = Depends(get_db)):
    """GET /api/cases/{id}/report/pdf — Download as PDF."""
    result = await db.execute(
        select(Report).where(Report.case_id == case_id)
    )
    report = result.scalar_one_or_none()
    if not report or not report.report_html:
        raise HTTPException(status_code=404, detail="Report not found")

    from ..export.pdf_export import html_to_pdf
    pdf_bytes = html_to_pdf(report.report_html)

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=second_opinion_{case_id}.pdf"},
    )


@router.get("/{case_id}/report/docx")
async def get_report_docx(case_id: UUID, db: AsyncSession = Depends(get_db)):
    """GET /api/cases/{id}/report/docx — Download as DOCX."""
    result = await db.execute(
        select(Report).where(Report.case_id == case_id)
    )
    report = result.scalar_one_or_none()
    if not report or not report.report_markdown:
        raise HTTPException(status_code=404, detail="Report not found")

    from ..export.docx_export import markdown_to_docx
    docx_bytes = markdown_to_docx(report.report_markdown)

    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f"attachment; filename=second_opinion_{case_id}.docx"},
    )


@router.post("/{case_id}/followup", response_model=FollowUpResponse)
async def ask_followup(
    case_id: UUID,
    body: FollowUpRequest,
    db: AsyncSession = Depends(get_db),
):
    """POST /api/cases/{id}/followup — Ask a follow-up question about the report."""
    result = await db.execute(
        select(Report).where(Report.case_id == case_id)
    )
    report = result.scalar_one_or_none()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    from ..pipeline.gemini import call_gemini

    prompt = f"""You are answering a follow-up question about a medical second opinion report.

REPORT SUMMARY:
Primary Diagnosis: {report.primary_diagnosis}
Executive Summary: {report.executive_summary or 'N/A'}
Analysis excerpt: {(report.medgemma_clean or '')[:3000]}

PATIENT'S QUESTION: {body.question}

Provide a clear, evidence-based answer in 2-4 paragraphs. Reference specific findings from the report where relevant."""

    answer = call_gemini(prompt, max_tokens=1024, temperature=0.3)

    followup = FollowUp(
        case_id=case_id,
        report_id=report.id,
        question=body.question,
        answer=answer,
    )
    db.add(followup)
    await db.commit()
    await db.refresh(followup)

    return FollowUpResponse(
        question=followup.question,
        answer=followup.answer,
        created_at=followup.created_at,
    )
