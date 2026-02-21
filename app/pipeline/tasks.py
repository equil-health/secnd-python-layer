"""Celery task chain — orchestrates the full pipeline.

Each task: broadcasts running -> executes -> saves to DB -> broadcasts done.
"""

import time
import json
import tempfile
import redis

from celery import chain
from celery_app import app

from ..config import settings

_redis = redis.Redis.from_url(settings.REDIS_URL)


def broadcast(case_id: str, message: dict):
    """Publish pipeline status update via Redis pub/sub."""
    message["case_id"] = case_id
    _redis.publish(f"pipeline:{case_id}", json.dumps(message, default=str))


def dispatch_pipeline(case_id: str):
    """Start the full pipeline as a Celery chain."""
    pipeline = chain(
        analyze_case.s(None, case_id),
        clean_output.s(case_id),
        validate_claims.s(case_id),
        extract_claims_task.s(case_id),
        search_evidence.s(case_id),
        synthesize_evidence_task.s(case_id),
        storm_research.s(case_id),
        compile_report.s(case_id),
    )
    pipeline.apply_async()


def _build_medgemma_prompt(case) -> str:
    """Build the MedGemma second-opinion analysis prompt from a Case model."""
    # Build case text from structured fields or raw text
    case_text = case.raw_case_text or ""
    if not case_text:
        parts = []
        if case.referring_diagnosis:
            parts.append(f"Referring Diagnosis: {case.referring_diagnosis}")
        if case.patient_age and case.patient_sex:
            parts.append(f"Patient: {case.patient_age}-year-old {case.patient_sex}")
        if case.patient_ethnicity:
            parts[-1] += f", {case.patient_ethnicity} ethnicity"
        if case.presenting_complaint:
            parts.append(f"Presenting Complaint:\n{case.presenting_complaint}")
        if case.medical_history:
            parts.append(f"Medical History:\n{case.medical_history}")
        if case.medications:
            parts.append(f"Medications:\n{case.medications}")
        if case.physical_exam:
            parts.append(f"Physical Exam:\n{case.physical_exam}")
        if case.lab_results:
            lab_lines = []
            for lab in case.lab_results:
                flag = f" ({lab.get('flag', '')})" if lab.get("flag") else ""
                lab_lines.append(f"- {lab['name']}: {lab['value']} {lab.get('unit', '')}{flag}")
            parts.append("Labs:\n" + "\n".join(lab_lines))
        if case.imaging_reports:
            parts.append(f"Imaging:\n{case.imaging_reports}")
        if case.specific_question:
            parts.append(f"Key Question: {case.specific_question}")
        case_text = "\n\n".join(parts)

    return f"""You are a senior specialist providing a second opinion. A referring physician has made a diagnosis and the patient seeks a second opinion.

IMPORTANT: Only recommend tests, antibodies, and scoring systems that are REAL and WIDELY USED in clinical practice. Do NOT invent or guess test names.

Your task is to CRITICALLY EVALUATE the referring diagnosis. Structure your analysis:

1. CASE SUMMARY: Key clinical findings in brief.

2. EVALUATION OF REFERRING DIAGNOSIS:
   - Evidence SUPPORTING the diagnosis
   - Evidence AGAINST the diagnosis
   - Your assessment: How likely is this diagnosis?

3. ALTERNATIVE DIAGNOSES (ranked by likelihood for THIS patient):
   For each, explain which specific findings support it.

4. THE ALBUMIN-GLOBULIN GAP (if relevant):
   What does the protein pattern indicate?

5. RECOMMENDED NEXT STEPS:
   - Tests to do before invasive procedures
   - Should biopsy/procedure still proceed? What should pathology look for?
   - Your overall clinical impression

6. PATIENT COMMUNICATION:
   How to explain this to the anxious patient simply.

CLINICAL CASE:
{case_text}

SECOND OPINION ANALYSIS:"""


def _extract_preview(text: str, max_len: int = 100) -> str:
    """Extract a short preview from analysis text."""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0] + "..."


def _get_sync_session():
    """Get a synchronous DB session for Celery tasks."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    # Convert async URL to sync
    db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2").replace("postgresql://", "postgresql+psycopg2://")
    if "psycopg2+psycopg2" in db_url:
        db_url = db_url.replace("psycopg2+psycopg2", "psycopg2")
    engine = create_engine(db_url)
    return Session(engine)


def _get_or_create_report(session, case_id: str):
    """Get existing report for case or create one."""
    from ..models.report import Report
    report = session.query(Report).filter_by(case_id=case_id).first()
    if not report:
        import uuid
        report = Report(case_id=case_id)
        session.add(report)
        session.commit()
    return report


@app.task(bind=True, name="pipeline.analyze_case")
def analyze_case(self, prev_result, case_id: str):
    """Step 2: MedGemma clinical analysis."""
    broadcast(case_id, {
        "type": "step_update", "step": 2,
        "label": "MedGemma analyzing case...", "status": "running",
    })

    from .medgemma import call_medgemma
    from ..models.case import Case

    session = _get_sync_session()
    try:
        case = session.query(Case).filter_by(id=case_id).first()
        prompt = _build_medgemma_prompt(case)

        start = time.time()
        raw_analysis = call_medgemma(prompt, max_tokens=settings.MEDGEMMA_MAX_TOKENS)
        duration = time.time() - start

        report = _get_or_create_report(session, case_id)
        report.medgemma_raw = raw_analysis
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 2,
            "label": "MedGemma analyzing case...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": _extract_preview(raw_analysis),
        })

        return {"medgemma_raw_length": len(raw_analysis)}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.clean_output")
def clean_output(self, prev_result, case_id: str):
    """Step 3: Dedup + format MedGemma output."""
    broadcast(case_id, {
        "type": "step_update", "step": 3,
        "label": "Cleaning output...", "status": "running",
    })

    from ..postprocess.dedup import dedup_medgemma
    from ..postprocess.formatter import format_medgemma
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()

        start = time.time()
        cleaned = dedup_medgemma(report.medgemma_raw)
        formatted = format_medgemma(cleaned)
        duration = time.time() - start

        report.medgemma_clean = formatted
        session.commit()

        chars_removed = len(report.medgemma_raw) - len(formatted)

        broadcast(case_id, {
            "type": "step_update", "step": 3,
            "label": "Cleaning output...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"Removed {chars_removed} chars of duplication",
        })

        return {"cleaned_length": len(formatted)}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.validate_claims")
def validate_claims(self, prev_result, case_id: str):
    """Step 4: Gemini validates MedGemma for hallucinations."""
    broadcast(case_id, {
        "type": "step_update", "step": 4,
        "label": "Validating claims...", "status": "running",
    })

    from .hallucination_guard import check_hallucinations, apply_corrections
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()

        start = time.time()
        validation = check_hallucinations(report.medgemma_clean)
        duration = time.time() - start

        issues = validation.get("issues", [])
        if issues:
            report.medgemma_clean = apply_corrections(report.medgemma_clean, issues)

        report.hallucination_check = validation
        session.commit()

        preview = f"{len(issues)} hallucination(s) flagged and corrected" if issues else "No hallucinations detected"

        broadcast(case_id, {
            "type": "step_update", "step": 4,
            "label": "Validating claims...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": preview,
        })

        return {"hallucinations": len(issues)}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.extract_claims")
def extract_claims_task(self, prev_result, case_id: str):
    """Step 5: Gemini extracts verifiable claims."""
    broadcast(case_id, {
        "type": "step_update", "step": 5,
        "label": "Extracting key claims...", "status": "running",
    })

    from .claim_extractor import extract_claims
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()

        start = time.time()
        claims_data = extract_claims(report.medgemma_clean)
        duration = time.time() - start

        report.extracted_claims = claims_data["claims"]
        report.primary_diagnosis = claims_data["primary_diagnosis"]
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 5,
            "label": "Extracting key claims...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{len(claims_data['claims'])} verifiable claims extracted",
        })

        return {"claims_count": len(claims_data["claims"]), "primary_dx": claims_data["primary_diagnosis"]}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.search_evidence")
def search_evidence(self, prev_result, case_id: str):
    """Step 6: Serper searches each claim."""
    broadcast(case_id, {
        "type": "step_update", "step": 6,
        "label": "Searching evidence...", "status": "running",
        "progress": "0/? claims",
    })

    from .serper import search_serper
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        claims = report.extracted_claims or []

        start = time.time()
        all_refs = []
        ref_counter = 1

        for i, claim in enumerate(claims):
            broadcast(case_id, {
                "type": "step_update", "step": 6,
                "label": "Searching evidence...", "status": "running",
                "progress": f"{i + 1}/{len(claims)} claims",
            })

            query = claim.get("search_query", "")
            if not query:
                continue

            results = search_serper(query, num_results=settings.SERPER_RESULTS_PER_QUERY)

            claim_refs = []
            for r in results:
                ref_id = ref_counter
                all_refs.append({
                    "id": ref_id,
                    "title": r["title"],
                    "url": r["url"],
                    "snippet": r["snippet"],
                    "for_claim": claim.get("claim", "")[:80],
                })
                claim_refs.append(ref_id)
                ref_counter += 1

            claim["references"] = claim_refs
            claim["search_results"] = results

        duration = time.time() - start

        report.evidence_results = claims
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 6,
            "label": "Searching evidence...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{len(all_refs)} sources found",
        })

        return {"total_refs": len(all_refs), "serper_refs": all_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.synthesize_evidence")
def synthesize_evidence_task(self, prev_result, case_id: str):
    """Step 7: Gemini verifies claims against evidence."""
    broadcast(case_id, {
        "type": "step_update", "step": 7,
        "label": "Verifying claims against evidence...", "status": "running",
    })

    from .evidence_verifier import synthesize_evidence
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()

        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        start = time.time()
        synthesis = synthesize_evidence(
            primary_diagnosis=report.primary_diagnosis or "unknown",
            evidence_results=report.evidence_results or [],
            all_references=serper_refs,
        )
        duration = time.time() - start

        report.evidence_synthesis = synthesis
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 7,
            "label": "Verifying claims against evidence...", "status": "done",
            "duration_s": round(duration, 1),
        })

        return {"synthesis_length": len(synthesis), "serper_refs": serper_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.storm_research")
def storm_research(self, prev_result, case_id: str):
    """Step 8: STORM deep research on the diagnostic dilemma."""
    broadcast(case_id, {
        "type": "step_update", "step": 8,
        "label": "STORM deep research...", "status": "running",
    })

    from .storm_runner import run_storm
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()

        # Derive STORM topic from primary diagnosis
        primary_dx = report.primary_diagnosis or "diagnostic dilemma"
        topic = f"{primary_dx} differential diagnosis clinical evidence"

        output_dir = tempfile.mkdtemp(prefix="storm_")

        start = time.time()
        result = run_storm(topic=topic, output_dir=output_dir)
        duration = time.time() - start

        report.storm_article_raw = result["article"]
        report.storm_url_to_info = result["url_to_info"]
        session.commit()

        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        if result["error"]:
            broadcast(case_id, {
                "type": "step_update", "step": 8,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Completed with warning: {result['error'][:80]}",
            })
        else:
            broadcast(case_id, {
                "type": "step_update", "step": 8,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Article: {len(result['article'])} chars",
            })

        return {"storm_length": len(result["article"] or ""), "serper_refs": serper_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.compile_report")
def compile_report(self, prev_result, case_id: str):
    """Step 9: Compile all outputs into final report."""
    broadcast(case_id, {
        "type": "step_update", "step": 9,
        "label": "Building report...", "status": "running",
    })

    from ..postprocess.report_compiler import compile_report as _compile
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()

        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        start = time.time()
        compiled = _compile(
            medgemma_clean=report.medgemma_clean or "",
            hallucination_check=report.hallucination_check or {},
            evidence_results=report.evidence_results or [],
            evidence_synthesis=report.evidence_synthesis or "",
            storm_article=report.storm_article_raw,
            storm_url_to_info=report.storm_url_to_info,
            serper_refs=serper_refs,
            primary_diagnosis=report.primary_diagnosis or "unknown",
            raw_case_text=case.raw_case_text or case.presenting_complaint or "",
        )
        duration = time.time() - start

        report.report_markdown = compiled["report_markdown"]
        report.report_html = compiled["report_html"]
        report.references = compiled["references"]
        report.executive_summary = compiled["executive_summary"]
        report.total_sources = compiled["total_sources"]
        report.storm_article_clean = compiled["storm_article_clean"]
        report.total_claims = len(report.evidence_results or [])

        # Update case status
        case.status = "completed"

        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 9,
            "label": "Building report...", "status": "done",
            "duration_s": round(duration, 1),
        })

        broadcast(case_id, {
            "type": "complete",
            "report_url": f"/api/cases/{case_id}/report",
            "total_sources": compiled["total_sources"],
            "primary_diagnosis": report.primary_diagnosis or "",
            "executive_summary": compiled["executive_summary"][:200],
        })

        return {"report_length": len(compiled["report_markdown"])}
    finally:
        session.close()
