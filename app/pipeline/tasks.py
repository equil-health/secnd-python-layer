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
        verify_citations.s(case_id),
        synthesize_evidence_task.s(case_id),
        storm_research.s(case_id),
        verify_storm_citations.s(case_id),
        compile_report.s(case_id),
    )
    pipeline.apply_async()


def _build_case_text(case) -> str:
    """Build case text from structured fields or raw text (shared helper)."""
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
    return case_text


def _build_medgemma_prompt(case) -> str:
    """Build the MedGemma second-opinion analysis prompt from a Case model."""
    case_text = _build_case_text(case)

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


def _build_zebra_prompt(case) -> str:
    """Build the MedGemma zebra-mode prompt for rare disease differential."""
    case_text = _build_case_text(case)

    return f"""You are a senior specialist with expertise in rare and uncommon diseases. A referring physician has made a diagnosis, but the patient's presentation has atypical features that may suggest a rarer condition.

Your task is to THINK ZEBRA — go beyond the obvious "horses" (common diagnoses) and consider "zebras" (rare diseases) that could explain the clinical picture.

IMPORTANT: Only recommend tests, antibodies, and scoring systems that are REAL and WIDELY USED in clinical practice. Do NOT invent or guess test names.

Structure your analysis:

1. CASE SUMMARY: Key clinical findings in brief.

2. COMMON DIAGNOSES CONSIDERED AND EXCLUDED (HORSES):
   List the 2-3 most likely common diagnoses. For each, explain which specific findings ARGUE AGAINST it.

3. ZEBRA HYPOTHESES (rare disease candidates, ranked by fit):
   For each rare condition:
   - Name and brief description
   - Orphanet/OMIM ID if known
   - Estimated prevalence
   - Which specific findings in THIS patient support it
   - Which findings argue against it
   - Confirmatory tests needed

4. PATTERN RECOGNITION:
   What combination of findings makes this case unusual? What "red flags" for rare disease are present?

5. RECOMMENDED DIAGNOSTIC PATHWAY:
   - Step-by-step workup to differentiate between zebra hypotheses
   - Genetic testing if applicable
   - Specialist referrals needed
   - Timeline urgency

6. PATIENT COMMUNICATION:
   How to explain to the patient that a rare condition is being investigated.

FEW-SHOT EXAMPLE:
A 28-year-old female with recurrent deep vein thrombosis, livedo reticularis, and recurrent miscarriages was referred with "hypercoagulable state — consider Factor V Leiden."
- HORSE EXCLUDED: Factor V Leiden — does not explain livedo reticularis or miscarriages together
- HORSE EXCLUDED: Protein C/S deficiency — does not explain the skin findings
- ZEBRA HYPOTHESIS 1: Antiphospholipid Syndrome (APS) — Orphanet 464, prevalence ~1:2000. The triad of thrombosis + livedo + pregnancy loss is classic. Confirm with anticardiolipin antibodies, lupus anticoagulant, anti-beta2 glycoprotein I.
- ZEBRA HYPOTHESIS 2: Sneddon Syndrome — rare variant with livedo + stroke risk, if CNS symptoms emerge.

CLINICAL CASE:
{case_text}

THINK ZEBRA — RARE DISEASE ANALYSIS:"""


def _get_search_suffix(mode: str) -> str:
    """Return search query suffix for rare disease site filtering in zebra mode."""
    if mode == "zebra":
        return "site:orpha.net OR site:rarediseases.info.nih.gov OR site:rarediseases.org OR site:omim.org"
    return ""


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
    from .medgemma import call_medgemma
    from ..models.case import Case

    session = _get_sync_session()
    try:
        case = session.query(Case).filter_by(id=case_id).first()
        mode = case.diagnosis_mode or "standard"

        label = "MedGemma analyzing case (Zebra mode)..." if mode == "zebra" else "MedGemma analyzing case..."
        broadcast(case_id, {
            "type": "step_update", "step": 2,
            "label": label, "status": "running",
        })

        prompt = _build_zebra_prompt(case) if mode == "zebra" else _build_medgemma_prompt(case)

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
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        mode = case.diagnosis_mode or "standard"

        start = time.time()
        claims_data = extract_claims(report.medgemma_clean, mode=mode)
        duration = time.time() - start

        if mode == "zebra":
            # Store full dict so compile_report can access excluded_common/zebra_hypotheses
            report.extracted_claims = claims_data
        else:
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
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        mode = case.diagnosis_mode or "standard"
        search_suffix = _get_search_suffix(mode)
        raw_claims = report.extracted_claims or []
        # In zebra mode, extracted_claims is a dict with a "claims" key
        claims = raw_claims.get("claims", []) if isinstance(raw_claims, dict) else raw_claims

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

            results = search_serper(query, num_results=settings.SERPER_RESULTS_PER_QUERY, query_suffix=search_suffix)

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


@app.task(bind=True, name="pipeline.verify_citations", soft_time_limit=300, time_limit=360)
def verify_citations(self, prev_result, case_id: str):
    """Step 7: Verify Serper references against OpenAlex."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 7,
        "label": "Verifying citations (OpenAlex)...", "status": "running",
    })

    from .openalex import OpenAlexVerifier
    from ..models.report import Report

    session = _get_sync_session()
    start = time.time()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        verifier = OpenAlexVerifier(settings.OPENALEX_EMAIL, settings.OPENALEX_API_KEY)
        enriched_refs = verifier.verify_all(serper_refs)
        duration = time.time() - start

        # Compute stats
        total = len(enriched_refs)
        verified = sum(1 for r in enriched_refs if r.get("is_verified"))
        retracted = sum(1 for r in enriched_refs if r.get("is_retracted"))
        peer_reviewed = sum(1 for r in enriched_refs if r.get("quality_tier") in ("peer-reviewed", "strong", "landmark"))
        unverified = sum(1 for r in enriched_refs if r.get("quality_tier") == "unverified")
        landmark = sum(1 for r in enriched_refs if r.get("quality_tier") == "landmark")

        stats = {
            "total": total,
            "verified": verified,
            "retracted": retracted,
            "peer_reviewed": peer_reviewed,
            "unverified": unverified,
            "landmark": landmark,
        }

        report.verification_stats = stats
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 7,
            "label": "Verifying citations (OpenAlex)...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{verified}/{total} verified, {retracted} retracted",
        })

        return {"serper_refs": enriched_refs, "verification_stats": stats}
    except SoftTimeLimitExceeded:
        try:
            session.commit()
        except Exception:
            pass
        duration = time.time() - start
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []
        broadcast(case_id, {
            "type": "step_update", "step": 7,
            "label": "Verifying citations (OpenAlex)...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "Citation verification timed out — continuing with partial results",
        })
        return {"serper_refs": serper_refs, "verification_stats": {}}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.synthesize_evidence")
def synthesize_evidence_task(self, prev_result, case_id: str):
    """Step 8: Gemini verifies claims against evidence."""
    broadcast(case_id, {
        "type": "step_update", "step": 8,
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
            "type": "step_update", "step": 8,
            "label": "Verifying claims against evidence...", "status": "done",
            "duration_s": round(duration, 1),
        })

        return {"synthesis_length": len(synthesis), "serper_refs": serper_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.storm_research", soft_time_limit=300, time_limit=360)
def storm_research(self, prev_result, case_id: str):
    """Step 9: STORM deep research on the diagnostic dilemma."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 9,
        "label": "STORM deep research...", "status": "running",
    })

    from .storm_runner import run_storm
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    start = time.time()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        mode = case.diagnosis_mode or "standard"

        # Derive STORM topic from primary diagnosis
        primary_dx = report.primary_diagnosis or "diagnostic dilemma"
        if mode == "zebra":
            # Build a symptoms summary from the case for zebra STORM topic
            symptoms = case.presenting_complaint or primary_dx
            topic = f"Rare disease differential diagnosis for {symptoms[:120]}"
        else:
            topic = f"{primary_dx} differential diagnosis clinical evidence"

        output_dir = tempfile.mkdtemp(prefix="storm_")

        result = run_storm(topic=topic, output_dir=output_dir)
        duration = time.time() - start

        report.storm_article_raw = result["article"]
        report.storm_url_to_info = result["url_to_info"]
        session.commit()

        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        backend = result.get("search_backend", "unknown")
        if result["error"]:
            broadcast(case_id, {
                "type": "step_update", "step": 9,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Warning ({backend}): {result['error'][:80]}",
            })
        else:
            broadcast(case_id, {
                "type": "step_update", "step": 9,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Article: {len(result['article'])} chars ({backend})",
            })

        return {"storm_length": len(result["article"] or ""), "serper_refs": serper_refs}
    except SoftTimeLimitExceeded:
        duration = time.time() - start
        broadcast(case_id, {
            "type": "step_update", "step": 9,
            "label": "STORM deep research...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "STORM timed out — continuing pipeline without deep research",
        })
        return {"storm_length": 0, "serper_refs": prev_result.get("serper_refs", []) if prev_result else []}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.verify_storm_citations", soft_time_limit=300, time_limit=360)
def verify_storm_citations(self, prev_result, case_id: str):
    """Step 10: Verify STORM references against OpenAlex."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 10,
        "label": "Verifying STORM citations...", "status": "running",
    })

    from .openalex import OpenAlexVerifier
    from ..models.report import Report
    from sqlalchemy.orm.attributes import flag_modified

    session = _get_sync_session()
    start = time.time()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        verifier = OpenAlexVerifier(settings.OPENALEX_EMAIL, settings.OPENALEX_API_KEY)

        # Convert STORM url_to_info into reference list for verification
        storm_url_to_info = report.storm_url_to_info or {}
        storm_refs = []
        for url, info in storm_url_to_info.items():
            storm_refs.append({
                "url": url,
                "title": info.get("title", "") if isinstance(info, dict) else "",
            })

        if storm_refs:
            verifier.verify_all(storm_refs)

            # Merge verification data back into storm_url_to_info
            for ref in storm_refs:
                url = ref.get("url", "")
                if url in storm_url_to_info:
                    info = storm_url_to_info[url]
                    if isinstance(info, dict):
                        info["is_verified"] = ref.get("is_verified", False)
                        info["quality_tier"] = ref.get("quality_tier", "unverified")
                        info["citation_count"] = ref.get("citation_count", 0)
                        info["is_retracted"] = ref.get("is_retracted", False)
                        info["doi"] = ref.get("doi")
                        info["journal"] = ref.get("journal")
                        info["year"] = ref.get("year")

            report.storm_url_to_info = storm_url_to_info
            flag_modified(report, "storm_url_to_info")

        # Update verification stats with combined counts
        existing_stats = report.verification_stats or {}
        storm_verified = sum(1 for r in storm_refs if r.get("is_verified"))
        storm_retracted = sum(1 for r in storm_refs if r.get("is_retracted"))
        existing_stats["storm_total"] = len(storm_refs)
        existing_stats["storm_verified"] = storm_verified
        existing_stats["storm_retracted"] = storm_retracted
        report.verification_stats = existing_stats
        flag_modified(report, "verification_stats")

        session.commit()
        duration = time.time() - start

        broadcast(case_id, {
            "type": "step_update", "step": 10,
            "label": "Verifying STORM citations...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{storm_verified}/{len(storm_refs)} STORM refs verified",
        })

        return {"serper_refs": serper_refs}
    except SoftTimeLimitExceeded:
        # Save whatever we've verified so far
        try:
            session.commit()
        except Exception:
            pass
        duration = time.time() - start
        broadcast(case_id, {
            "type": "step_update", "step": 10,
            "label": "Verifying STORM citations...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "Citation verification timed out — continuing with partial results",
        })
        return {"serper_refs": prev_result.get("serper_refs", []) if prev_result else []}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.compile_report")
def compile_report(self, prev_result, case_id: str):
    """Step 11: Compile all outputs into final report."""
    broadcast(case_id, {
        "type": "step_update", "step": 11,
        "label": "Building report...", "status": "running",
    })

    from ..postprocess.report_compiler import compile_report as _compile, compile_zebra_report
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        mode = case.diagnosis_mode or "standard"

        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        verification_stats = report.verification_stats

        start = time.time()
        if mode == "zebra":
            # Extract zebra-specific data from claims if available
            claims_data = report.extracted_claims
            excluded_common = []
            zebra_hypotheses = []
            if isinstance(claims_data, dict):
                excluded_common = claims_data.get("excluded_common", [])
                zebra_hypotheses = claims_data.get("zebra_hypotheses", [])

            compiled = compile_zebra_report(
                medgemma_clean=report.medgemma_clean or "",
                hallucination_check=report.hallucination_check or {},
                evidence_results=report.evidence_results or [],
                evidence_synthesis=report.evidence_synthesis or "",
                storm_article=report.storm_article_raw,
                storm_url_to_info=report.storm_url_to_info,
                serper_refs=serper_refs,
                primary_diagnosis=report.primary_diagnosis or "unknown",
                raw_case_text=case.raw_case_text or case.presenting_complaint or "",
                excluded_common=excluded_common,
                zebra_hypotheses=zebra_hypotheses,
                verification_stats=verification_stats,
            )
        else:
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
                verification_stats=verification_stats,
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
            "type": "step_update", "step": 11,
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


# ============================================================
# Research Pipeline v2 (10 steps — full evidence pipeline)
# ============================================================

def dispatch_research_pipeline_v2(case_id: str):
    """Start the enhanced 10-step research pipeline as a Celery chain."""
    pipeline = chain(
        research_generate_questions.s(None, case_id),   # Step 2 (reused)
        research_costorm.s(case_id),                     # Step 3
        research_hallucination_guard.s(case_id),         # Step 4
        research_extract_claims_task.s(case_id),         # Step 5
        research_search_evidence.s(case_id),             # Step 6
        research_verify_citations.s(case_id),            # Step 7
        research_synthesize_evidence.s(case_id),         # Step 8
        research_generate_summary.s(case_id),            # Step 9
        research_compile_report_v2.s(case_id),           # Step 10
    )
    pipeline.apply_async()


@app.task(bind=True, name="pipeline.research_costorm", soft_time_limit=300, time_limit=360)
def research_costorm(self, prev_result, case_id: str):
    """Step 3 (v2): Co-STORM / STORM deep research on the topic."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 3,
        "label": "Co-STORM deep research...", "status": "running",
    })

    from .costorm_runner import run_costorm
    from ..models.report import Report

    session = _get_sync_session()
    start = time.time()
    try:
        refined_topic = prev_result.get("refined_topic", "research topic") if prev_result else "research topic"
        output_dir = tempfile.mkdtemp(prefix="costorm_research_")

        result = run_costorm(topic=refined_topic, output_dir=output_dir)
        duration = time.time() - start

        report = session.query(Report).filter_by(case_id=case_id).first()
        if report:
            report.storm_article_raw = result["article"]
            report.storm_url_to_info = result["url_to_info"]
            session.commit()

        engine = result.get("engine", "unknown")
        backend = result.get("search_backend", "unknown")
        if result["error"]:
            broadcast(case_id, {
                "type": "step_update", "step": 3,
                "label": "Co-STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Warning ({engine}/{backend}): {result['error'][:80]}",
            })
        else:
            broadcast(case_id, {
                "type": "step_update", "step": 3,
                "label": "Co-STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Article: {len(result['article'] or '')} chars ({engine}/{backend})",
            })

        return {"storm_length": len(result["article"] or ""), "refined_topic": refined_topic}
    except SoftTimeLimitExceeded:
        duration = time.time() - start
        broadcast(case_id, {
            "type": "step_update", "step": 3,
            "label": "Co-STORM deep research...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "Co-STORM timed out — continuing pipeline",
        })
        return {"storm_length": 0, "refined_topic": prev_result.get("refined_topic", "research topic") if prev_result else "research topic"}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_hallucination_guard")
def research_hallucination_guard(self, prev_result, case_id: str):
    """Step 4 (v2): Validate STORM article for hallucinations."""
    broadcast(case_id, {
        "type": "step_update", "step": 4,
        "label": "Checking for hallucinations...", "status": "running",
    })

    from .hallucination_guard import check_research_hallucinations, apply_corrections
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        article = report.storm_article_raw or ""

        start = time.time()
        validation = check_research_hallucinations(article)
        duration = time.time() - start

        issues = validation.get("issues", [])
        if issues:
            corrected = apply_corrections(article, issues)
            report.storm_article_raw = corrected

        report.hallucination_check = validation
        session.commit()

        preview = f"{len(issues)} issue(s) flagged and corrected" if issues else "No hallucinations detected"

        broadcast(case_id, {
            "type": "step_update", "step": 4,
            "label": "Checking for hallucinations...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": preview,
        })

        return {"hallucinations": len(issues)}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_extract_claims")
def research_extract_claims_task(self, prev_result, case_id: str):
    """Step 5 (v2): Extract verifiable claims from STORM article."""
    broadcast(case_id, {
        "type": "step_update", "step": 5,
        "label": "Extracting research claims...", "status": "running",
    })

    from .claim_extractor import extract_research_claims
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        article = report.storm_article_raw or ""

        start = time.time()
        claims_data = extract_research_claims(article)
        duration = time.time() - start

        report.extracted_claims = claims_data
        report.primary_diagnosis = claims_data.get("primary_topic", "research topic")
        session.commit()

        claims = claims_data.get("claims", [])
        broadcast(case_id, {
            "type": "step_update", "step": 5,
            "label": "Extracting research claims...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{len(claims)} verifiable claims extracted",
        })

        return {"claims_count": len(claims), "primary_topic": claims_data.get("primary_topic", "")}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_search_evidence")
def research_search_evidence(self, prev_result, case_id: str):
    """Step 6 (v2): Serper searches each research claim + semantic pre-filter."""
    broadcast(case_id, {
        "type": "step_update", "step": 6,
        "label": "Searching evidence...", "status": "running",
        "progress": "0/? claims",
    })

    from .serper import search_serper
    from .prompts import MEDICAL_SEARCH_SUFFIX, MEDICAL_KEYWORD_SUFFIX
    from ..models.report import Report

    # Import semantic pre-filter (graceful fallback if unavailable)
    try:
        from ..breaking.semantic_utils import filter_evidence_by_relevance
        _has_semantic = True
    except Exception:
        _has_semantic = False

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        raw_claims = report.extracted_claims or {}
        claims = raw_claims.get("claims", []) if isinstance(raw_claims, dict) else raw_claims

        start = time.time()
        all_refs = []
        ref_counter = 1
        filtered_count = 0

        for i, claim in enumerate(claims):
            broadcast(case_id, {
                "type": "step_update", "step": 6,
                "label": "Searching evidence...", "status": "running",
                "progress": f"{i + 1}/{len(claims)} claims",
            })

            query = claim.get("search_query", "")
            if not query:
                continue

            # Append medical keywords to the query and use medical site filter
            query = f"{query} {MEDICAL_KEYWORD_SUFFIX}"
            results = search_serper(query, num_results=settings.SERPER_RESULTS_PER_QUERY, query_suffix=MEDICAL_SEARCH_SUFFIX)

            # Semantic pre-filter: discard results with low relevance to the claim
            if _has_semantic and results:
                before = len(results)
                results = filter_evidence_by_relevance(
                    claim.get("claim", query), results, threshold=0.68
                )
                filtered_count += before - len(results)

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

        preview = f"{len(all_refs)} sources found"
        if filtered_count:
            preview += f" ({filtered_count} low-relevance filtered)"

        broadcast(case_id, {
            "type": "step_update", "step": 6,
            "label": "Searching evidence...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": preview,
        })

        return {"total_refs": len(all_refs), "serper_refs": all_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_verify_citations", soft_time_limit=300, time_limit=360)
def research_verify_citations(self, prev_result, case_id: str):
    """Step 7 (v2): Verify Serper references against OpenAlex."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 7,
        "label": "Verifying citations (OpenAlex)...", "status": "running",
    })

    from .openalex import OpenAlexVerifier
    from ..models.report import Report

    session = _get_sync_session()
    start = time.time()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        verifier = OpenAlexVerifier(settings.OPENALEX_EMAIL, settings.OPENALEX_API_KEY)
        enriched_refs = verifier.verify_all(serper_refs)
        duration = time.time() - start

        total = len(enriched_refs)
        verified = sum(1 for r in enriched_refs if r.get("is_verified"))
        retracted = sum(1 for r in enriched_refs if r.get("is_retracted"))
        peer_reviewed = sum(1 for r in enriched_refs if r.get("quality_tier") in ("peer-reviewed", "strong", "landmark"))
        unverified = sum(1 for r in enriched_refs if r.get("quality_tier") == "unverified")
        landmark = sum(1 for r in enriched_refs if r.get("quality_tier") == "landmark")

        stats = {
            "total": total,
            "verified": verified,
            "retracted": retracted,
            "peer_reviewed": peer_reviewed,
            "unverified": unverified,
            "landmark": landmark,
        }

        report.verification_stats = stats
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 7,
            "label": "Verifying citations (OpenAlex)...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{verified}/{total} verified, {retracted} retracted",
        })

        return {"serper_refs": enriched_refs, "verification_stats": stats}
    except SoftTimeLimitExceeded:
        try:
            session.commit()
        except Exception:
            pass
        duration = time.time() - start
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []
        broadcast(case_id, {
            "type": "step_update", "step": 7,
            "label": "Verifying citations (OpenAlex)...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "Citation verification timed out — continuing with partial results",
        })
        return {"serper_refs": serper_refs, "verification_stats": {}}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_synthesize_evidence")
def research_synthesize_evidence(self, prev_result, case_id: str):
    """Step 8 (v2): Gemini synthesizes evidence vs research claims."""
    broadcast(case_id, {
        "type": "step_update", "step": 8,
        "label": "Synthesizing evidence...", "status": "running",
    })

    from .evidence_verifier import synthesize_research_evidence
    from ..models.report import Report

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        start = time.time()
        synthesis = synthesize_research_evidence(
            primary_topic=report.primary_diagnosis or "research topic",
            evidence_results=report.evidence_results or [],
            all_references=serper_refs,
        )
        duration = time.time() - start

        report.evidence_synthesis = synthesis
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 8,
            "label": "Synthesizing evidence...", "status": "done",
            "duration_s": round(duration, 1),
        })

        return {"synthesis_length": len(synthesis), "serper_refs": serper_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_generate_summary")
def research_generate_summary(self, prev_result, case_id: str):
    """Step 9 (v2): Generate research executive summary."""
    broadcast(case_id, {
        "type": "step_update", "step": 9,
        "label": "Generating executive summary...", "status": "running",
    })

    from ..postprocess.summarizer import generate_research_summary
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        start = time.time()
        exec_summary = generate_research_summary(
            article=report.storm_article_raw or "",
            evidence_synthesis=report.evidence_synthesis or "",
            research_topic=case.research_topic or "Research Topic",
            specialty=case.specialty or "General Medicine",
            total_sources=len(serper_refs) or report.total_sources or 0,
        )
        duration = time.time() - start

        report.executive_summary = exec_summary
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 9,
            "label": "Generating executive summary...", "status": "done",
            "duration_s": round(duration, 1),
        })

        return {"summary_length": len(exec_summary), "serper_refs": serper_refs}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_compile_report_v2")
def research_compile_report_v2(self, prev_result, case_id: str):
    """Step 10 (v2): Compile enhanced research report."""
    broadcast(case_id, {
        "type": "step_update", "step": 10,
        "label": "Compiling research report...", "status": "running",
    })

    from ..postprocess.research_report_compiler_v2 import compile_research_report_v2 as _compile_v2
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()
        serper_refs = prev_result.get("serper_refs", []) if prev_result else []

        start = time.time()
        compiled = _compile_v2(
            research_topic=case.research_topic or "Research Topic",
            specialty=case.specialty,
            research_intent=case.research_intent,
            storm_article=report.storm_article_raw,
            storm_url_to_info=report.storm_url_to_info,
            evidence_results=report.evidence_results or [],
            evidence_synthesis=report.evidence_synthesis or "",
            hallucination_check=report.hallucination_check or {},
            executive_summary=report.executive_summary or "",
            serper_refs=serper_refs,
            verification_stats=report.verification_stats,
        )
        duration = time.time() - start

        report.report_markdown = compiled["report_markdown"]
        report.report_html = compiled["report_html"]
        report.references = compiled["references"]
        report.total_sources = compiled["total_sources"]
        report.storm_article_clean = compiled["storm_article_clean"]
        report.total_claims = len(report.evidence_results or [])

        case.status = "completed"
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 10,
            "label": "Compiling research report...", "status": "done",
            "duration_s": round(duration, 1),
        })

        broadcast(case_id, {
            "type": "complete",
            "report_url": f"/api/cases/{case_id}/report",
            "total_sources": compiled["total_sources"],
            "executive_summary": (report.executive_summary or "")[:200],
        })

        return {"report_length": len(compiled["report_markdown"])}
    finally:
        session.close()


# ============================================================
# Research Pipeline v1 (lighter: 4 steps — kept for backward compat)
# ============================================================

def dispatch_research_pipeline(case_id: str):
    """Start the research pipeline as a Celery chain."""
    pipeline = chain(
        research_generate_questions.s(None, case_id),
        research_storm.s(case_id),
        research_compile_report.s(case_id),
    )
    pipeline.apply_async()


@app.task(bind=True, name="pipeline.research_generate_questions")
def research_generate_questions(self, prev_result, case_id: str):
    """Step 2: Gemini generates research questions + refined topic."""
    broadcast(case_id, {
        "type": "step_update", "step": 2,
        "label": "Generating research questions...", "status": "running",
    })

    from .gemini import call_gemini
    from .prompts import build_medical_prompt
    from ..models.case import Case

    session = _get_sync_session()
    try:
        case = session.query(Case).filter_by(id=case_id).first()
        topic = case.research_topic or ""
        context = case.raw_case_text or ""
        specialty = case.specialty or ""

        specialty_guidance = ""
        if specialty:
            specialty_guidance = f"""
You are a {specialty} specialist researcher. Tailor your research questions to:
- Use {specialty}-specific terminology, scoring systems, and guidelines
- Reference the most relevant {specialty} journals and trials
- Consider {specialty}-specific diagnostic criteria and treatment pathways
- Include questions about epidemiology, pathophysiology, and emerging therapies
  relevant to {specialty}
"""
        else:
            specialty_guidance = """
You are a general medical researcher. Generate broadly applicable research questions
covering epidemiology, diagnosis, treatment, and prognosis.
"""

        step_instruction = f"""You are a medical research assistant.{specialty_guidance}

Given the following research topic, generate:
1. A refined, specific topic suitable for deep literature research (1 sentence)
2. 5-7 focused research questions that would comprehensively explore this topic
   from a {specialty or "general medicine"} perspective
3. A coherence check: does the specialty "{specialty}" align with the research topic? If not, explain the mismatch.

Topic: {topic}
{f"Additional context: {context}" if context else ""}

Return JSON only:
{{
    "refined_topic": "...",
    "questions": ["question 1", "question 2", ...],
    "coherence_ok": true/false,
    "coherence_warning": "null or explanation if specialty does not match topic"
}}"""

        prompt = build_medical_prompt(step_instruction, topic, specialty)

        start = time.time()
        import json as _json
        import re as _re

        raw = call_gemini(prompt, max_tokens=1024, temperature=0.4)
        raw = _re.sub(r"^```json\s*", "", raw.strip())
        raw = _re.sub(r"\s*```$", "", raw)

        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            data = {"refined_topic": topic, "questions": []}

        duration = time.time() - start

        report = _get_or_create_report(session, case_id)
        report.extracted_claims = data
        session.commit()

        questions = data.get("questions", [])
        broadcast(case_id, {
            "type": "step_update", "step": 2,
            "label": "Generating research questions...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": f"{len(questions)} research questions generated",
        })

        return {"refined_topic": data.get("refined_topic", topic), "questions": questions}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_storm", soft_time_limit=300, time_limit=360)
def research_storm(self, prev_result, case_id: str):
    """Step 3: STORM deep research on the topic."""
    from celery.exceptions import SoftTimeLimitExceeded

    broadcast(case_id, {
        "type": "step_update", "step": 3,
        "label": "STORM deep research...", "status": "running",
    })

    from .storm_runner import run_storm
    from ..models.report import Report

    session = _get_sync_session()
    start = time.time()
    try:
        refined_topic = prev_result.get("refined_topic", "research topic") if prev_result else "research topic"
        output_dir = tempfile.mkdtemp(prefix="storm_research_")

        result = run_storm(topic=refined_topic, output_dir=output_dir)
        duration = time.time() - start

        report = session.query(Report).filter_by(case_id=case_id).first()
        if report:
            report.storm_article_raw = result["article"]
            report.storm_url_to_info = result["url_to_info"]
            session.commit()

        backend = result.get("search_backend", "unknown")
        if result["error"]:
            broadcast(case_id, {
                "type": "step_update", "step": 3,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Warning ({backend}): {result['error'][:80]}",
            })
        else:
            broadcast(case_id, {
                "type": "step_update", "step": 3,
                "label": "STORM deep research...", "status": "done",
                "duration_s": round(duration, 1),
                "preview": f"Article: {len(result['article'] or '')} chars ({backend})",
            })

        return {"storm_length": len(result["article"] or ""), "refined_topic": refined_topic}
    except SoftTimeLimitExceeded:
        duration = time.time() - start
        broadcast(case_id, {
            "type": "step_update", "step": 3,
            "label": "STORM deep research...", "status": "done",
            "duration_s": round(duration, 1),
            "preview": "STORM timed out — continuing pipeline without deep research",
        })
        return {"storm_length": 0, "refined_topic": prev_result.get("refined_topic", "research topic") if prev_result else "research topic"}
    finally:
        session.close()


@app.task(bind=True, name="pipeline.research_compile_report")
def research_compile_report(self, prev_result, case_id: str):
    """Step 4: Compile research report with executive summary."""
    broadcast(case_id, {
        "type": "step_update", "step": 4,
        "label": "Compiling research report...", "status": "running",
    })

    from .gemini import call_gemini
    from ..postprocess.research_report_compiler import compile_research_report
    from ..models.report import Report
    from ..models.case import Case

    session = _get_sync_session()
    try:
        report = session.query(Report).filter_by(case_id=case_id).first()
        case = session.query(Case).filter_by(id=case_id).first()

        research_topic = case.research_topic or "Research Topic"
        research_data = report.extracted_claims or {}
        questions = research_data.get("questions", [])
        storm_article = report.storm_article_raw or ""

        # Generate executive summary via Gemini
        summary_prompt = f"""Write a concise executive summary (3-5 sentences) for a research report on the following topic.
Focus on key findings and conclusions.

Topic: {research_topic}

Article excerpt (first 3000 chars):
{storm_article[:3000]}

Executive summary:"""

        start = time.time()
        exec_summary = call_gemini(summary_prompt, max_tokens=512, temperature=0.3)

        compiled = compile_research_report(
            research_topic=research_topic,
            research_questions=questions,
            storm_article=storm_article,
            storm_url_to_info=report.storm_url_to_info,
            executive_summary=exec_summary,
        )
        duration = time.time() - start

        report.report_markdown = compiled["report_markdown"]
        report.report_html = compiled["report_html"]
        report.references = compiled["references"]
        report.executive_summary = compiled["executive_summary"]
        report.total_sources = compiled["total_sources"]
        report.storm_article_clean = compiled["storm_article_clean"]

        case.status = "completed"
        session.commit()

        broadcast(case_id, {
            "type": "step_update", "step": 4,
            "label": "Compiling research report...", "status": "done",
            "duration_s": round(duration, 1),
        })

        broadcast(case_id, {
            "type": "complete",
            "report_url": f"/api/cases/{case_id}/report",
            "total_sources": compiled["total_sources"],
            "executive_summary": exec_summary[:200],
        })

        return {"report_length": len(compiled["report_markdown"])}
    finally:
        session.close()
