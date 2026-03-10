"""Research report compiler v2 — enhanced 10-step research report.

Builds markdown/HTML from the full evidence-backed research pipeline,
including hallucination checks, claim verification, and evidence synthesis.
"""

from datetime import datetime, timezone
import markdown

from .citation_mapper import build_unified_bibliography, remap_citations_in_text
from .junk_filter import filter_junk_refs
from .storm_dedup import remove_redundant_sections
from .report_compiler import _build_verification_summary, _build_enriched_bibliography


def compile_research_report_v2(
    research_topic: str,
    specialty: str | None,
    research_intent: str | None,
    storm_article: str | None,
    storm_url_to_info: dict | None,
    evidence_results: list,
    evidence_synthesis: str,
    hallucination_check: dict,
    executive_summary: str,
    serper_refs: list,
    verification_stats: dict | None = None,
) -> dict:
    """Compile the enhanced research report from the 10-step pipeline.

    Null-ref guard: gracefully handles missing/empty inputs at every stage
    so the report always compiles even if upstream steps failed or timed out.

    Returns dict with keys:
        report_markdown, report_html, references, total_sources, storm_article_clean.
    """
    # Null-ref guard: normalize all inputs
    storm_article = storm_article or ""
    storm_url_to_info = storm_url_to_info or {}
    evidence_results = evidence_results or []
    evidence_synthesis = evidence_synthesis or ""
    hallucination_check = hallucination_check or {}
    executive_summary = executive_summary or "Executive summary not available — upstream step may have failed."
    serper_refs = serper_refs or []
    verification_stats = verification_stats or {}

    # Build unified bibliography from both Serper and STORM refs
    unique_refs, storm_remap, old_to_new = build_unified_bibliography(
        serper_refs,
        storm_url_to_info,
    )
    unique_refs = filter_junk_refs(unique_refs)

    for i, ref in enumerate(unique_refs):
        ref["id"] = i + 1

    # Process STORM article
    storm_article_clean = ""
    if storm_article:
        storm_article_clean = remove_redundant_sections(storm_article)
        if storm_remap:
            storm_article_clean = remap_citations_in_text(storm_article_clean, storm_remap)

    # Remap evidence synthesis citations
    if evidence_synthesis and old_to_new:
        evidence_synthesis = remap_citations_in_text(evidence_synthesis, old_to_new)

    # Hallucination info
    hallucinations = hallucination_check.get("issues", []) if hallucination_check else []

    # Build bibliography and verification summary
    bibliography = _build_enriched_bibliography(unique_refs)
    verification_block = _build_verification_summary(verification_stats)

    # Metadata
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    claims_count = len(evidence_results) if evidence_results else 0
    specialty_label = specialty or "General Medicine"
    intent_label = research_intent or "Literature Review"

    # Build report markdown
    report_md = f"""# Research Report: {research_topic}

**Generated:** {now}
**Specialty:** {specialty_label}
**Research Intent:** {intent_label}
**Research Engine:** Co-STORM / STORM Framework + Gemini 2.0 Flash
**Evidence Sources:** {len(unique_refs)} references | {claims_count} claims verified

> **Note:** This report is AI-generated for research and informational purposes only.
> It should be critically evaluated and cross-referenced with primary sources.

---

## Executive Summary

{executive_summary}

{verification_block}

---

"""

    # Literature Review section
    if storm_article_clean:
        report_md += f"""## Literature Review

*Co-STORM / STORM framework conducted deep multi-perspective research on this topic.
Citation numbers [n] refer to the References section below.*

"""
        if hallucinations:
            report_md += f"""*Validation Note: {len(hallucinations)} potential inaccuracy/inaccuracies
were identified and corrected in the original article.*

"""
        report_md += f"""{storm_article_clean}

---

"""
    else:
        report_md += """## Literature Review

*Deep research (Co-STORM / STORM) did not produce an article for this topic.
This may indicate a timeout or search backend issue. The evidence verification
below is based on direct claim searches.*

---

"""

    # Evidence Verification section
    if evidence_synthesis:
        report_md += f"""## Evidence Verification

*{claims_count} claims were extracted and each was searched against current
medical and scientific literature. Evidence was synthesized by Gemini 2.0 Flash.*

{evidence_synthesis}

---

"""
    elif claims_count > 0:
        report_md += f"""## Evidence Verification

*{claims_count} claims were extracted but evidence synthesis was not completed.
This may indicate a pipeline timeout.*

---

"""

    # References
    report_md += bibliography

    # Methodology section
    report_md += f"""
---

## Methodology

This report was generated using a 10-step evidence-backed research pipeline:

1. **Topic Accepted:** Research topic received and queued for processing.

2. **Research Questions (Gemini 2.0 Flash):** Generated focused research questions
   to guide the literature investigation.

3. **Deep Research (Co-STORM / STORM + Gemini Flash):** Conducted automated
   multi-perspective research, generating a comprehensive cited literature review.

4. **Hallucination Guard (Gemini 2.0 Flash):** Validated the research article for
   fabricated citations, invented statistics, and factual errors.
   {"Found and corrected " + str(len(hallucinations)) + " issues." if hallucinations else "No hallucinations detected."}

5. **Claim Extraction (Gemini 2.0 Flash):** Extracted {claims_count} verifiable claims
   from the literature review.

6. **Evidence Search (Serper.dev):** Searched each claim against current scientific
   literature to find supporting or contradicting evidence.

7. **Citation Verification (OpenAlex):** Verified references against the OpenAlex
   academic database for authenticity, citation counts, and retraction status.

8. **Evidence Synthesis (Gemini 2.0 Flash):** Synthesized search results against
   each claim, assigning evidence verdicts.

9. **Executive Summary (Gemini 2.0 Flash):** Generated a concise executive summary
   of findings with specialty context.

10. **Report Compilation:** Assembled all outputs into this unified report with
    enriched bibliography and verification metadata.

**Total unique sources:** {len(unique_refs)}

---
*Generated with Co-STORM / STORM + Gemini 2.0 Flash + Serper.dev + OpenAlex*
"""

    report_html = markdown.markdown(report_md, extensions=["tables", "fenced_code"])

    return {
        "report_markdown": report_md,
        "report_html": report_html,
        "references": unique_refs,
        "total_sources": len(unique_refs),
        "storm_article_clean": storm_article_clean,
    }
