"""Final report compiler — merges all pipeline outputs into markdown/HTML.

Ported from v5 lines 907-1096.
"""

from datetime import datetime, timezone
import markdown

from .citation_mapper import build_unified_bibliography, remap_citations_in_text
from .junk_filter import filter_junk_refs
from .storm_dedup import remove_redundant_sections
from .summarizer import generate_executive_summary


def compile_report(
    medgemma_clean: str,
    hallucination_check: dict,
    evidence_results: list,
    evidence_synthesis: str,
    storm_article: str | None,
    storm_url_to_info: dict | None,
    serper_refs: list,
    primary_diagnosis: str,
    raw_case_text: str = "",
) -> dict:
    """Compile all pipeline outputs into the final report.

    Returns dict with keys:
        report_markdown, report_html, references, executive_summary,
        total_sources, storm_article_clean.
    """
    # Build unified bibliography
    unique_refs, storm_remap, old_to_new = build_unified_bibliography(
        serper_refs,
        storm_url_to_info or {},
    )

    # Filter junk refs
    unique_refs = filter_junk_refs(unique_refs)

    # Re-number sequentially after filtering
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

    # Generate executive summary
    exec_summary = generate_executive_summary(
        medgemma_analysis=medgemma_clean,
        evidence_synthesis=evidence_synthesis,
        primary_diagnosis=primary_diagnosis,
        total_sources=len(unique_refs),
        hallucination_count=len(hallucinations),
    )

    # Build bibliography markdown
    bibliography = ""
    if unique_refs:
        bibliography = "\n## References\n\n"
        for ref in unique_refs:
            title = ref.get("title") or "Untitled"
            url = ref["url"]
            snippet = ref.get("snippet", "")
            if snippet:
                bibliography += f"**[{ref['id']}]** {title}. {snippet}  \n{url}\n\n"
            else:
                bibliography += f"**[{ref['id']}]** {title}.  \n{url}\n\n"

    # Build report markdown
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    claims_count = len(evidence_results) if evidence_results else 0

    report_md = f"""# Second Opinion Report

**Generated:** {now}
**Clinical Analysis:** MedGemma 1.5 4B-IT (Google Vertex AI)
**Evidence Verification:** Gemini 2.0 Flash + Serper.dev ({len(unique_refs)} sources)
**Deep Research:** STORM Framework

> **Disclaimer:** This report is AI-generated for informational and research purposes only.
> It does not constitute medical advice, diagnosis, or treatment. All clinical decisions
> must be made by qualified healthcare professionals.

---

## Executive Summary

{exec_summary}

---

## Part 1: MedGemma Clinical Analysis (Second Opinion)

*MedGemma 1.5 4B-IT was asked to critically evaluate the referring diagnosis and
consider alternative explanations for this patient's presentation.*
"""

    if hallucinations:
        report_md += f"""
*Validation Note: Gemini 2.0 Flash identified {len(hallucinations)} potential inaccuracies
in MedGemma's output. These have been flagged or corrected inline below.*

"""

    report_md += f"""{medgemma_clean}

---

## Part 2: Evidence Verification

*Each key claim from MedGemma's analysis was extracted and searched against current
medical literature. Gemini 2.0 Flash synthesized the evidence.*

{evidence_synthesis}

---

"""

    if storm_article_clean:
        report_md += f"""## Part 3: STORM Literature Review

*Stanford's STORM framework conducted deep research on the diagnostic dilemma.
Citation numbers [n] refer to the unified References section below.*

{storm_article_clean}

---

"""

    report_md += bibliography

    report_md += f"""
---

## Methodology

This report was generated using a multi-stage evidence-backed pipeline:

1. **Clinical Analysis (MedGemma 1.5 4B-IT):** Analyzed the case as a second opinion,
   critically evaluating the referring diagnosis and ranking alternatives.

2. **Hallucination Guard (Gemini 2.0 Flash):** Validated MedGemma's recommended tests
   and clinical criteria against known medical standards.
   {"Found and corrected " + str(len(hallucinations)) + " issues." if hallucinations else "No hallucinations detected."}

3. **Evidence Verification (Gemini Flash + Serper.dev):** Extracted {claims_count} key
   claims, searched each against medical literature, and assessed evidence.

4. **Deep Research (STORM + Gemini Flash):** Conducted automated multi-perspective
   research on the specific diagnostic dilemma, generating a cited literature review.

**Total unique sources:** {len(unique_refs)}

---
*Generated with MedGemma 1.5 4B-IT + Gemini 2.0 Flash + STORM + Serper.dev*
"""

    # Render HTML
    report_html = markdown.markdown(report_md, extensions=["tables", "fenced_code"])

    return {
        "report_markdown": report_md,
        "report_html": report_html,
        "references": unique_refs,
        "executive_summary": exec_summary,
        "total_sources": len(unique_refs),
        "storm_article_clean": storm_article_clean,
    }
