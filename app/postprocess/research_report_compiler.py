"""Research report compiler — builds markdown/HTML from STORM research output."""

from datetime import datetime, timezone
import markdown

from .citation_mapper import build_unified_bibliography, remap_citations_in_text
from .junk_filter import filter_junk_refs


def compile_research_report(
    research_topic: str,
    research_questions: list | None,
    storm_article: str | None,
    storm_url_to_info: dict | None,
    executive_summary: str = "",
) -> dict:
    """Compile research pipeline outputs into the final report.

    Returns dict with keys:
        report_markdown, report_html, references, executive_summary,
        total_sources, storm_article_clean.
    """
    # Build unified bibliography from STORM refs only (no serper in research pipeline)
    unique_refs, storm_remap, _ = build_unified_bibliography(
        [],
        storm_url_to_info or {},
    )
    unique_refs = filter_junk_refs(unique_refs)

    for i, ref in enumerate(unique_refs):
        ref["id"] = i + 1

    # Process STORM article
    storm_article_clean = ""
    if storm_article:
        from .storm_dedup import remove_redundant_sections
        storm_article_clean = remove_redundant_sections(storm_article)
        if storm_remap:
            storm_article_clean = remap_citations_in_text(storm_article_clean, storm_remap)

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

    # Build research questions section
    questions_md = ""
    if research_questions:
        questions_md = "## Research Questions\n\n"
        for i, q in enumerate(research_questions, 1):
            if isinstance(q, dict):
                questions_md += f"{i}. {q.get('question', q.get('text', str(q)))}\n"
            else:
                questions_md += f"{i}. {q}\n"
        questions_md += "\n---\n\n"

    # Build report markdown
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    report_md = f"""# Research Report: {research_topic}

**Generated:** {now}
**Research Engine:** STORM Framework + Gemini 2.0 Flash
**Sources:** {len(unique_refs)} references

> **Note:** This report is AI-generated for research and informational purposes only.
> It should be critically evaluated and cross-referenced with primary sources.

---

## Executive Summary

{executive_summary}

---

{questions_md}"""

    if storm_article_clean:
        report_md += f"""## Literature Review

*STORM framework conducted deep multi-perspective research on this topic.
Citation numbers [n] refer to the References section below.*

{storm_article_clean}

---

"""

    report_md += bibliography

    report_md += f"""
---

## Methodology

This report was generated using an automated research pipeline:

1. **Topic Analysis (Gemini 2.0 Flash):** Analyzed the research topic and generated
   focused research questions to guide the investigation.

2. **Deep Research (STORM + Gemini Flash):** Conducted automated multi-perspective
   research, generating a comprehensive cited literature review.

3. **Report Compilation:** Assembled findings with executive summary and unified
   bibliography.

**Total unique sources:** {len(unique_refs)}

---
*Generated with Gemini 2.0 Flash + STORM Framework*
"""

    report_html = markdown.markdown(report_md, extensions=["tables", "fenced_code"])

    return {
        "report_markdown": report_md,
        "report_html": report_html,
        "references": unique_refs,
        "executive_summary": executive_summary,
        "total_sources": len(unique_refs),
        "storm_article_clean": storm_article_clean,
    }
