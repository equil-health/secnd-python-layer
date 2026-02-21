"""Executive summary generation via Gemini — NEW (from spec)."""

from ..pipeline.gemini import call_gemini

SUMMARY_PROMPT = """Write a 3-paragraph executive summary of this medical second opinion.

Paragraph 1: What the referring physician diagnosed and what this analysis found instead.
Paragraph 2: Key evidence supporting the alternative diagnosis (cite specific lab values).
Paragraph 3: Recommended next steps and whether biopsy should proceed.

Keep it under 200 words. Use plain language a patient could understand.
Do NOT use any headers, bullets, or markdown formatting — just 3 paragraphs.

PRIMARY DIAGNOSIS FOUND: {primary_diagnosis}
SOURCES REVIEWED: {total_sources}
MEDGEMMA ANALYSIS (excerpt): {medgemma_excerpt}
EVIDENCE REVIEW (excerpt): {evidence_excerpt}"""


def generate_executive_summary(
    medgemma_analysis: str,
    evidence_synthesis: str,
    primary_diagnosis: str,
    total_sources: int,
    hallucination_count: int = 0,
) -> str:
    """Generate a 3-paragraph executive summary via Gemini."""
    return call_gemini(
        SUMMARY_PROMPT.format(
            primary_diagnosis=primary_diagnosis,
            total_sources=total_sources,
            medgemma_excerpt=medgemma_analysis[:3000],
            evidence_excerpt=evidence_synthesis[:2000],
        ),
        max_tokens=500,
        temperature=0.3,
    )
