"""Executive summary generation via Gemini — NEW (from spec)."""

from ..pipeline.gemini import call_gemini
from ..pipeline.prompts import build_medical_prompt

ZEBRA_SUMMARY_PROMPT = """Write a 3-paragraph executive summary of this rare disease (zebra) analysis.

Paragraph 1: What common diagnoses were considered and excluded, and why the clinical
picture suggests a rare condition instead.
Paragraph 2: The top rare disease hypotheses identified, with the key clinical features
that support each one (cite specific lab values, symptoms, or findings).
Paragraph 3: Recommended next steps for confirming the rare disease diagnosis, including
specific genetic tests, specialist referrals, or confirmatory workups.

Keep it under 250 words. Use plain language a patient could understand.
Do NOT use any headers, bullets, or markdown formatting — just 3 paragraphs.

PRIMARY RARE DIAGNOSIS: {primary_diagnosis}
SOURCES REVIEWED: {total_sources}
ZEBRA ANALYSIS (excerpt): {medgemma_excerpt}
EVIDENCE REVIEW (excerpt): {evidence_excerpt}"""

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


RESEARCH_SUMMARY_PROMPT = """Write a 3-paragraph executive summary of this research report.

Paragraph 1: What the research topic is, what specialty area it falls under ({specialty}),
and what the key question or focus of the investigation was.
Paragraph 2: The most important findings from the literature review, including key evidence
and any areas of consensus or controversy among researchers.
Paragraph 3: Implications for clinical practice or future research, and any notable gaps
in the current evidence base.

Keep it under 250 words. Use clear, professional language suitable for a clinician audience.
Do NOT use any headers, bullets, or markdown formatting — just 3 paragraphs.

RESEARCH TOPIC: {research_topic}
SPECIALTY: {specialty}
SOURCES REVIEWED: {total_sources}
LITERATURE REVIEW (excerpt): {article_excerpt}
EVIDENCE REVIEW (excerpt): {evidence_excerpt}"""


def generate_research_summary(
    article: str,
    evidence_synthesis: str,
    research_topic: str,
    specialty: str = "General Medicine",
    total_sources: int = 0,
) -> str:
    """Generate a 3-paragraph research executive summary via Gemini."""
    wrapped_prompt = build_medical_prompt(
        RESEARCH_SUMMARY_PROMPT.format(
            research_topic=research_topic,
            specialty=specialty or "General Medicine",
            total_sources=total_sources,
            article_excerpt=article[:3000],
            evidence_excerpt=evidence_synthesis[:2000],
        ),
        research_topic=research_topic,
        specialty=specialty or "General Medicine",
    )
    return call_gemini(
        wrapped_prompt,
        max_tokens=600,
        temperature=0.3,
    )


def generate_zebra_summary(
    medgemma_analysis: str,
    evidence_synthesis: str,
    primary_diagnosis: str,
    total_sources: int,
) -> str:
    """Generate a zebra-mode executive summary via Gemini."""
    return call_gemini(
        ZEBRA_SUMMARY_PROMPT.format(
            primary_diagnosis=primary_diagnosis,
            total_sources=total_sources,
            medgemma_excerpt=medgemma_analysis[:3000],
            evidence_excerpt=evidence_synthesis[:2000],
        ),
        max_tokens=600,
        temperature=0.3,
    )
