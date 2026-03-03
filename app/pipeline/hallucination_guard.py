"""Hallucination guard — Gemini validates MedGemma output.

Ported from v5 lines 460-548.
"""

import json
import re

from .gemini import call_gemini

VALIDATION_PROMPT = """You are a medical fact-checker. A medical AI (MedGemma 4B) generated the analysis below.
Small language models sometimes hallucinate non-existent tests, antibodies, or scoring systems.

YOUR TASK: Check the analysis for factual errors. Specifically:

1. Are ALL recommended lab tests and antibodies REAL and CLINICALLY RECOGNIZED?
   - Flag any test or antibody that does not exist in standard clinical practice
   - Flag any test that exists but is irrelevant to hepatology/liver disease
   - For each flagged item, suggest the correct alternative

2. Are the cited clinical guidelines REAL?
   - Flag any guideline that doesn't exist

3. Are the diagnostic criteria described ACCURATE?
   - Flag any scoring system or criteria that is described incorrectly

Return your response in this EXACT format (no markdown backticks):
{{"hallucinations_found": true/false, "issues": [{{"text": "the exact hallucinated text from the analysis", "problem": "why this is wrong", "correction": "what it should say instead"}}], "validated_clean": true/false}}

If no hallucinations found, return:
{{"hallucinations_found": false, "issues": [], "validated_clean": true}}

MEDGEMMA ANALYSIS TO CHECK:
{analysis}"""


def check_hallucinations(analysis: str) -> dict:
    """Validate MedGemma output against known medical standards.

    Returns dict with keys: hallucinations_found, issues, validated_clean.
    """
    raw = call_gemini(
        VALIDATION_PROMPT.format(analysis=analysis[:8000]),
        max_tokens=2048,
        temperature=0.1,
    )

    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"hallucinations_found": False, "issues": [], "parse_error": True}

    return result


RESEARCH_VALIDATION_PROMPT = """You are a research fact-checker. An AI research pipeline generated the article below.
AI-generated research articles sometimes contain fabricated citations, invented statistics, or incorrect claims.

YOUR TASK: Check the article for factual errors. Specifically:

1. Are ALL cited references and statistics REAL?
   - Flag any citation that appears fabricated (non-existent journal, made-up DOI)
   - Flag any statistic or prevalence figure that seems invented
   - For each flagged item, suggest how to verify or correct it

2. Are the cited clinical guidelines and drug approvals REAL?
   - Flag any guideline or approval status that doesn't exist
   - Flag any drug described with an incorrect approval status

3. Are the described mechanisms and relationships ACCURATE?
   - Flag any biological mechanism described incorrectly
   - Flag any causal claim that contradicts established literature

Return your response in this EXACT format (no markdown backticks):
{{"hallucinations_found": true/false, "issues": [{{"text": "the exact hallucinated text from the article", "problem": "why this is wrong", "correction": "what it should say instead"}}], "validated_clean": true/false}}

If no hallucinations found, return:
{{"hallucinations_found": false, "issues": [], "validated_clean": true}}

RESEARCH ARTICLE TO CHECK:
{analysis}"""


def check_research_hallucinations(article: str) -> dict:
    """Validate a research article for fabricated citations, invented stats, and factual errors.

    Returns dict with keys: hallucinations_found, issues, validated_clean.
    """
    raw = call_gemini(
        RESEARCH_VALIDATION_PROMPT.format(analysis=article[:8000]),
        max_tokens=2048,
        temperature=0.1,
    )

    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"hallucinations_found": False, "issues": [], "parse_error": True}

    return result


def apply_corrections(analysis: str, issues: list) -> str:
    """Apply hallucination corrections inline in the analysis text."""
    for issue in issues:
        bad = issue.get("text", "")
        fix = issue.get("correction", "")
        if bad and bad in analysis:
            if fix:
                analysis = analysis.replace(bad, f"{fix} [corrected]")
            else:
                problem = issue.get("problem", "")[:60]
                analysis = analysis.replace(bad, f"{bad} [flagged: {problem}]")
    return analysis
