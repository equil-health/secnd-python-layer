"""Evidence synthesis — Gemini reviews search results vs claims.

Ported from v5 lines 686-729.
"""

from .gemini import call_gemini

SYNTHESIS_PROMPT = """You are a medical evidence reviewer. A specialist AI (MedGemma) analyzed a clinical case and made several diagnostic claims. We searched medical literature for each claim.

YOUR TASK: For each claim, assess whether the literature SUPPORTS, PARTIALLY SUPPORTS, or CONTRADICTS it. Write a structured evidence review.

Format your response as a markdown document with this structure:

## Evidence Review: [{primary_diagnosis}]

### Claim 1: [claim text]
**Verdict: SUPPORTED / PARTIALLY SUPPORTED / CONTRADICTED / INSUFFICIENT EVIDENCE**
Evidence: [2-3 sentences synthesizing what the search results show, citing reference numbers like [1], [2]]

### Claim 2: ...
(repeat for each claim)

## Overall Assessment
[3-4 sentences on whether the literature supports MedGemma's analysis overall]

## Key Takeaway for Patient
[2-3 sentences in plain language]

MEDGEMMA'S PRIMARY DIAGNOSIS: {primary_diagnosis}

MEDGEMMA'S KEY CLAIMS AND SEARCH EVIDENCE:
{evidence_context}

EVIDENCE REVIEW:"""


def _build_evidence_context(evidence_results: list, all_references: list) -> str:
    """Build a text summary of search results for each claim."""
    context = ""
    for ev in evidence_results:
        context += f"\n### Claim: {ev['claim']}\n"
        context += f"Search: {ev['search_query']}\n"
        for sr in ev.get("search_results", [])[:3]:
            ref_ids = [r["id"] for r in all_references if r["url"] == sr.get("url")]
            ref_tag = f"[{ref_ids[0]}]" if ref_ids else ""
            context += f"- {ref_tag} {sr.get('title', '')}: {sr.get('snippet', '')}\n"
    return context


def synthesize_evidence(
    primary_diagnosis: str,
    evidence_results: list,
    all_references: list,
) -> str:
    """Have Gemini synthesize evidence for/against each claim.

    Returns evidence synthesis as markdown text.
    """
    evidence_context = _build_evidence_context(evidence_results, all_references)

    return call_gemini(
        SYNTHESIS_PROMPT.format(
            primary_diagnosis=primary_diagnosis,
            evidence_context=evidence_context,
        ),
        max_tokens=4096,
        temperature=0.3,
    )
