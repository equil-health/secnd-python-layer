"""Evidence synthesis — Gemini reviews search results vs claims.

Ported from v5 lines 686-729.
"""

from .gemini import call_gemini
from .prompts import build_medical_prompt

SYNTHESIS_PROMPT = """You are a medical evidence reviewer. A specialist AI (MedGemma) analyzed a clinical case and made several diagnostic claims. We searched medical literature for each claim.

YOUR TASK: For each claim, assess whether the literature SUPPORTS, PARTIALLY SUPPORTS, or CONTRADICTS it. Write a structured evidence review.

IMPORTANT — Evidence Quality:
- Verified papers (found in OpenAlex) carry more weight
- Landmark papers (100+ citations) are strongest evidence
- Preprints are not yet peer-reviewed — note this
- RETRACTED papers MUST be excluded from your conclusions

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
    """Build a text summary of search results for each claim, with verification metadata."""
    # Build a URL -> ref lookup for quick access to verification data
    ref_by_url = {r["url"]: r for r in all_references if r.get("url")}

    context = ""
    for ev in evidence_results:
        context += f"\n### Claim: {ev['claim']}\n"
        context += f"Search: {ev['search_query']}\n"
        for sr in ev.get("search_results", [])[:3]:
            url = sr.get("url", "")
            ref_ids = [r["id"] for r in all_references if r["url"] == url]
            ref_tag = f"[{ref_ids[0]}]" if ref_ids else ""

            # Add verification metadata if available
            ref_data = ref_by_url.get(url, {})
            meta_parts = []
            if ref_data.get("is_retracted"):
                meta_parts.append("RETRACTED")
            elif ref_data.get("is_verified"):
                tier = ref_data.get("quality_tier", "")
                cite_count = ref_data.get("citation_count", 0)
                journal = ref_data.get("journal", "")
                year = ref_data.get("year", "")
                label = f"Verified"
                if cite_count:
                    label += f", Cited {cite_count} times"
                if journal:
                    label += f", {journal}"
                if year:
                    label += f" {year}"
                meta_parts.append(label)
            elif ref_data.get("quality_tier") == "unverified":
                meta_parts.append("Unverified")

            meta_str = f" [{', '.join(meta_parts)}]" if meta_parts else ""
            context += f"- {ref_tag}{meta_str} {sr.get('title', '')}: {sr.get('snippet', '')}\n"
    return context


RESEARCH_SYNTHESIS_PROMPT = """You are a research evidence reviewer. A research pipeline extracted claims from a literature review article and searched for supporting evidence.

YOUR TASK: For each claim, assess whether the literature SUPPORTS, PARTIALLY SUPPORTS, or CONTRADICTS it. Write a structured evidence review.

IMPORTANT — Evidence Quality:
- Verified papers (found in OpenAlex) carry more weight
- Landmark papers (100+ citations) are strongest evidence
- Preprints are not yet peer-reviewed — note this
- RETRACTED papers MUST be excluded from your conclusions

Format your response as a markdown document with this structure:

## Evidence Review: [{primary_topic}]

### Claim 1: [claim text]
**Verdict: SUPPORTED / PARTIALLY SUPPORTED / CONTRADICTED / INSUFFICIENT EVIDENCE**
Evidence: [2-3 sentences synthesizing what the search results show, citing reference numbers like [1], [2]]

### Claim 2: ...
(repeat for each claim)

## Overall Assessment
[3-4 sentences on the strength of evidence for the key findings in this research area]

## Research Gaps
[2-3 sentences identifying areas where evidence is lacking or conflicting]

PRIMARY RESEARCH TOPIC: {primary_topic}

CLAIMS AND SEARCH EVIDENCE:
{evidence_context}

EVIDENCE REVIEW:"""


def synthesize_evidence(
    primary_diagnosis: str,
    evidence_results: list,
    all_references: list,
) -> str:
    """Have Gemini synthesize evidence for/against each claim.

    Returns evidence synthesis as markdown text.
    """
    evidence_context = _build_evidence_context(evidence_results, all_references)

    wrapped_prompt = build_medical_prompt(
        SYNTHESIS_PROMPT.format(
            primary_diagnosis=primary_diagnosis,
            evidence_context=evidence_context,
        )
    )
    return call_gemini(
        wrapped_prompt,
        max_tokens=4096,
        temperature=0.3,
    )


def synthesize_research_evidence(
    primary_topic: str,
    evidence_results: list,
    all_references: list,
) -> str:
    """Have Gemini synthesize evidence for research claims.

    Returns evidence synthesis as markdown text.
    """
    evidence_context = _build_evidence_context(evidence_results, all_references)

    wrapped_prompt = build_medical_prompt(
        RESEARCH_SYNTHESIS_PROMPT.format(
            primary_topic=primary_topic,
            evidence_context=evidence_context,
        )
    )
    return call_gemini(
        wrapped_prompt,
        max_tokens=4096,
        temperature=0.3,
    )
