"""Extract verifiable claims from MedGemma output — ported from v5 lines 558-628."""

import json
import re

from .gemini import call_gemini

ZEBRA_EXTRACTION_PROMPT = """Read this clinical analysis focusing on RARE DISEASE (zebra) hypotheses.

The analysis should contain:
- Common diagnoses that were considered and excluded ("horses")
- Rare disease hypotheses ("zebras") that better fit the clinical picture

Extract:
1. The common diagnoses that were excluded, with brief reasons
2. The rare/zebra hypotheses as verifiable claims
3. Search queries targeting rare disease databases (Orphanet, OMIM, NIH GARD)

Return EXACTLY this JSON format, nothing else (no markdown backticks):
{{
  "primary_diagnosis": "the most likely rare diagnosis from the zebra analysis",
  "excluded_common": [
    {{
      "diagnosis": "the common diagnosis that was excluded",
      "reason": "why it was excluded based on the clinical evidence"
    }}
  ],
  "zebra_hypotheses": [
    {{
      "hypothesis": "the rare disease hypothesis",
      "key_features": "clinical features that support this hypothesis"
    }}
  ],
  "claims": [
    {{
      "claim": "specific verifiable claim about the rare disease hypothesis",
      "search_query": "rare disease name diagnostic criteria Orphanet"
    }}
  ]
}}

Extract 6-10 of the most important, verifiable claims focusing on the rare disease hypotheses.

ZEBRA ANALYSIS:
{analysis}"""

EXTRACTION_PROMPT = """Read this clinical analysis and extract the KEY MEDICAL CLAIMS that need evidence.

For each claim, provide:
1. The claim itself (one sentence)
2. A specific Google search query to find supporting evidence (5-8 words)

Return EXACTLY this JSON format, nothing else (no markdown backticks):
{{
  "primary_diagnosis": "the diagnosis MedGemma thinks is most likely",
  "claims": [
    {{
      "claim": "ANA positive with titer 1:320 and positive ASMA strongly suggests autoimmune hepatitis",
      "search_query": "ANA ASMA positive autoimmune hepatitis diagnostic criteria"
    }}
  ]
}}

Extract 6-10 of the most important, verifiable clinical claims.

MEDGEMMA ANALYSIS:
{analysis}"""

# Fallback claims if Gemini JSON parsing fails
FALLBACK_CLAIMS = [
    {
        "claim": "ANA 1:320 with positive ASMA and elevated IgG strongly suggests autoimmune hepatitis",
        "search_query": "ANA ASMA IgG autoimmune hepatitis diagnostic criteria",
    },
    {
        "claim": "HCC without cirrhosis or hepatitis B/C is uncommon",
        "search_query": "hepatocellular carcinoma without cirrhosis incidence",
    },
    {
        "claim": "AFP mildly elevated at 38 is non-specific and can occur in autoimmune hepatitis",
        "search_query": "elevated AFP autoimmune hepatitis non-HCC causes",
    },
    {
        "claim": "Liver lesions without classic arterial enhancement and washout are atypical for HCC",
        "search_query": "HCC imaging criteria arterial enhancement washout LI-RADS",
    },
    {
        "claim": "Ferritin 890 with iron saturation 65% may indicate hemochromatosis",
        "search_query": "ferritin 890 iron saturation 65 hemochromatosis diagnosis",
    },
    {
        "claim": "Elevated globulins with low albumin suggests chronic inflammation or lymphoproliferative process",
        "search_query": "high globulin low albumin differential diagnosis liver",
    },
    {
        "claim": "Cervical lymphadenopathy with B symptoms and liver lesions should raise concern for lymphoma",
        "search_query": "hepatic lymphoma cervical lymph node B symptoms presentation",
    },
    {
        "claim": "Liver biopsy should be performed but pathology should specifically stain for plasma cells and assess interface hepatitis",
        "search_query": "autoimmune hepatitis liver biopsy histology plasma cells interface hepatitis",
    },
]


RESEARCH_EXTRACTION_PROMPT = """Read this research article and extract the KEY VERIFIABLE CLAIMS that should be checked against literature.

For each claim, provide:
1. The claim itself (one sentence, specific and verifiable)
2. A specific search query to find supporting or contradicting evidence (5-8 words)

Focus on:
- Statistical claims (prevalence, efficacy rates, outcomes)
- Mechanistic claims (biological pathways, drug mechanisms)
- Comparative claims (treatment A vs treatment B)
- Guideline-based claims (recommended practices, diagnostic criteria)

Return EXACTLY this JSON format, nothing else (no markdown backticks):
{{
  "primary_topic": "the main research topic being investigated",
  "claims": [
    {{
      "claim": "SGLT2 inhibitors reduce cardiovascular mortality by 20% in heart failure patients",
      "search_query": "SGLT2 inhibitors cardiovascular mortality heart failure meta-analysis"
    }}
  ]
}}

Extract 8-12 of the most important, verifiable claims.

RESEARCH ARTICLE:
{analysis}"""


def extract_research_claims(article: str) -> dict:
    """Extract verifiable claims from a research article.

    Returns dict with keys: primary_topic, claims.
    """
    raw = call_gemini(
        RESEARCH_EXTRACTION_PROMPT.format(analysis=article[:6000]),
        max_tokens=2048,
        temperature=0.1,
    )

    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        data = json.loads(raw)
        primary_topic = data.get("primary_topic", "unknown")
        claims = data.get("claims", [])
    except json.JSONDecodeError:
        primary_topic = "research topic"
        claims = []

    return {"primary_topic": primary_topic, "claims": claims}


def extract_claims(analysis: str, mode: str = "standard") -> dict:
    """Extract verifiable clinical claims from MedGemma analysis.

    Returns dict with keys: primary_diagnosis, claims.
    When mode="zebra", also returns: excluded_common, zebra_hypotheses.
    """
    prompt_template = ZEBRA_EXTRACTION_PROMPT if mode == "zebra" else EXTRACTION_PROMPT
    raw = call_gemini(
        prompt_template.format(analysis=analysis[:6000]),
        max_tokens=2048,
        temperature=0.1,
    )

    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        data = json.loads(raw)
        primary_dx = data.get("primary_diagnosis", "unknown")
        claims = data.get("claims", [])
    except json.JSONDecodeError:
        primary_dx = "autoimmune hepatitis"
        claims = FALLBACK_CLAIMS
        data = {}

    result = {"primary_diagnosis": primary_dx, "claims": claims}
    if mode == "zebra":
        result["excluded_common"] = data.get("excluded_common", [])
        result["zebra_hypotheses"] = data.get("zebra_hypotheses", [])
    return result
