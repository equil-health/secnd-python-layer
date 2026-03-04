"""Domain validation — blocklist + Gemini classifier for ambiguous terms.

Defence layer that catches non-medical or ambiguous topics before they
enter the research pipeline.
"""

import json
import re

from .gemini import call_gemini

# Terms that are common in both medicine and tech/AI.
# Each entry has the medical meaning, the non-medical meaning, and
# context hints that, if found in the topic string, resolve the ambiguity
# toward the medical interpretation.
KNOWN_AMBIGUOUS_TERMS: dict[str, dict] = {
    "RAG": {
        "medical": "Recombinase Activating Gene — essential for V(D)J recombination in immune cells",
        "non_medical": "Retrieval-Augmented Generation — an AI/NLP technique",
        "context_hints": [
            "gene", "mutation", "SCID", "immunodeficiency", "lymphocyte",
            "V(D)J", "recombinase", "immune", "deficiency", "recombination",
            "B cell", "T cell", "Omenn",
        ],
    },
    "STORM": {
        "medical": "Stanford Translational Oncology Research in Medicine",
        "non_medical": "STORM AI research framework for article generation",
        "context_hints": [
            "oncology", "translational", "cancer", "tumor", "stanford",
            "chemotherapy", "clinical trial",
        ],
    },
    "BERT": {
        "medical": "Brief Evaluation of Receptive-Expressive Language (speech-language tool)",
        "non_medical": "Bidirectional Encoder Representations from Transformers (NLP model)",
        "context_hints": [
            "language evaluation", "speech", "receptive", "expressive",
            "pediatric", "developmental",
        ],
    },
    "ATLAS": {
        "medical": "Adjuvant Tamoxifen Longer Against Shorter (breast cancer trial)",
        "non_medical": "Various software tools / databases named ATLAS",
        "context_hints": [
            "tamoxifen", "breast cancer", "adjuvant", "endocrine", "trial",
        ],
    },
    "FALCON": {
        "medical": "Clinical context — interpret based on surrounding terms",
        "non_medical": "Falcon LLM — large language model by TII",
        "context_hints": [
            "clinical", "patient", "treatment", "diagnosis", "syndrome",
        ],
    },
    "LLM": {
        "medical": "Large Loop excision of the Myometrium / LLETZ procedure",
        "non_medical": "Large Language Model — AI text generation",
        "context_hints": [
            "excision", "myometrium", "cervical", "LLETZ", "colposcopy",
            "loop", "biopsy",
        ],
    },
    "GPT": {
        "medical": "Glutamic Pyruvic Transaminase (ALT / SGPT liver enzyme)",
        "non_medical": "Generative Pre-trained Transformer — OpenAI language model",
        "context_hints": [
            "transaminase", "liver", "ALT", "SGPT", "hepatic", "enzyme",
            "aminotransferase",
        ],
    },
}


def check_known_ambiguity(topic: str) -> dict | None:
    """Fast static check for known ambiguous terms in the topic.

    Returns ``None`` if no ambiguity is found (or if medical context hints
    resolve the ambiguity).  Otherwise returns a dict::

        {
            "term": "RAG",
            "medical_meaning": "...",
            "non_medical_meaning": "...",
        }
    """
    topic_upper = topic.upper()
    topic_lower = topic.lower()

    for term, info in KNOWN_AMBIGUOUS_TERMS.items():
        # Word-boundary match so "RAG" in "DRAG" doesn't trigger
        if not re.search(rf"\b{re.escape(term)}\b", topic_upper):
            continue

        # If any medical context hint is present, the user almost certainly
        # means the medical interpretation → no ambiguity for this term.
        hints = info.get("context_hints", [])
        if any(hint.lower() in topic_lower for hint in hints):
            continue  # resolved — keep checking other terms

        return {
            "term": term,
            "medical_meaning": info["medical"],
            "non_medical_meaning": info["non_medical"],
        }

    return None


def validate_medical_domain(topic: str, specialty: str = "") -> dict:
    """Ask Gemini whether *topic* is a medical research topic.

    Called only when :func:`check_known_ambiguity` flags something, so this
    is **not** on the hot path for normal submissions.

    Returns::

        {
            "is_medical": bool,
            "confidence": float,        # 0.0 – 1.0
            "medical_interpretation": str,
            "non_medical_interpretation": str,
            "reasoning": str,
        }
    """
    classifier_prompt = f"""You are a medical domain classifier. Determine whether the following
research topic is about medicine / biomedical science or about a non-medical
domain (e.g. computer science, AI/ML, engineering).

Research topic: "{topic}"
{f'Specialty provided: {specialty}' if specialty else ''}

Return EXACTLY this JSON (no markdown backticks):
{{"is_medical": true/false, "confidence": 0.0-1.0, "medical_interpretation": "how this topic would be understood in medicine", "non_medical_interpretation": "how this topic would be understood outside medicine", "reasoning": "brief explanation"}}"""

    raw = call_gemini(classifier_prompt, max_tokens=512, temperature=0.1)

    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        # Conservative fallback: flag as ambiguous so the user is asked
        result = {
            "is_medical": False,
            "confidence": 0.0,
            "medical_interpretation": "Could not determine",
            "non_medical_interpretation": "Could not determine",
            "reasoning": "Gemini response could not be parsed",
        }

    return result
