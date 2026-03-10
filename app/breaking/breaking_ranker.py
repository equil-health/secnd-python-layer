"""Breaking Steps B2, B2.5, B3 — Semantic dedup, Gemini rank, OpenAlex verify, urgency.

B2:   Semantic dedup → Gemini selects top 7 per specialty with snippets + research_topics
B2.5: OpenAlex verify on ranked headlines — filter retractions, enrich with quality data
B3:   Gemini urgency classification (ALERT / MAJOR / NEW)
"""

import json
import logging

from ..config import settings
from ..pipeline.gemini import call_gemini
from ..pipeline.openalex import OpenAlexVerifier
from .semantic_utils import semantic_dedup

logger = logging.getLogger(__name__)


# ── B2: Rank headlines ──────────────────────────────────────────────

RANK_PROMPT = """You are a medical news editor for Indian physicians.

SPECIALTY: {specialty}

Below are {count} medical news headlines from the last 24 hours.
Select the TOP {top_n} most clinically significant headlines.

For each selected headline, provide:
1. The original title (exact match)
2. A 1-2 sentence clinical snippet explaining why this matters
3. A precise research_topic — a clinical research question suitable for deep
   literature review (NOT a restatement of the title)

HEADLINES:
{headlines_text}

Return JSON array only, ordered by clinical significance:
[
  {{
    "title": "exact original title",
    "url": "original url",
    "source": "original source",
    "snippet": "1-2 sentence clinical significance",
    "research_topic": "precise clinical research question for deep review",
    "rank_score": 50-100,
    "rank_position": 1
  }},
  ...
]
"""


def rank_headlines(
    raw_headlines: list[dict],
    specialty: str,
    top_n: int = 7,
) -> list[dict]:
    """B2: Semantic dedup → Gemini rank → top N headlines per specialty.

    Args:
        raw_headlines: Raw headlines from B1 fetcher (typically 20)
        specialty: Medical specialty
        top_n: Number of headlines to select (default 7)

    Returns:
        List of top_n ranked headline dicts
    """
    if not raw_headlines:
        return []

    # Semantic deduplication first
    deduped = semantic_dedup(raw_headlines, threshold=settings.BREAKING_DEDUP_THRESHOLD)
    logger.info(
        f"[Breaking B2] {specialty}: {len(raw_headlines)} raw → "
        f"{len(deduped)} after dedup"
    )

    if len(deduped) <= top_n:
        # Not enough to rank — return all with defaults
        for i, h in enumerate(deduped):
            h.setdefault("rank_score", 50)
            h.setdefault("rank_position", i + 1)
            h.setdefault("snippet", "")
            h.setdefault("research_topic", h.get("title", ""))
        return deduped

    # Format headlines for Gemini
    headlines_text = "\n".join(
        f"{i+1}. [{h.get('source', 'Unknown')}] {h.get('title', '')}\n"
        f"   URL: {h.get('url', '')}"
        for i, h in enumerate(deduped)
    )

    prompt = RANK_PROMPT.format(
        specialty=specialty,
        count=len(deduped),
        top_n=top_n,
        headlines_text=headlines_text,
    )

    try:
        result_text = call_gemini(prompt, max_tokens=2048, temperature=0.1)
        # Clean markdown code fences if present
        result_text = result_text.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1]
        if result_text.endswith("```"):
            result_text = result_text.rsplit("```", 1)[0]
        result_text = result_text.strip()

        ranked = json.loads(result_text)

        # Merge back original data (Gemini may lose fields)
        url_map = {h["url"]: h for h in deduped if h.get("url")}
        enriched = []
        for i, r in enumerate(ranked[:top_n]):
            original = url_map.get(r.get("url"), {})
            merged = {**original, **r}
            merged["rank_position"] = i + 1
            merged["specialty"] = specialty
            enriched.append(merged)

        logger.info(f"[Breaking B2] {specialty}: ranked {len(enriched)} headlines")
        return enriched

    except Exception as e:
        logger.error(f"[Breaking B2] Gemini rank failed for {specialty}: {e}")
        # Fallback: return first top_n deduped headlines
        for i, h in enumerate(deduped[:top_n]):
            h["rank_position"] = i + 1
            h["rank_score"] = 50
            h["specialty"] = specialty
        return deduped[:top_n]


# ── B2.5: OpenAlex verification ─────────────────────────────────────

def verify_breaking_sources(headlines: list[dict]) -> list[dict]:
    """B2.5: Run OpenAlex verification on ranked Breaking headlines.

    Enriches headlines linking to scholarly works (journal URLs).
    Headlines linking to news articles return is_verified=False — expected.
    Retractions are filtered — must not reach the doctor's feed.

    Args:
        headlines: Ranked headlines for one specialty (up to 7)

    Returns:
        Headlines with OpenAlex metadata. Retracted headlines removed.
    """
    verifier = OpenAlexVerifier(
        email=settings.OPENALEX_EMAIL,
        api_key=settings.OPENALEX_API_KEY,
    )
    enriched = []

    for h in headlines:
        result = verifier.verify_single(h.get("url", ""), h.get("title", ""))

        if result:
            h["is_verified"] = True
            h["citation_count"] = result.get("cited_by_count")
            h["quality_tier"] = result.get("quality_tier")
            h["is_retracted"] = result.get("is_retracted", False)
            h["journal"] = result.get("journal")
        else:
            h["is_verified"] = False
            h["citation_count"] = None
            h["quality_tier"] = None
            h["is_retracted"] = False

        if h.get("is_retracted"):
            logger.warning(
                f"[Breaking B2.5] Retracted source filtered. "
                f"URL: {h.get('url', 'unknown')} | Title: {h.get('title', '')[:80]}"
            )
            continue

        enriched.append(h)

    logger.info(
        f"[Breaking B2.5] {len(headlines)} in → {len(enriched)} out "
        f"({len(headlines) - len(enriched)} retracted/filtered)"
    )
    return enriched


# ── B3: Urgency classification ──────────────────────────────────────

URGENCY_PROMPT = """You are a medical urgency classifier for a physician news app.

Classify each headline into exactly one urgency tier:

ALERT — Drug recall, black box warning, trial stopped for patient harm,
        CDSCO/FDA safety communication, drug market withdrawal.
        Expected: 0-1 per day total across all specialties.

MAJOR — Landmark RCT result, major guideline update (AHA/ESC/NMC/WHO),
        new drug first-approval by CDSCO or FDA, large practice-changing
        systematic review. Expected: 1-3 per day.

NEW   — Observational study, updated meta-analysis, expert commentary,
        Phase I/II result, case series. Default when in doubt.

SPECIALTY: {specialty}

HEADLINES:
{headlines_json}

Return JSON array with exactly the same headlines, adding urgency_tier and
urgency_reason (one sentence) to each:
[
  {{
    "title": "exact title",
    "urgency_tier": "ALERT|MAJOR|NEW",
    "urgency_reason": "one sentence reason"
  }},
  ...
]
"""


def assign_urgency(headlines: list[dict], specialty: str) -> list[dict]:
    """B3: Classify urgency tier for each headline via Gemini.

    Args:
        headlines: Verified headlines from B2.5 (up to 7)
        specialty: Medical specialty

    Returns:
        Headlines with urgency_tier and urgency_reason added.
    """
    if not headlines:
        return []

    headlines_json = json.dumps(
        [
            {
                "title": h.get("title", ""),
                "snippet": h.get("snippet", ""),
                "source": h.get("source", ""),
                "is_verified": h.get("is_verified", False),
                "citation_count": h.get("citation_count"),
                "quality_tier": h.get("quality_tier"),
            }
            for h in headlines
        ],
        indent=2,
    )

    prompt = URGENCY_PROMPT.format(
        specialty=specialty,
        headlines_json=headlines_json,
    )

    try:
        result_text = call_gemini(prompt, max_tokens=1024, temperature=0.1)
        result_text = result_text.strip()
        if result_text.startswith("```"):
            result_text = result_text.split("\n", 1)[-1]
        if result_text.endswith("```"):
            result_text = result_text.rsplit("```", 1)[0]
        result_text = result_text.strip()

        classified = json.loads(result_text)

        # Merge urgency back into headlines (match by title)
        title_map = {c["title"]: c for c in classified}
        for h in headlines:
            match = title_map.get(h.get("title", ""))
            if match:
                h["urgency_tier"] = match.get("urgency_tier", "NEW")
                h["urgency_reason"] = match.get("urgency_reason", "")
            else:
                h["urgency_tier"] = "NEW"
                h["urgency_reason"] = ""

        alert_count = sum(1 for h in headlines if h.get("urgency_tier") == "ALERT")
        major_count = sum(1 for h in headlines if h.get("urgency_tier") == "MAJOR")
        logger.info(
            f"[Breaking B3] {specialty}: "
            f"{alert_count} ALERT, {major_count} MAJOR, "
            f"{len(headlines) - alert_count - major_count} NEW"
        )
        return headlines

    except Exception as e:
        logger.error(f"[Breaking B3] Urgency classification failed for {specialty}: {e}")
        for h in headlines:
            h.setdefault("urgency_tier", "NEW")
            h.setdefault("urgency_reason", "")
        return headlines
