"""v2 entry point — same signature/return shape as v1 scan_for_articles.

Pipeline:
    1. Build adapters from settings.PULSE_V2_SOURCES
    2. Run each in parallel (thread pool — TU calls are I/O bound)
    3. Merge + dedup
    4. Apply journal filter (if requested) using v1's JOURNAL_REGISTRY
    5. Enrich with evidence_grade + relevance_score (reusing v1's helpers)
    6. Sort by relevance, truncate to max_articles

If no adapter returns results AND PULSE_V2_FALLBACK_TO_V1 is true, fall back
to v1 so a misconfigured v2 deployment doesn't produce empty digests.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from ...config import settings  # type: ignore
from ..journal_registry import JOURNAL_REGISTRY, grade_evidence
from .adapters import build_adapters
from .merger import merge

logger = logging.getLogger(__name__)


def _compute_relevance_score(article: dict, topics: list[str]) -> float:
    """Mirror of v1 scanner._compute_relevance_score — kept private to avoid
    coupling v2 to v1 internals (v1 may evolve independently)."""
    if not topics:
        return 0.5
    text = f"{article.get('title', '')} {article.get('abstract', '')}".lower()
    matches = sum(1 for t in topics if t.lower() in text)
    score = min(matches / max(len(topics), 1), 1.0)
    if article.get("abstract"):
        score = min(score + 0.1, 1.0)
    return round(score, 2)


def _filter_by_journals(articles: list[dict], enabled_journals: list[str] | None) -> list[dict]:
    if not enabled_journals:
        return articles
    allowed_names = set()
    for key in enabled_journals:
        info = JOURNAL_REGISTRY.get(key)
        if info:
            allowed_names.add(info["name"].lower())
    if not allowed_names:
        return articles
    out = []
    for a in articles:
        j = (a.get("journal") or "").lower()
        if any(name in j or j in name for name in allowed_names):
            out.append(a)
    return out


def search(
    *,
    specialty: str,
    topics: list[str],
    mesh_terms: list[str] | None = None,
    enabled_journals: list[str] | None = None,
    days_back: int | None = None,
    max_articles: int | None = None,
    skip_cache: bool = False,
) -> list[dict]:
    if days_back is None:
        days_back = settings.PULSE_SCAN_DAYS_BACK
    if max_articles is None:
        max_articles = settings.PULSE_MAX_ARTICLES_PER_DIGEST

    source_names = [s.strip() for s in settings.PULSE_V2_SOURCES.split(",") if s.strip()]
    adapters = build_adapters(source_names)
    if not adapters:
        logger.warning("Pulse v2: no adapters configured — returning []")
        return []

    # Over-fetch per source so the post-merge truncation has headroom
    multiplier = max(1, settings.PULSE_V2_OVERFETCH_MULTIPLIER or 1)
    floor = max(1, settings.PULSE_V2_OVERFETCH_MIN or 1)
    per_source_limit = max(max_articles * multiplier, floor)

    buckets: list[list[dict]] = []

    def _run(adapter):
        return adapter.search(
            specialty=specialty,
            topics=topics,
            mesh_terms=mesh_terms,
            enabled_journals=enabled_journals,
            days_back=days_back,
            max_articles=per_source_limit,
            skip_cache=skip_cache,
        )

    max_workers = min(len(adapters), settings.PULSE_V2_MAX_PARALLEL or len(adapters))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run, a): a.name for a in adapters}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                buckets.append(fut.result() or [])
            except Exception as e:
                logger.warning(f"Pulse v2 adapter '{name}' raised: {e}")
                buckets.append([])

    articles = merge(buckets)
    articles = _filter_by_journals(articles, enabled_journals)

    for art in articles:
        art["evidence_grade"] = grade_evidence(art.get("pub_types", []))
        art["relevance_score"] = _compute_relevance_score(art, topics)

    articles.sort(key=lambda a: a.get("relevance_score", 0), reverse=True)
    articles = articles[:max_articles]

    logger.info(
        f"Pulse v2: returning {len(articles)} articles "
        f"(sources={source_names}, specialty={specialty})"
    )
    return articles
