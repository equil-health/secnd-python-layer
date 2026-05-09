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


def _compute_relevance_score(
    article: dict, topics: list[str], specialty: str = ""
) -> float:
    """Score article relevance on [0, 1].

    When `topics` is non-empty the score is the fraction of topic terms
    present in title+abstract, plus a 0.1 bonus for having an abstract at all.

    When `topics` is empty (e.g. a specialty-only digest) we fall back to a
    specialty-keyword scan so the sort still has signal — otherwise every
    article tied at the same score and Python's stable sort just preserved
    insertion order, which made the first-listed source win every slot.
    """
    text = f"{article.get('title', '')} {article.get('abstract', '')}".lower()
    if topics:
        matches = sum(1 for t in topics if t.lower() in text)
        score = min(matches / max(len(topics), 1), 1.0)
    elif specialty:
        # Specialty-only mode: reward articles that actually mention the
        # specialty (or a single token of it) somewhere in the text.
        spec = specialty.lower().strip()
        tokens = [tok for tok in spec.split() if len(tok) > 3]
        hit_full = 1 if spec and spec in text else 0
        hit_tok = sum(1 for tok in tokens if tok in text)
        denom = 1 + len(tokens)
        score = min((hit_full + hit_tok) / denom, 1.0) if denom else 0.0
    else:
        score = 0.0
    if article.get("abstract"):
        score = min(score + 0.1, 1.0)
    return round(score, 2)


def _interleave_by_source(articles: list[dict], max_n: int) -> list[dict]:
    """Round-robin draw across sources so the final list reflects every
    adapter that returned results — not just whichever one happened to
    sit first in PULSE_V2_SOURCES.

    Within each source, the input order (already sorted by relevance) is
    preserved, so we still surface the best article from each source first.
    """
    if max_n <= 0 or not articles:
        return []
    by_src: dict[str, list[dict]] = {}
    src_order: list[str] = []  # preserve first-seen order for determinism
    for a in articles:
        primary = ""
        srcs = a.get("sources")
        if isinstance(srcs, list) and srcs:
            primary = srcs[0] or ""
        if not primary:
            primary = a.get("source") or ""
        primary = primary or "unknown"
        if primary not in by_src:
            by_src[primary] = []
            src_order.append(primary)
        by_src[primary].append(a)

    out: list[dict] = []
    idx = 0
    while len(out) < max_n:
        added = False
        for src in src_order:
            if idx < len(by_src[src]) and len(out) < max_n:
                out.append(by_src[src][idx])
                added = True
        if not added:
            break
        idx += 1
    return out


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
    logger.warning(
        f"PULSE_DEBUG search: ENTRY specialty={specialty!r} topics={topics} "
        f"mesh={mesh_terms} journals={enabled_journals} max={max_articles} "
        f"sources={source_names} version={settings.PULSE_VERSION}"
    )
    adapters = build_adapters(source_names)
    logger.warning(
        f"PULSE_DEBUG search: built {len(adapters)} adapters: "
        f"{[a.name for a in adapters]}"
    )
    if not adapters:
        logger.warning("PULSE_DEBUG search: no adapters configured — returning []")
        return []

    # Over-fetch per source so the post-merge truncation has headroom
    multiplier = max(1, settings.PULSE_V2_OVERFETCH_MULTIPLIER or 1)
    floor = max(1, settings.PULSE_V2_OVERFETCH_MIN or 1)
    per_source_limit = max(max_articles * multiplier, floor)

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

    # Preserve PULSE_V2_SOURCES order so the merger's "first-source-wins"
    # tie-breaker reflects user-configured priority, not whoever was fastest.
    max_workers = min(len(adapters), settings.PULSE_V2_MAX_PARALLEL or len(adapters))
    buckets: list[list[dict]] = [[] for _ in adapters]
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run, a): i for i, a in enumerate(adapters)}
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                buckets[i] = fut.result() or []
            except Exception as e:
                logger.warning(
                    f"Pulse v2 adapter '{adapters[i].name}' raised: {e}"
                )
                buckets[i] = []

    per_bucket = {adapters[i].name: len(buckets[i]) for i in range(len(adapters))}
    logger.warning(f"PULSE_DEBUG search: per-source buckets after fan-out: {per_bucket}")

    # PULSE_DEBUG: sample one record's identifier fields per source so we can
    # see WHY the merger isn't matching across sources (DOI/PMID extraction).
    for i, adapter in enumerate(adapters):
        if buckets[i]:
            sample = buckets[i][0]
            logger.warning(
                f"PULSE_DEBUG dedup-keys[{adapter.name}]: "
                f"doi={sample.get('doi')!r} pmid={sample.get('pmid')!r} "
                f"title={(sample.get('title') or '')[:60]!r} "
                f"date={sample.get('published_date')!r}"
            )

    articles = merge(buckets)
    logger.warning(f"PULSE_DEBUG search: after merge, {len(articles)} unique articles")
    pre_filter = len(articles)
    articles = _filter_by_journals(articles, enabled_journals)
    logger.warning(
        f"PULSE_DEBUG search: after journal filter ({enabled_journals}): "
        f"{pre_filter} -> {len(articles)}"
    )

    for art in articles:
        art["evidence_grade"] = grade_evidence(art.get("pub_types", []))
        art["relevance_score"] = _compute_relevance_score(art, topics, specialty)

    articles.sort(key=lambda a: a.get("relevance_score", 0), reverse=True)

    # Round-robin across sources before truncation — otherwise stable-sort on
    # tied scores hands every slot to whichever source is first in
    # PULSE_V2_SOURCES, producing a "PubMed-only" result even when 5 adapters
    # returned data. See _interleave_by_source for details.
    pre_interleave = len(articles)
    articles = _interleave_by_source(articles, max_articles)
    logger.warning(
        f"PULSE_DEBUG search: interleave {pre_interleave} -> {len(articles)} "
        f"(max_articles={max_articles})"
    )

    src_tally: dict[str, int] = {}
    for a in articles:
        for s in (a.get("sources") or [a.get("source", "")]):
            if s:
                src_tally[s] = src_tally.get(s, 0) + 1
    logger.warning(
        f"PULSE_DEBUG search: RETURNING {len(articles)} articles, "
        f"src_tally={src_tally} (sources_cfg={source_names}, specialty={specialty})"
    )
    return articles
