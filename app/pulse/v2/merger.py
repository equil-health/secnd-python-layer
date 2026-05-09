"""Dedupe + score + rank multi-source results.

Dedup key precedence: DOI → PMID → (lower-case title, year). Earlier sources
in the input list win — order your adapter list with the most-trusted source
first (PubMed > Europe PMC > OpenAlex > preprints).
"""

from __future__ import annotations

import logging
from typing import Iterable

logger = logging.getLogger(__name__)


def _dedup_key(a: dict) -> str | None:
    if a.get("doi"):
        return f"doi:{a['doi'].lower()}"
    if a.get("pmid"):
        return f"pmid:{a['pmid']}"
    title = (a.get("title") or "").strip().lower()
    if not title:
        return None
    year = (a.get("published_date") or "")[:4]
    return f"title:{title}|{year}"


def merge(buckets: Iterable[list[dict]]) -> list[dict]:
    """Union the per-source result lists, keeping the first occurrence of each key.

    Records the merged provenance in `sources` (list[str]) so downstream code can
    show "found in: PubMed, OpenAlex". The original `source` field is preserved.
    """
    seen: dict[str, dict] = {}
    for bucket in buckets:
        for art in bucket:
            key = _dedup_key(art)
            if key is None:
                continue
            if key not in seen:
                art = dict(art)
                art["sources"] = [art.get("source", "")]
                seen[key] = art
            else:
                src = art.get("source", "")
                if src and src not in seen[key]["sources"]:
                    seen[key]["sources"].append(src)
                # Backfill missing fields from later sources (abstract is the big one)
                for field in ("abstract", "doi", "pmid", "journal", "published_date", "article_url"):
                    if not seen[key].get(field) and art.get(field):
                        seen[key][field] = art[field]
                # Union pub_types
                a_types = set(seen[key].get("pub_types") or [])
                b_types = set(art.get("pub_types") or [])
                if b_types - a_types:
                    seen[key]["pub_types"] = sorted(a_types | b_types)
    merged = list(seen.values())
    logger.info(f"Pulse v2 merge: {sum(len(b) for b in []) if False else ''}{len(merged)} unique articles after dedup")
    return merged
