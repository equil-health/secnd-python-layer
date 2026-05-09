"""SourceAdapter protocol — every v2 adapter conforms to this contract.

Adapters return dicts in the SAME shape v1 produces, so downstream code
(tldr_generator, tasks.py persistence) is unchanged.

Required keys per article dict:
    pmid, title, authors (list[str]), journal, doi, abstract,
    published_date (YYYY-MM-DD str | ""), pub_types (list[str]),
    article_url, source (str — set by adapter, e.g. "pubmed", "openalex")

Optional (filled later by enrichment):
    evidence_grade, relevance_score, access_strategy, tldr
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class SourceAdapter(Protocol):
    name: str

    def search(
        self,
        *,
        specialty: str,
        topics: list[str],
        mesh_terms: list[str] | None,
        enabled_journals: list[str] | None,
        days_back: int,
        max_articles: int,
        skip_cache: bool = False,
    ) -> list[dict]:
        ...
