"""Per-source adapters built on top of ToolUniverse.

Each adapter is small: build the args, call the tool, normalise the response.
Adapters never raise — failures are logged and they return [].
"""

from __future__ import annotations

import logging
from typing import Any

from .base import SourceAdapter  # noqa: F401  (kept for type clarity)
from .normaliser import normalise_many
from .tooluniverse_client import run_tool

logger = logging.getLogger(__name__)


def _free_text_query(specialty: str, topics: list[str], mesh_terms: list[str] | None) -> str:
    """Most ToolUniverse adapters take a free-text query, not PubMed boolean syntax."""
    parts: list[str] = []
    if specialty:
        parts.append(specialty)
    if topics:
        parts.extend(topics)
    if mesh_terms:
        parts.extend(mesh_terms)
    # de-dupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        k = p.lower().strip()
        if k and k not in seen:
            seen.add(k)
            out.append(p)
    return " ".join(out)


def _extract_records(resp: Any) -> list:
    """Normalise the wrapper shape — TU tools return either a list or a dict
    with a 'results' / 'data' / 'papers' key depending on the tool."""
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in ("results", "data", "papers", "articles", "items", "records"):
            v = resp.get(k)
            if isinstance(v, list):
                return v
        # Some tools return {'paper': {...}} for single-result lookups
        for k in ("paper", "article", "record"):
            v = resp.get(k)
            if isinstance(v, dict):
                return [v]
    return []


# ── Adapters ─────────────────────────────────────────────────────


class _BaseTUAdapter:
    name: str = ""
    tool_name: str = ""

    def search(
        self,
        *,
        specialty: str,
        topics: list[str],
        mesh_terms: list[str] | None,
        enabled_journals: list[str] | None,  # noqa: ARG002 (per-source filtering varies)
        days_back: int,  # noqa: ARG002 (most TU tools accept year not days)
        max_articles: int,
        skip_cache: bool = False,  # noqa: ARG002 (TU has its own caching)
    ) -> list[dict]:
        query = _free_text_query(specialty, topics, mesh_terms)
        args = self._build_args(query=query, max_articles=max_articles)
        resp = run_tool(self.tool_name, args)
        records = _extract_records(resp)
        normalised = normalise_many(records, source=self.name)
        logger.info(
            f"Pulse v2 [{self.name}] query='{query[:80]}' → {len(records)} raw, "
            f"{len(normalised)} normalised"
        )
        return normalised

    def _build_args(self, *, query: str, max_articles: int) -> dict:
        return {"query": query, "limit": max_articles}


class PubMedAdapter(_BaseTUAdapter):
    name = "pubmed"
    tool_name = "PubMed"


class EuropePMCAdapter(_BaseTUAdapter):
    name = "europe_pmc"
    tool_name = "EuropePMC"


class OpenAlexAdapter(_BaseTUAdapter):
    name = "openalex"
    tool_name = "OpenAlex"


class SemanticScholarAdapter(_BaseTUAdapter):
    name = "semantic_scholar"
    tool_name = "SemanticScholar"


class CrossrefAdapter(_BaseTUAdapter):
    name = "crossref"
    tool_name = "Crossref"


class BioRxivAdapter(_BaseTUAdapter):
    name = "biorxiv"
    tool_name = "BioRxiv"


class MedRxivAdapter(_BaseTUAdapter):
    name = "medrxiv"
    tool_name = "MedRxiv"


# Registry — keys are the names you use in PULSE_V2_SOURCES env var.
ADAPTER_REGISTRY: dict[str, type] = {
    "pubmed": PubMedAdapter,
    "europe_pmc": EuropePMCAdapter,
    "openalex": OpenAlexAdapter,
    "semantic_scholar": SemanticScholarAdapter,
    "crossref": CrossrefAdapter,
    "biorxiv": BioRxivAdapter,
    "medrxiv": MedRxivAdapter,
}


def build_adapters(names: list[str]) -> list:
    """Resolve adapter names → instances, skipping unknown names with a warning."""
    out = []
    for n in names:
        cls = ADAPTER_REGISTRY.get(n.strip().lower())
        if cls is None:
            logger.warning(f"Pulse v2: unknown source '{n}' — skipping")
            continue
        out.append(cls())
    return out
