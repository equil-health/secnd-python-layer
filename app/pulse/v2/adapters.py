"""Per-source adapters built on top of ToolUniverse.

The real tool names in ToolUniverse use snake_case suffixes like
*_search_articles / *_search_papers / *_search_works (verified on
v with 2266 loaded tools). Adapters introspect each tool's schema at
runtime to map our generic ('query', 'limit') onto whatever parameter
names that specific tool happens to expose — so we don't have to
hard-code one set of names per tool.

Adapters never raise — failures are logged and they return [].
"""

from __future__ import annotations

import logging
from typing import Any

from .normaliser import normalise_many
from .tooluniverse_client import get_tool_schema, run_tool

logger = logging.getLogger(__name__)


# Candidate parameter names we'll try for each logical slot, in order.
# The first one that appears in the tool's schema wins.
QUERY_PARAM_CANDIDATES = (
    "query", "search", "q", "search_term", "search_query",
    "keyword", "keywords", "search_keywords", "term", "text", "topic",
)
LIMIT_PARAM_CANDIDATES = (
    "limit", "max_results", "per_page", "n_results", "top_k", "size",
    "page_size", "num_results", "count", "rows",
)


def _free_text_query(specialty: str, topics: list[str], mesh_terms: list[str] | None) -> str:
    parts: list[str] = []
    if specialty:
        parts.append(specialty)
    if topics:
        parts.extend(topics)
    if mesh_terms:
        parts.extend(mesh_terms)
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
        for k in (
            "results", "data", "papers", "articles", "items", "records",
            "works", "publications", "preprints", "documents", "hits",
        ):
            v = resp.get(k)
            if isinstance(v, list):
                return v
        for k in ("paper", "article", "record", "work"):
            v = resp.get(k)
            if isinstance(v, dict):
                return [v]
    return []


def _schema_param_names(schema: dict | None) -> set[str]:
    """Pull the set of parameter names from a TU tool schema dict, regardless
    of which schema layout this version uses."""
    if not schema:
        return set()
    out: set[str] = set()
    # Common shapes: schema['parameter']['properties'] (OpenAI-style),
    # schema['parameters']['properties'], or schema['arguments'] dict, or list of {name}.
    candidates = (
        schema.get("parameter"),
        schema.get("parameters"),
        schema.get("arguments"),
        schema.get("input_schema"),
    )
    for cand in candidates:
        if isinstance(cand, dict):
            props = cand.get("properties")
            if isinstance(props, dict):
                out.update(props.keys())
            else:
                # arguments-as-dict: keys are param names
                out.update(k for k in cand.keys() if not k.startswith("$"))
        elif isinstance(cand, list):
            for item in cand:
                if isinstance(item, dict) and "name" in item:
                    out.add(item["name"])
    return out


def _pick(name_set: set[str], candidates: tuple[str, ...], fallback: str) -> str:
    for c in candidates:
        if c in name_set:
            return c
    return fallback


class _BaseTUAdapter:
    name: str = ""
    tool_name: str = ""
    # Optional: extra static args specific to this tool (e.g. result_type=lite).
    extra_args: dict[str, Any] = {}

    def search(
        self,
        *,
        specialty: str,
        topics: list[str],
        mesh_terms: list[str] | None,
        enabled_journals: list[str] | None,  # noqa: ARG002
        days_back: int,  # noqa: ARG002
        max_articles: int,
        skip_cache: bool = False,  # noqa: ARG002
    ) -> list[dict]:
        logger.warning(
            f"PULSE_DEBUG adapter[{self.name}]: search() entered, tool={self.tool_name}, "
            f"max_articles={max_articles}"
        )
        query = _free_text_query(specialty, topics, mesh_terms)
        schema = get_tool_schema(self.tool_name)
        if schema is None:
            logger.warning(
                f"PULSE_DEBUG adapter[{self.name}]: tool '{self.tool_name}' not registered — skipping"
            )
            return []

        params = _schema_param_names(schema)
        q_key = _pick(params, QUERY_PARAM_CANDIDATES, "query")
        l_key = _pick(params, LIMIT_PARAM_CANDIDATES, "limit")
        logger.warning(
            f"PULSE_DEBUG adapter[{self.name}]: schema params={sorted(params)}, "
            f"q_key={q_key!r}, l_key={l_key!r}"
        )

        args: dict[str, Any] = {q_key: query, l_key: max_articles}
        # Merge any tool-specific static args, but don't overwrite the keys
        # we just chose (in case a subclass set the same key).
        for k, v in self.extra_args.items():
            args.setdefault(k, v)

        resp = run_tool(self.tool_name, args)
        records = _extract_records(resp)
        normalised = normalise_many(records, source=self.name)
        logger.warning(
            f"PULSE_DEBUG adapter[{self.name}]: tool={self.tool_name} args_keys={list(args.keys())} "
            f"query='{query[:80]}' → {len(records)} raw, {len(normalised)} normalised"
        )
        return normalised


# ── Concrete adapters — names verified against the 2266-tool registry ────


class PubMedAdapter(_BaseTUAdapter):
    name = "pubmed"
    tool_name = "PubMed_search_articles"
    # Without this PubMed returns title+pmid only — abstracts are
    # fetched lazily, and our relevance scorer would always 0 these out.
    extra_args = {"include_abstract": True, "sort": "relevance"}


class EuropePMCAdapter(_BaseTUAdapter):
    name = "europe_pmc"
    tool_name = "EuropePMC_search_articles"


class OpenAlexWorksAdapter(_BaseTUAdapter):
    """Direct works search — broader coverage."""
    name = "openalex"
    tool_name = "openalex_search_works"
    # OpenAlex's native param is `search`; `query` is documented as an
    # alias today but we pin to native to avoid future breakage.
    extra_args = {"sort": "relevance_score:desc"}


class OpenAlexLitAdapter(_BaseTUAdapter):
    """Curated literature-search variant — sometimes tighter results."""
    name = "openalex_lit"
    tool_name = "openalex_literature_search"


class SemanticScholarAdapter(_BaseTUAdapter):
    name = "semantic_scholar"
    tool_name = "SemanticScholar_search_papers"
    # Same problem as PubMed — abstracts arrive only when explicitly asked.
    extra_args = {"include_abstract": True, "sort": "citationCount:desc"}


class CrossrefAdapter(_BaseTUAdapter):
    name = "crossref"
    tool_name = "Crossref_search_works"
    # Crossref has lots of metadata-only / book / dataset records — keep
    # to journal articles with abstracts to compete with EPMC quality.
    extra_args = {"filter": "type:journal-article,has-abstract:true"}


class CoreAdapter(_BaseTUAdapter):
    name = "core"
    tool_name = "CORE_search_papers"


class DOAJAdapter(_BaseTUAdapter):
    name = "doaj"
    tool_name = "DOAJ_search_articles"


class PMCAdapter(_BaseTUAdapter):
    name = "pmc"
    tool_name = "PMC_search_papers"


class DBLPAdapter(_BaseTUAdapter):
    name = "dblp"
    tool_name = "DBLP_search_publications"


class HALAdapter(_BaseTUAdapter):
    name = "hal"
    tool_name = "HAL_search_archive"


class ArXivAdapter(_BaseTUAdapter):
    name = "arxiv"
    tool_name = "ArXiv_search_papers"


# Note: BioRxiv/MedRxiv on this TU build expose only get_preprint /
# list_recent_preprints — no general keyword search. We deliberately
# omit them from the search registry; users who want preprints should
# enable bioRxiv/medRxiv via Europe PMC (which already indexes them).


ADAPTER_REGISTRY: dict[str, type] = {
    "pubmed": PubMedAdapter,
    "europe_pmc": EuropePMCAdapter,
    "openalex": OpenAlexWorksAdapter,
    "openalex_lit": OpenAlexLitAdapter,
    "semantic_scholar": SemanticScholarAdapter,
    "crossref": CrossrefAdapter,
    "core": CoreAdapter,
    "doaj": DOAJAdapter,
    "pmc": PMCAdapter,
    "dblp": DBLPAdapter,
    "hal": HALAdapter,
    "arxiv": ArXivAdapter,
}


def build_adapters(names: list[str]) -> list:
    out = []
    for n in names:
        cls = ADAPTER_REGISTRY.get(n.strip().lower())
        if cls is None:
            logger.warning(f"Pulse v2: unknown source '{n}' — skipping")
            continue
        out.append(cls())
    return out
