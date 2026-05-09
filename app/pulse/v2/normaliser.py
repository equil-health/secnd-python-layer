"""Normalise heterogeneous ToolUniverse outputs into the v1 article-dict shape.

v1 shape (from abstract_fetcher._parse_pubmed_xml):
    pmid, title, authors (list[str]), journal, doi, abstract,
    published_date (YYYY-MM-DD), pub_types (list[str]), article_url

We add `source` so the merger can dedupe with provenance.
"""

from __future__ import annotations

import re
from typing import Any


def _as_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _author_names(raw: Any) -> list[str]:
    """ToolUniverse returns authors variously: list[str], list[dict{name|given|family}], or string."""
    out: list[str] = []
    for a in _as_list(raw):
        if isinstance(a, str):
            if a.strip():
                out.append(a.strip())
        elif isinstance(a, dict):
            name = a.get("name") or a.get("display_name")
            if not name:
                first = a.get("given") or a.get("forename") or a.get("first") or ""
                last = a.get("family") or a.get("lastname") or a.get("last") or ""
                name = f"{last} {first}".strip()
            if name:
                out.append(name)
    return out


_DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)


def _clean_doi(raw: Any) -> str:
    if not raw:
        return ""
    s = str(raw)
    m = _DOI_RE.search(s)
    return m.group(0) if m else ""


def _coerce_str(val: Any) -> str:
    """Coerce a TU field to a string. Some tools return dicts like
    {'value': 'X'} or {'#text': 'X'} for what should be a scalar — extract
    the inner text before stringifying so we never persist a Python repr."""
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict):
        for key in ("value", "#text", "text", "name", "title", "url", "href", "$"):
            inner = val.get(key)
            if isinstance(inner, str) and inner:
                return inner
        # No recognised inner key — return empty rather than the dict's repr.
        return ""
    if isinstance(val, list):
        for item in val:
            s = _coerce_str(item)
            if s:
                return s
        return ""
    return str(val)


def _strip_html(text: Any) -> str:
    s = _coerce_str(text)
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _published_date(raw: Any) -> str:
    """Coerce to YYYY-MM-DD; fall back to YYYY-01-01 if only year is available."""
    if not raw:
        return ""
    s = str(raw).strip()
    # Already ISO-ish
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    m = re.match(r"^(\d{4})-(\d{1,2})$", s)
    if m:
        y, mo = m.groups()
        return f"{y}-{mo.zfill(2)}-15"
    m = re.match(r"^(\d{4})$", s)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


def _build_url(pmid: str, doi: str, fallback: str = "") -> str:
    if doi:
        return f"https://doi.org/{doi}"
    if pmid:
        return f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
    return fallback or ""


def normalise(record: dict, *, source: str) -> dict | None:
    """Normalise a single tool record. Return None if the record is unusable
    (e.g. missing both title and DOI/PMID)."""
    if not isinstance(record, dict):
        return None

    title = _strip_html(record.get("title") or record.get("name") or "")
    doi = _clean_doi(_coerce_str(record.get("doi") or record.get("DOI")))
    pmid = _coerce_str(record.get("pmid") or record.get("PMID")).strip()
    if not title and not (doi or pmid):
        return None

    abstract = _strip_html(
        record.get("abstract")
        or record.get("abstract_text")
        or record.get("summary")
        or ""
    )
    journal = _strip_html(
        record.get("journal")
        or record.get("venue")
        or record.get("publisher")
        or record.get("container_title")
        or ""
    )
    authors = _author_names(record.get("authors") or record.get("author"))
    # PubMed via TU surfaces dates under pubdate/epubdate/sortpubdate; OpenAlex
    # uses publication_year; Crossref uses created/issued/published-print
    # (each often as a list of dicts). Try a wide set of aliases — the first
    # one that yields a parseable YYYY-MM-DD wins.
    published = ""
    for cand in (
        record.get("published_date"),
        record.get("publication_date"),
        record.get("pubdate"),
        record.get("epubdate"),
        record.get("sortpubdate"),
        record.get("date"),
        record.get("issued"),
        record.get("created"),
        record.get("published-print"),
        record.get("published-online"),
        record.get("publication_year"),
        record.get("year"),
    ):
        if not cand:
            continue
        # Crossref-style {'date-parts': [[2024, 3, 5]]}
        if isinstance(cand, dict):
            dp = cand.get("date-parts")
            if isinstance(dp, list) and dp and isinstance(dp[0], list) and dp[0]:
                parts = dp[0]
                cand = "-".join(str(int(p)) for p in parts[:3])
            else:
                cand = _coerce_str(cand)
        elif isinstance(cand, list) and cand:
            cand = _coerce_str(cand[0])
        parsed = _published_date(cand)
        if parsed:
            published = parsed
            break
    pub_types = [
        _coerce_str(p) for p in _as_list(record.get("pub_types") or record.get("type"))
        if _coerce_str(p)
    ]

    article_url = _coerce_str(
        record.get("article_url")
        or record.get("url")
        or record.get("link")
    ) or _build_url(pmid, doi)

    return {
        "pmid": pmid,
        "title": title,
        "authors": authors,
        "journal": journal,
        "doi": doi,
        "abstract": abstract,
        "published_date": published,
        "pub_types": pub_types,
        "article_url": article_url,
        "source": source,
    }


def normalise_many(records: list, *, source: str) -> list[dict]:
    out = []
    for r in records or []:
        n = normalise(r, source=source)
        if n is not None:
            out.append(n)
    return out
