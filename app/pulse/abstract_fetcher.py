"""Abstract fetcher — PubMed E-Utilities + Crossref client with Redis cache."""

import hashlib
import json
import time
import logging
import xml.etree.ElementTree as ET

import requests
import redis

from ..config import settings
from .journal_registry import USER_AGENT, record_error, is_domain_blacklisted

logger = logging.getLogger(__name__)

_redis = None


def _get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(settings.REDIS_URL)
    return _redis

PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
CROSSREF_BASE = "https://api.crossref.org/works"
CACHE_TTL = 86400  # 24 hours


class AbstractFetcher:
    """Fetches article metadata from PubMed E-Utilities and Crossref."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._has_api_key = bool(settings.NCBI_API_KEY)
        self._rate_delay = 0.1 if self._has_api_key else 1.0

    def _pubmed_params(self) -> dict:
        """Common PubMed API params."""
        params = {"tool": "secnd", "email": settings.NCBI_EMAIL}
        if self._has_api_key:
            params["api_key"] = settings.NCBI_API_KEY
        return params

    def _rate_limit(self):
        time.sleep(self._rate_delay)

    # ── E-Search ─────────────────────────────────────────────────

    def search_pubmed(self, query: str, date_start: str, date_end: str, max_results: int = 20, skip_cache: bool = False) -> list[str]:
        """Search PubMed and return list of PMIDs.

        Args:
            query: PubMed search query string
            date_start: YYYY/MM/DD format
            date_end: YYYY/MM/DD format
            max_results: maximum number of results
            skip_cache: bypass Redis cache (used after preference changes)
        """
        cache_key = f"pulse:search:{hashlib.md5(f'{query}:{date_start}:{date_end}:{max_results}'.encode()).hexdigest()}"
        if not skip_cache:
            cached = _get_redis().get(cache_key)
            if cached:
                return json.loads(cached)

        params = {
            **self._pubmed_params(),
            "db": "pubmed",
            "term": query,
            "retmax": max_results,
            "retmode": "json",
            "sort": "relevance",
            "datetype": "pdat",
            "mindate": date_start,
            "maxdate": date_end,
        }

        self._rate_limit()
        try:
            resp = self.session.get(f"{PUBMED_BASE}/esearch.fcgi", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            pmids = data.get("esearchresult", {}).get("idlist", [])
            _get_redis().setex(cache_key, CACHE_TTL, json.dumps(pmids))
            return pmids
        except Exception as e:
            logger.error(f"PubMed E-Search failed: {e}")
            record_error("eutils.ncbi.nlm.nih.gov")
            return []

    # ── E-Fetch ──────────────────────────────────────────────────

    def fetch_pubmed_articles(self, pmids: list[str]) -> list[dict]:
        """Fetch article metadata for a batch of PMIDs via E-Fetch XML."""
        if not pmids:
            return []

        # Check cache for individual PMIDs
        results = []
        uncached_pmids = []
        for pmid in pmids:
            cached = _get_redis().get(f"pulse:article:{pmid}")
            if cached:
                results.append(json.loads(cached))
            else:
                uncached_pmids.append(pmid)

        if not uncached_pmids:
            return results

        if is_domain_blacklisted("eutils.ncbi.nlm.nih.gov"):
            logger.warning("PubMed E-Fetch domain blacklisted by circuit breaker")
            return results

        params = {
            **self._pubmed_params(),
            "db": "pubmed",
            "id": ",".join(uncached_pmids),
            "retmode": "xml",
        }

        self._rate_limit()
        try:
            resp = self.session.get(f"{PUBMED_BASE}/efetch.fcgi", params=params, timeout=60)
            resp.raise_for_status()
            articles = self._parse_pubmed_xml(resp.text)
            for article in articles:
                _get_redis().setex(f"pulse:article:{article['pmid']}", CACHE_TTL, json.dumps(article, default=str))
            results.extend(articles)
        except Exception as e:
            logger.error(f"PubMed E-Fetch failed: {e}")
            record_error("eutils.ncbi.nlm.nih.gov")

        return results

    def _parse_pubmed_xml(self, xml_text: str) -> list[dict]:
        """Parse PubMed E-Fetch XML into article dicts."""
        articles = []
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return []

        for article_elem in root.findall(".//PubmedArticle"):
            try:
                medline = article_elem.find("MedlineCitation")
                article = medline.find("Article")
                pmid = medline.findtext("PMID", "")

                # Title
                title = article.findtext("ArticleTitle", "")

                # Authors
                authors = []
                author_list = article.find("AuthorList")
                if author_list is not None:
                    for author in author_list.findall("Author"):
                        last = author.findtext("LastName", "")
                        first = author.findtext("ForeName", "")
                        if last:
                            authors.append(f"{last} {first}".strip())

                # Journal
                journal_elem = article.find("Journal")
                journal = journal_elem.findtext("Title", "") if journal_elem is not None else ""

                # Abstract
                abstract_elem = article.find("Abstract")
                abstract = ""
                if abstract_elem is not None:
                    abstract_parts = []
                    for at in abstract_elem.findall("AbstractText"):
                        label = at.get("Label", "")
                        text = at.text or ""
                        if label:
                            abstract_parts.append(f"{label}: {text}")
                        else:
                            abstract_parts.append(text)
                    abstract = "\n".join(abstract_parts)

                # DOI
                doi = ""
                for eid in article.findall("ELocationID"):
                    if eid.get("EIdType") == "doi":
                        doi = eid.text or ""
                        break

                # Published date
                pub_date = ""
                pd_elem = journal_elem.find(".//PubDate") if journal_elem is not None else None
                if pd_elem is not None:
                    year = pd_elem.findtext("Year", "")
                    month = pd_elem.findtext("Month", "01")
                    day = pd_elem.findtext("Day", "01")
                    # Month might be text like "Jan"
                    month_map = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                                 "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                                 "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
                    month = month_map.get(month, month)
                    if year:
                        pub_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

                # Publication types
                pub_types = []
                pt_list = article.find("PublicationTypeList")
                if pt_list is not None:
                    for pt in pt_list.findall("PublicationType"):
                        if pt.text:
                            pub_types.append(pt.text)

                # Article URL
                article_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
                if doi:
                    article_url = f"https://doi.org/{doi}"

                articles.append({
                    "pmid": pmid,
                    "title": title,
                    "authors": authors,
                    "journal": journal,
                    "doi": doi,
                    "abstract": abstract,
                    "published_date": pub_date,
                    "pub_types": pub_types,
                    "article_url": article_url,
                })
            except Exception as e:
                logger.error(f"Error parsing article: {e}")
                continue

        return articles

    # ── Crossref fallback ────────────────────────────────────────

    def fetch_crossref(self, doi: str) -> dict | None:
        """Fetch abstract from Crossref as fallback when PubMed abstract is missing."""
        if not doi:
            return None

        cache_key = f"pulse:crossref:{doi}"
        cached = _get_redis().get(cache_key)
        if cached:
            return json.loads(cached)

        if is_domain_blacklisted("api.crossref.org"):
            return None

        self._rate_limit()
        try:
            resp = self.session.get(f"{CROSSREF_BASE}/{doi}", timeout=15,
                                    headers={"Accept": "application/json"})
            resp.raise_for_status()
            data = resp.json().get("message", {})
            result = {
                "abstract": data.get("abstract", ""),
                "title": data.get("title", [""])[0] if data.get("title") else "",
            }
            _get_redis().setex(cache_key, CACHE_TTL, json.dumps(result))
            return result
        except Exception as e:
            logger.error(f"Crossref fetch failed for {doi}: {e}")
            record_error("api.crossref.org")
            return None
