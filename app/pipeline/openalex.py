"""OpenAlex citation verifier — verify references against 250M+ scholarly works.

Provides DOI/PMID/title lookup, batch verification, quality tiers, and Redis caching.
See docs/openalex_implementation.md for full spec.
"""

import hashlib
import json
import logging
import re
import time

import redis
import requests

from ..config import settings

logger = logging.getLogger(__name__)

_redis = redis.Redis.from_url(settings.REDIS_URL)
CACHE_TTL = 86400  # 24 hours

# Domains that are authoritative but not scholarly papers — skip verification
SKIP_DOMAINS = {
    "mayoclinic.org",
    "wikipedia.org",
    "cdc.gov",
    "who.int",
    "uptodate.com",
    "medlineplus.gov",
    "nih.gov",
    "webmd.com",
    "healthline.com",
    "drugs.com",
    "fda.gov",
    "nhs.uk",
    "clevelandclinic.org",
    "hopkinsmedicine.org",
    "merckmanuals.com",
    "orpha.net",
    "omim.org",
    "rarediseases.org",
    "rarediseases.info.nih.gov",
}

# Fields to request from OpenAlex API for efficiency
SELECT_FIELDS = (
    "id,doi,title,publication_year,type,cited_by_count,"
    "is_retracted,is_paratext,primary_location,authorships,open_access"
)


class OpenAlexVerifier:
    """Verify references against the OpenAlex database."""

    def __init__(self, email: str = "", api_key: str = ""):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": f"SECND-Pipeline/1.0 (mailto:{email})" if email else "SECND-Pipeline/1.0",
        })
        self.api_key = api_key
        self._last_request_time = 0.0

    def _get(self, url: str, params: dict | None = None) -> dict | None:
        """Rate-limited GET request to OpenAlex API."""
        # Polite rate limiting: max 10 req/s
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)

        if params is None:
            params = {}
        params["select"] = SELECT_FIELDS
        if self.api_key:
            params["api_key"] = self.api_key

        self._last_request_time = time.time()
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"[OpenAlex] HTTP {resp.status_code} for {url}")
        except requests.Timeout:
            logger.warning(f"[OpenAlex] Timeout (15s) for {url}")
        except Exception as e:
            logger.warning(f"[OpenAlex] Request failed: {e}")
        return None

    def _should_skip(self, url: str) -> bool:
        """Check if URL is a non-scholarly domain that should skip verification."""
        if not url:
            return True
        url_lower = url.lower()
        for domain in SKIP_DOMAINS:
            if domain in url_lower:
                return True
        return False

    def _extract_doi(self, url: str) -> str | None:
        """Extract DOI from common URL patterns."""
        if not url:
            return None
        # Direct doi.org links
        m = re.search(r"doi\.org/(10\.\d{4,}/[^\s&?#]+)", url)
        if m:
            return m.group(1)
        # ScienceDirect
        m = re.search(r"sciencedirect\.com/science/article/pii/([A-Z0-9]+)", url, re.I)
        if m:
            return None  # PII, not DOI — need title search
        # Springer / Nature
        m = re.search(r"(?:springer|nature)\.com/articles?/(10\.\d{4,}/[^\s&?#]+)", url)
        if m:
            return m.group(1)
        # Wiley
        m = re.search(r"onlinelibrary\.wiley\.com/doi/(10\.\d{4,}/[^\s&?#]+)", url)
        if m:
            return m.group(1)
        # Generic DOI pattern in URL
        m = re.search(r"(10\.\d{4,}/[^\s&?#]+)", url)
        if m:
            return m.group(1)
        return None

    def _extract_pmid(self, url: str) -> str | None:
        """Extract PubMed ID from URL."""
        if not url:
            return None
        m = re.search(r"pubmed\.ncbi\.nlm\.nih\.gov/(\d+)", url)
        if m:
            return m.group(1)
        m = re.search(r"ncbi\.nlm\.nih\.gov/pubmed/(\d+)", url)
        if m:
            return m.group(1)
        return None

    def _parse_work(self, work: dict) -> dict:
        """Extract structured data from an OpenAlex work object."""
        # Journal info from primary_location
        journal = None
        issn = None
        impact_factor = None
        location = work.get("primary_location") or {}
        source = location.get("source") or {}
        if source:
            journal = source.get("display_name")
            issn = source.get("issn_l")
            # OpenAlex doesn't provide impact factor directly; we skip it

        # Open access
        oa = work.get("open_access") or {}
        is_oa = oa.get("is_oa", False)
        oa_url = oa.get("oa_url")

        # Authors (first 3)
        authorships = work.get("authorships") or []
        authors = []
        for auth in authorships[:3]:
            author_obj = auth.get("author") or {}
            name = author_obj.get("display_name")
            if name:
                authors.append(name)

        doi_raw = work.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "") if doi_raw else None

        return {
            "openalex_id": work.get("id"),
            "doi": doi,
            "title": work.get("title"),
            "year": work.get("publication_year"),
            "type": work.get("type"),
            "cited_by_count": work.get("cited_by_count", 0),
            "is_retracted": work.get("is_retracted", False),
            "is_paratext": work.get("is_paratext", False),
            "journal": journal,
            "issn": issn,
            "impact_factor": impact_factor,
            "is_oa": is_oa,
            "oa_url": oa_url,
            "authors": authors,
        }

    def verify_single(self, url: str, title: str = "") -> dict | None:
        """Verify a single reference. Returns OpenAlex data or None.

        Lookup chain: DOI -> PMID -> title search.
        """
        # Check cache first
        cache_input = f"{url}|{title}"
        cache_key = f"openalex:{hashlib.md5(cache_input.encode()).hexdigest()}"
        try:
            cached = _redis.get(cache_key)
            if cached:
                data = json.loads(cached)
                return data if data else None
        except redis.ConnectionError:
            pass

        result = None

        # Try DOI lookup
        doi = self._extract_doi(url)
        if doi:
            data = self._get(f"https://api.openalex.org/works/doi:{doi}")
            if data and data.get("id"):
                result = self._parse_work(data)

        # Try PMID lookup
        if not result:
            pmid = self._extract_pmid(url)
            if pmid:
                data = self._get(f"https://api.openalex.org/works/pmid:{pmid}")
                if data and data.get("id"):
                    result = self._parse_work(data)

        # Try title search
        if not result and title:
            data = self._get(
                "https://api.openalex.org/works",
                params={"search": title, "per_page": "3"},
            )
            if data and data.get("results"):
                for candidate in data["results"]:
                    candidate_title = candidate.get("title") or ""
                    if self._title_match(title, candidate_title):
                        result = self._parse_work(candidate)
                        break

        # Cache result (even None, to avoid repeated lookups)
        try:
            _redis.setex(cache_key, CACHE_TTL, json.dumps(result or {}))
        except redis.ConnectionError:
            pass

        return result

    def verify_batch_dois(self, dois: list[str]) -> dict[str, dict]:
        """Batch-verify up to 50 DOIs using pipe separator.

        Returns dict mapping DOI -> parsed work data.
        """
        if not dois:
            return {}

        results = {}
        # OpenAlex supports filter with pipe-separated DOIs, max ~50
        for i in range(0, len(dois), 50):
            batch = dois[i:i + 50]
            doi_filter = "|".join(f"https://doi.org/{d}" for d in batch)
            data = self._get(
                "https://api.openalex.org/works",
                params={"filter": f"doi:{doi_filter}", "per_page": "50"},
            )
            if data and data.get("results"):
                for work in data["results"]:
                    parsed = self._parse_work(work)
                    if parsed["doi"]:
                        results[parsed["doi"].lower()] = parsed

        return results

    def _title_match(self, query_title: str, result_title: str) -> bool:
        """Check if titles match with >= 60% word overlap."""
        if not query_title or not result_title:
            return False
        q_words = set(query_title.lower().split())
        r_words = set(result_title.lower().split())
        if not q_words:
            return False
        overlap = len(q_words & r_words) / len(q_words)
        return overlap >= 0.6

    def verify_all(self, references: list[dict]) -> list[dict]:
        """Verify all references with batch DOI optimization.

        Phase 1: Batch DOI lookup for all refs with extractable DOIs.
        Phase 2: Individual lookups for remaining refs (PMID/title search).
        Phase 3: Sort by quality tier.

        Returns enriched references list with verification data added.
        """
        total = len(references)
        logger.info(f"[OpenAlex] Starting verification of {total} references")

        # Phase 1: Extract DOIs and batch-verify
        doi_map = {}  # ref_index -> doi
        skipped = 0
        for i, ref in enumerate(references):
            if self._should_skip(ref.get("url", "")):
                ref["is_verified"] = False
                ref["quality_tier"] = "guideline" if not self._is_generic_skip(ref.get("url", "")) else "other"
                ref["verification_skipped"] = True
                skipped += 1
                continue
            doi = self._extract_doi(ref.get("url", ""))
            if doi:
                doi_map[i] = doi

        # Batch DOI lookup
        unique_dois = list(set(doi_map.values()))
        logger.info(f"[OpenAlex] Phase 1: {skipped} skipped, {len(unique_dois)} DOIs for batch lookup")
        batch_results = self.verify_batch_dois(unique_dois) if unique_dois else {}
        logger.info(f"[OpenAlex] Phase 1 done: {len(batch_results)} DOIs resolved")

        # Apply batch results
        batch_hits = 0
        for i, doi in doi_map.items():
            oa_data = batch_results.get(doi.lower())
            if oa_data:
                self._apply_verification(references[i], oa_data)
                batch_hits += 1

        # Phase 2: Individual lookups for misses
        remaining = sum(1 for r in references if r.get("is_verified") is None and not r.get("verification_skipped"))
        logger.info(f"[OpenAlex] Phase 2: {remaining} refs need individual lookup")
        individual_done = 0
        for i, ref in enumerate(references):
            if ref.get("is_verified") is not None or ref.get("verification_skipped"):
                continue
            if self._should_skip(ref.get("url", "")):
                ref["is_verified"] = False
                ref["quality_tier"] = "other"
                ref["verification_skipped"] = True
                continue

            oa_data = self.verify_single(ref.get("url", ""), ref.get("title", ""))
            if oa_data:
                self._apply_verification(ref, oa_data)
            else:
                ref["is_verified"] = False
                ref["quality_tier"] = "unverified"

            individual_done += 1
            if individual_done % 10 == 0:
                logger.info(f"[OpenAlex] Phase 2 progress: {individual_done}/{remaining}")

        logger.info(f"[OpenAlex] Phase 2 done: {individual_done} individual lookups completed")

        # Phase 3: Sort by quality (keep original IDs)
        tier_order = {
            "retracted": 99,
            "landmark": 1,
            "strong": 2,
            "peer-reviewed": 3,
            "preprint": 4,
            "guideline": 5,
            "other": 6,
            "paratext": 7,
            "unverified": 8,
        }
        references.sort(key=lambda r: tier_order.get(r.get("quality_tier", "unverified"), 10))

        verified = sum(1 for r in references if r.get("is_verified"))
        logger.info(f"[OpenAlex] Verification complete: {verified}/{total} verified")

        return references

    def _is_generic_skip(self, url: str) -> bool:
        """Check if the skipped domain is a generic health site (not a guideline)."""
        guideline_domains = {"cdc.gov", "who.int", "fda.gov", "nhs.uk", "nih.gov"}
        url_lower = url.lower()
        for domain in guideline_domains:
            if domain in url_lower:
                return False
        return True

    def _apply_verification(self, ref: dict, oa_data: dict):
        """Apply OpenAlex verification data to a reference."""
        ref["is_verified"] = True
        ref["openalex_id"] = oa_data.get("openalex_id")
        ref["doi"] = oa_data.get("doi")
        ref["citation_count"] = oa_data.get("cited_by_count", 0)
        ref["is_retracted"] = oa_data.get("is_retracted", False)
        ref["is_paratext"] = oa_data.get("is_paratext", False)
        ref["year"] = oa_data.get("year")
        ref["journal"] = oa_data.get("journal")
        ref["is_oa"] = oa_data.get("is_oa", False)
        ref["oa_url"] = oa_data.get("oa_url")
        ref["authors"] = oa_data.get("authors", [])
        ref["work_type"] = oa_data.get("type")
        ref["quality_tier"] = self._compute_tier(oa_data)

    def _compute_tier(self, oa_data: dict) -> str:
        """Compute quality tier from OpenAlex data.

        Tiers: landmark / strong / peer-reviewed / preprint /
               guideline / unverified / retracted / paratext / other
        """
        if oa_data.get("is_retracted"):
            return "retracted"
        if oa_data.get("is_paratext"):
            return "paratext"

        work_type = (oa_data.get("type") or "").lower()
        cited = oa_data.get("cited_by_count", 0)

        # Preprints
        if work_type in ("preprint", "posted-content"):
            return "preprint"

        # Landmark papers: 100+ citations
        if cited >= 100:
            return "landmark"

        # Strong papers: 20+ citations
        if cited >= 20:
            return "strong"

        # Has a journal = peer-reviewed
        if oa_data.get("journal"):
            return "peer-reviewed"

        return "other"
