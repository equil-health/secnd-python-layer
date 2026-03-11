"""Breaking Step B1 — Fetch headlines via Serper news search.

Calls Serper news endpoint for each active specialty.
Fetches 20 raw headlines per specialty (200 total for 10 specialties).
time_filter="d" restricts to last 24 hours.
"""

import hashlib
import json
import logging
import time
import requests
import redis

from ..config import settings
from ..usage_tracker import tracker

logger = logging.getLogger(__name__)

_redis = redis.Redis.from_url(settings.REDIS_URL)
CACHE_TTL = 3600  # 1 hour for breaking news (fresher than research)

SPECIALTY_SEARCH_TERMS = {
    "Cardiology":       [
        "cardiology heart failure new study OR trial OR guideline",
        "cardiac arrhythmia OR STEMI OR coronary breakthrough",
    ],
    "Neurology":        [
        "neurology stroke OR dementia OR Alzheimer new study OR treatment",
        "epilepsy OR Parkinson OR multiple sclerosis breakthrough OR trial",
    ],
    "Hepatology":       [
        "liver disease hepatitis OR cirrhosis new study OR treatment",
        "NASH OR MASLD OR fatty liver drug OR trial",
    ],
    "Oncology":         [
        "cancer treatment new study OR breakthrough OR approval",
        "oncology immunotherapy OR chemotherapy OR targeted therapy trial",
    ],
    "Pulmonology":      [
        "lung disease COPD OR asthma new study OR treatment",
        "tuberculosis OR pneumonia OR pulmonary fibrosis breakthrough",
    ],
    "Endocrinology":    [
        "diabetes new drug OR treatment OR study OR guideline",
        "thyroid OR obesity OR insulin breakthrough OR trial",
    ],
    "Gastroenterology": [
        "gastroenterology IBD OR Crohn OR ulcerative colitis new study",
        "GI disease treatment OR drug OR guideline breakthrough",
    ],
    "General Medicine": [
        "medical news India drug approval OR clinical guideline 2026",
        "internal medicine new study OR treatment breakthrough",
    ],
    "Nephrology":       [
        "kidney disease CKD OR dialysis new study OR treatment",
        "nephrology transplant OR AKI breakthrough OR trial",
    ],
    "Rheumatology":     [
        "rheumatology lupus OR rheumatoid arthritis new treatment OR study",
        "autoimmune disease biologics OR trial breakthrough",
    ],
}


def active_specialties() -> list[str]:
    """Return list of active specialties for Breaking pipeline."""
    return list(SPECIALTY_SEARCH_TERMS.keys())


def fetch_breaking_headlines(
    specialty: str,
    max_results: int = 20,
    skip_cache: bool = False,
) -> list[dict]:
    """Fetch raw news headlines for a specialty via Serper news endpoint.

    Args:
        specialty: Medical specialty name (must be in SPECIALTY_SEARCH_TERMS)
        max_results: Number of headlines to fetch (default 20)
        skip_cache: Bypass Redis cache

    Returns:
        List of headline dicts with: title, url, source, snippet, published_at, specialty
    """
    queries = SPECIALTY_SEARCH_TERMS.get(specialty, [specialty])
    if isinstance(queries, str):
        queries = [queries]

    # Check cache
    cache_key = f"breaking:raw:{hashlib.md5(specialty.encode()).hexdigest()}"
    if not skip_cache:
        try:
            cached = _redis.get(cache_key)
            if cached:
                return json.loads(cached)
        except redis.ConnectionError:
            pass

    headlines = []
    seen_urls = set()
    start = time.time()
    status = "success"
    error_msg = None

    per_query_limit = max(max_results // len(queries), 10)

    for query in queries:
        try:
            resp = requests.post(
                "https://google.serper.dev/news",
                json={
                    "q": query,
                    "num": per_query_limit,
                    "tbs": "qdr:d",  # last 24 hours
                },
                headers={
                    "X-API-KEY": settings.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("news", []):
                    url = item.get("link", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    headlines.append({
                        "title": item.get("title", ""),
                        "url": url,
                        "source": item.get("source", ""),
                        "snippet": item.get("snippet", ""),
                        "published_at": item.get("date", ""),
                        "specialty": specialty,
                    })
            else:
                status = "error"
                error_msg = f"HTTP {resp.status_code} {resp.text[:200]}"
                logger.error(f"Serper news error for {specialty} query '{query}': {error_msg}")
        except Exception as e:
            status = "error"
            error_msg = str(e)[:200]
            logger.error(f"Serper news exception for {specialty}: {e}")

    tracker.log(
        "breaking", "serper_news", "fetch_headlines",
        request_summary=f"{specialty}: {len(queries)} queries"[:500],
        status=status,
        error_message=error_msg,
        duration_ms=int((time.time() - start) * 1000),
        num_results=len(headlines),
        metadata={"specialty": specialty},
    )

    # Cache
    try:
        _redis.setex(cache_key, CACHE_TTL, json.dumps(headlines))
    except redis.ConnectionError:
        pass

    logger.info(f"[Breaking B1] {specialty}: fetched {len(headlines)} raw headlines")
    return headlines
