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
    "Cardiology":       "cardiology cardiac heart failure arrhythmia coronary STEMI guidelines",
    "Neurology":        "neurology stroke epilepsy dementia Parkinson MS ALS guidelines",
    "Hepatology":       "hepatology liver cirrhosis hepatitis NASH MASLD DILI guidelines",
    "Oncology":         "oncology cancer chemotherapy immunotherapy targeted therapy trial",
    "Pulmonology":      "pulmonology COPD asthma ILD tuberculosis pneumonia NTEP",
    "Endocrinology":    "endocrinology diabetes thyroid adrenal pituitary insulin guidelines",
    "Gastroenterology": "gastroenterology IBD IBS colonoscopy H pylori guidelines",
    "General Medicine": "internal medicine India clinical guidelines NMC CDSCO drug approval",
    "Nephrology":       "nephrology CKD dialysis AKI renal transplant KDIGO guidelines",
    "Rheumatology":     "rheumatology autoimmune lupus SLE RA vasculitis biologics",
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
    query = SPECIALTY_SEARCH_TERMS.get(specialty, specialty)

    # Check cache
    cache_key = f"breaking:raw:{hashlib.md5(f'{specialty}:{query}'.encode()).hexdigest()}"
    if not skip_cache:
        try:
            cached = _redis.get(cache_key)
            if cached:
                return json.loads(cached)
        except redis.ConnectionError:
            pass

    headlines = []
    start = time.time()
    status = "success"
    error_msg = None
    try:
        resp = requests.post(
            "https://google.serper.dev/news",
            json={
                "q": query,
                "num": max_results,
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
                headlines.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "source": item.get("source", ""),
                    "snippet": item.get("snippet", ""),
                    "published_at": item.get("date", ""),
                    "specialty": specialty,
                })
        else:
            status = "error"
            error_msg = f"HTTP {resp.status_code} {resp.text[:200]}"
            logger.error(f"Serper news error for {specialty}: {error_msg}")
    except Exception as e:
        status = "error"
        error_msg = str(e)[:200]
        logger.error(f"Serper news exception for {specialty}: {e}")

    tracker.log(
        "breaking", "serper_news", "fetch_headlines",
        request_summary=f"{specialty}: {query}"[:500],
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
