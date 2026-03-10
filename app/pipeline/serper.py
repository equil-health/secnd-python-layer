"""Serper.dev search client with Redis caching — ported from v5 lines 355-379."""

import hashlib
import json
import time
import requests
import redis

from ..config import settings
from ..usage_tracker import tracker

_redis = redis.Redis.from_url(settings.REDIS_URL)
CACHE_TTL = 86400  # 24 hours


def check_serper_health() -> bool:
    """Quick health check — returns True if Serper API responds within 5s."""
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": "test", "num": 1},
            headers={
                "X-API-KEY": settings.SERPER_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        return resp.status_code == 200
    except Exception:
        return False


def search_serper(query: str, num_results: int = 5, query_suffix: str = "",
                  _module: str = "pipeline") -> list[dict]:
    """Search via Serper.dev API with Redis caching (24h TTL)."""
    if query_suffix:
        query = f"{query} {query_suffix}"
    # Check cache
    cache_key = f"search_cache:{hashlib.md5(query.encode()).hexdigest()}"
    try:
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
    except redis.ConnectionError:
        pass

    start = time.time()
    results = []
    status = "success"
    error_msg = None
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": num_results},
            headers={
                "X-API-KEY": settings.SERPER_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("organic", []):
                results.append({
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                })
        else:
            status = "error"
            error_msg = f"HTTP {resp.status_code}"
    except Exception as e:
        status = "error"
        error_msg = str(e)[:200]

    tracker.log(
        _module, "serper", "search_serper",
        request_summary=query[:500],
        status=status,
        error_message=error_msg,
        duration_ms=int((time.time() - start) * 1000),
        num_results=len(results),
    )

    # Cache results
    if results:
        try:
            _redis.setex(cache_key, CACHE_TTL, json.dumps(results))
        except redis.ConnectionError:
            pass

    return results
