"""Serper.dev search client with Redis caching — ported from v5 lines 355-379."""

import hashlib
import json
import requests
import redis

from ..config import settings

_redis = redis.Redis.from_url(settings.REDIS_URL)
CACHE_TTL = 86400  # 24 hours


def search_serper(query: str, num_results: int = 5) -> list[dict]:
    """Search via Serper.dev API with Redis caching (24h TTL)."""
    # Check cache
    cache_key = f"search_cache:{hashlib.md5(query.encode()).hexdigest()}"
    try:
        cached = _redis.get(cache_key)
        if cached:
            return json.loads(cached)
    except redis.ConnectionError:
        pass

    results = []
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
    except Exception:
        pass

    # Cache results
    try:
        _redis.setex(cache_key, CACHE_TTL, json.dumps(results))
    except redis.ConnectionError:
        pass

    return results
