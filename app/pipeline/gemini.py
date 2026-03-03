"""Gemini Flash API client — Google AI Studio REST."""

import time
import requests

from ..config import settings


def call_gemini(prompt: str, max_tokens: int = 2048, temperature: float = 0.3) -> str:
    """Call Gemini 2.0 Flash via Google AI Studio REST API.

    5 retries with aggressive backoff for rate limits.
    """
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={settings.GEMINI_API_KEY}"
    )
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": temperature,
        },
    }

    last_error = None
    for attempt in range(5):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]
            if resp.status_code == 429:
                # Free-tier rate limit — back off aggressively
                wait = min(15 * (2 ** attempt), 120)
                time.sleep(wait)
                continue
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            last_error = str(e)
        if attempt < 4:
            time.sleep(2 ** attempt)

    raise RuntimeError(f"Gemini Flash failed after 5 attempts: {last_error}")
