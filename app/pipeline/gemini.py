"""Gemini Flash API client — Google AI Studio REST."""

import time
import requests

from ..config import settings


def call_gemini(prompt: str, max_tokens: int = 2048, temperature: float = 0.3) -> str:
    """Call Gemini 2.0 Flash via Google AI Studio REST API.

    3 retries with exponential backoff.
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

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            pass
        if attempt < 2:
            time.sleep(2 ** attempt)

    raise RuntimeError("Gemini Flash failed after 3 attempts")
