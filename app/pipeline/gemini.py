"""Gemini Flash API client — ported from v5 lines 323-352."""

import time
import requests

from ..config import settings
from .medgemma import _get_credentials


def call_gemini(prompt: str, max_tokens: int = 2048, temperature: float = 0.3) -> str:
    """Call Gemini 2.0 Flash via Vertex AI REST API.

    3 retries with exponential backoff.
    """
    creds = _get_credentials()
    url = (
        f"https://{settings.GCP_LOCATION}-aiplatform.googleapis.com/v1/"
        f"projects/{settings.GCP_PROJECT_ID}/locations/{settings.GCP_LOCATION}/"
        f"publishers/google/models/gemini-2.0-flash:generateContent"
    )
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }
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
