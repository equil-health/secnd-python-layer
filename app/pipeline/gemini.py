"""Gemini Flash API client — Google AI Studio REST."""

import time
import requests

from ..config import settings
from ..usage_tracker import tracker


def call_gemini(prompt: str, max_tokens: int = 2048, temperature: float = 0.3,
                _module: str = "pipeline", _operation: str = "call_gemini",
                json_mode: bool = False) -> str:
    """Call Gemini 2.5 Flash via Google AI Studio REST API.

    5 retries with aggressive backoff for rate limits.
    """
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.5-flash:generateContent?key={settings.GEMINI_API_KEY}"
    )
    headers = {"Content-Type": "application/json"}
    gen_config = {
        "maxOutputTokens": max_tokens,
        "temperature": temperature,
    }
    if json_mode:
        gen_config["responseMimeType"] = "application/json"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": gen_config,
    }

    start = time.time()
    last_error = None
    for attempt in range(5):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                text_out = data["candidates"][0]["content"]["parts"][0]["text"]
                usage = data.get("usageMetadata", {})
                tracker.log(
                    _module, "gemini", _operation,
                    request_summary=prompt[:500],
                    model="gemini-2.5-flash",
                    status="success",
                    duration_ms=int((time.time() - start) * 1000),
                    input_tokens=usage.get("promptTokenCount"),
                    output_tokens=usage.get("candidatesTokenCount"),
                    input_chars=len(prompt),
                    output_chars=len(text_out),
                )
                return text_out
            if resp.status_code == 429:
                wait = min(15 * (2 ** attempt), 120)
                time.sleep(wait)
                continue
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            last_error = str(e)
        if attempt < 4:
            time.sleep(2 ** attempt)

    tracker.log(
        _module, "gemini", _operation,
        request_summary=prompt[:500],
        model="gemini-2.5-flash",
        status="error",
        error_message=last_error,
        duration_ms=int((time.time() - start) * 1000),
        input_chars=len(prompt),
    )
    raise RuntimeError(f"Gemini Flash failed after 5 attempts: {last_error}")
