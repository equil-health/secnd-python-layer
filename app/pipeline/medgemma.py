"""MedGemma API client — ported from v5 lines 175-205."""

import time
import requests
from google.oauth2 import service_account
import google.auth.transport.requests as google_requests

from ..config import settings
from ..usage_tracker import tracker

_credentials = None


def _get_credentials():
    """Get or refresh GCP credentials."""
    global _credentials
    if _credentials is None or _credentials.expired:
        _credentials = service_account.Credentials.from_service_account_file(
            settings.GCP_SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        _credentials.refresh(google_requests.Request())
    return _credentials


def call_medgemma(prompt: str, max_tokens: int = 4096, temperature: float = 0.3) -> str:
    """Call MedGemma on Vertex AI dedicated endpoint.

    3 retries with exponential backoff. Parses "Output:" prefix if present.
    """
    creds = _get_credentials()
    headers = {
        "Authorization": f"Bearer {creds.token}",
        "Content-Type": "application/json",
    }
    payload = {
        "instances": [{
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "top_p": 0.95,
        }]
    }

    start = time.time()
    last_error = None
    for attempt in range(3):
        try:
            resp = requests.post(
                settings.MEDGEMMA_PREDICT_URL,
                json=payload,
                headers=headers,
                timeout=180,
            )
            if resp.status_code == 200:
                data = resp.json()
                if "predictions" in data and data["predictions"]:
                    raw = data["predictions"][0]
                    if isinstance(raw, str) and "Output:" in raw:
                        result = raw.split("Output:", 1)[1].strip()
                    elif isinstance(raw, str):
                        result = raw.strip()
                    else:
                        result = str(data)
                else:
                    result = str(data)
                tracker.log(
                    "pipeline", "medgemma", "call_medgemma",
                    request_summary=prompt[:500],
                    model="medgemma-4b",
                    status="success",
                    duration_ms=int((time.time() - start) * 1000),
                    input_chars=len(prompt),
                    output_chars=len(result),
                )
                return result
            last_error = f"HTTP {resp.status_code}"
        except Exception as e:
            last_error = str(e)
        if attempt < 2:
            time.sleep(2 ** attempt)

    tracker.log(
        "pipeline", "medgemma", "call_medgemma",
        request_summary=prompt[:500],
        model="medgemma-4b",
        status="error",
        error_message=last_error,
        duration_ms=int((time.time() - start) * 1000),
        input_chars=len(prompt),
    )
    raise RuntimeError("MedGemma failed after 3 attempts")
