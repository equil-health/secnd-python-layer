"""Usage tracking middleware — centralized API call logging.

Every external API call (Gemini, MedGemma, Serper, OpenAlex, PubMed,
Embeddings, Crossref) is logged to the ``usage_log`` table with:
  - who (user_id, case_id)
  - what (module, service, operation)
  - request summary (truncated prompt/query)
  - response metrics (duration, tokens, chars, result count)
  - cost estimation
  - error tracking

Usage from any module:
    from app.usage_tracker import tracker

    # Context manager — auto-logs duration, status, errors
    with tracker.track("pipeline", "gemini", "call_gemini",
                       case_id=case_id, request_summary=prompt[:200]):
        result = call_gemini(prompt)
        tracker.set_output(output_chars=len(result))

    # Or use the decorator
    @tracker.wrap("breaking", "serper", "fetch_headlines")
    def fetch_headlines(specialty):
        ...

    # Or log manually
    tracker.log("pulse", "pubmed", "search_pubmed",
                request_summary=query, num_results=len(pmids),
                duration_ms=elapsed_ms)
"""

import logging
import threading
import time
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, text

from .config import settings

logger = logging.getLogger(__name__)

# Sync engine for usage logging (works in both async FastAPI and sync Celery)
_engine = None
_engine_lock = threading.Lock()

# Thread-local storage for context (case_id, user_id)
_context = threading.local()

# Cost per unit (USD) — approximate, updated as needed
COST_TABLE = {
    "gemini": {"input_per_1k_chars": 0.0000125, "output_per_1k_chars": 0.00005},
    "medgemma": {"per_call": 0.01},  # Vertex AI dedicated endpoint
    "embedding": {"per_call": 0.000025},  # per text embedded
    "serper": {"per_call": 0.004},  # ~$50/10k searches
    "serper_news": {"per_call": 0.004},
    "openalex": {"per_call": 0.0},  # free API
    "pubmed": {"per_call": 0.0},  # free API
    "crossref": {"per_call": 0.0},  # free API
}


def _get_engine():
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
                _engine = create_engine(sync_url, pool_size=2, max_overflow=3)
    return _engine


def _estimate_cost(service: str, input_chars: int = 0, output_chars: int = 0,
                   num_items: int = 1) -> float:
    """Estimate cost in USD for an API call."""
    rates = COST_TABLE.get(service, {})
    if "per_call" in rates:
        return rates["per_call"] * num_items
    cost = 0.0
    if "input_per_1k_chars" in rates and input_chars:
        cost += (input_chars / 1000) * rates["input_per_1k_chars"]
    if "output_per_1k_chars" in rates and output_chars:
        cost += (output_chars / 1000) * rates["output_per_1k_chars"]
    return cost


class UsageTracker:
    """Centralized usage tracking for all external API calls."""

    def set_context(self, user_id: str = None, case_id: str = None):
        """Set thread-local context for subsequent log calls."""
        if user_id is not None:
            _context.user_id = user_id
        if case_id is not None:
            _context.case_id = case_id

    def clear_context(self):
        """Clear thread-local context."""
        _context.user_id = None
        _context.case_id = None

    def _get_context_user_id(self):
        return getattr(_context, "user_id", None)

    def _get_context_case_id(self):
        return getattr(_context, "case_id", None)

    def log(
        self,
        module: str,
        service: str,
        operation: str,
        *,
        user_id: str = None,
        case_id: str = None,
        request_summary: str = None,
        model: str = None,
        status: str = "success",
        error_message: str = None,
        duration_ms: int = None,
        input_tokens: int = None,
        output_tokens: int = None,
        input_chars: int = None,
        output_chars: int = None,
        num_results: int = None,
        estimated_cost_usd: float = None,
        metadata: dict = None,
    ):
        """Write a single usage log entry to the database."""
        user_id = user_id or self._get_context_user_id()
        case_id = case_id or self._get_context_case_id()

        if estimated_cost_usd is None:
            estimated_cost_usd = _estimate_cost(
                service,
                input_chars=input_chars or 0,
                output_chars=output_chars or 0,
                num_items=num_results or 1,
            )

        # Truncate request summary
        if request_summary and len(request_summary) > 500:
            request_summary = request_summary[:497] + "..."

        try:
            engine = _get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO usage_log (
                            user_id, case_id, module, service, operation,
                            request_summary, model, status, error_message,
                            duration_ms, input_tokens, output_tokens,
                            input_chars, output_chars, num_results,
                            estimated_cost_usd, metadata
                        ) VALUES (
                            CAST(:user_id AS uuid), CAST(:case_id AS uuid),
                            :module, :service, :operation,
                            :request_summary, :model, :status, :error_message,
                            :duration_ms, :input_tokens, :output_tokens,
                            :input_chars, :output_chars, :num_results,
                            :estimated_cost_usd, CAST(:metadata AS jsonb)
                        )
                    """),
                    {
                        "user_id": user_id,
                        "case_id": case_id,
                        "module": module,
                        "service": service,
                        "operation": operation,
                        "request_summary": request_summary,
                        "model": model,
                        "status": status,
                        "error_message": error_message,
                        "duration_ms": duration_ms,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "input_chars": input_chars,
                        "output_chars": output_chars,
                        "num_results": num_results,
                        "estimated_cost_usd": estimated_cost_usd,
                        "metadata": __import__("json").dumps(metadata) if metadata else None,
                    },
                )
        except Exception as e:
            # Never let usage tracking break the actual pipeline
            logger.warning("Usage tracking failed: %s", e)

    @contextmanager
    def track(
        self,
        module: str,
        service: str,
        operation: str,
        *,
        user_id: str = None,
        case_id: str = None,
        request_summary: str = None,
        model: str = None,
        metadata: dict = None,
    ):
        """Context manager that auto-logs duration, status, and errors.

        Usage::

            with tracker.track("pipeline", "gemini", "hallucination_check",
                               case_id=cid, request_summary=prompt[:200]) as t:
                result = call_gemini(prompt)
                t["output_chars"] = len(result)
                t["num_results"] = len(issues)
        """
        t = {
            "input_chars": len(request_summary) if request_summary else None,
            "output_chars": None,
            "input_tokens": None,
            "output_tokens": None,
            "num_results": None,
            "extra_metadata": None,
        }
        start = time.time()
        status = "success"
        error_msg = None

        try:
            yield t
        except Exception as e:
            status = "error"
            error_msg = str(e)[:500]
            raise
        finally:
            duration_ms = int((time.time() - start) * 1000)
            merged_meta = {**(metadata or {}), **(t.get("extra_metadata") or {})}
            self.log(
                module, service, operation,
                user_id=user_id,
                case_id=case_id,
                request_summary=request_summary,
                model=model,
                status=status,
                error_message=error_msg,
                duration_ms=duration_ms,
                input_tokens=t.get("input_tokens"),
                output_tokens=t.get("output_tokens"),
                input_chars=t.get("input_chars"),
                output_chars=t.get("output_chars"),
                num_results=t.get("num_results"),
                metadata=merged_meta if merged_meta else None,
            )

    def wrap(self, module: str, service: str, operation: str, model: str = None):
        """Decorator that auto-tracks a function call.

        The decorated function's first positional arg is used as request_summary
        (truncated to 200 chars).
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                summary = str(args[0])[:200] if args else None
                with self.track(module, service, operation,
                                request_summary=summary, model=model) as t:
                    result = func(*args, **kwargs)
                    if isinstance(result, str):
                        t["output_chars"] = len(result)
                    elif isinstance(result, list):
                        t["num_results"] = len(result)
                    return result
            wrapper.__name__ = func.__name__
            wrapper.__doc__ = func.__doc__
            return wrapper
        return decorator


# Singleton
tracker = UsageTracker()
