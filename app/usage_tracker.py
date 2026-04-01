"""Usage tracking middleware — centralized API call logging.

Every external API call (Gemini, MedGemma, Serper, OpenAlex, PubMed,
Embeddings, Crossref, SDSS GPU) is logged to the ``usage_log`` table with:
  - who (user_id, case_id)
  - what (module, service, operation)
  - request summary (truncated prompt/query)
  - response metrics (duration, tokens, chars, result count)
  - cost estimation
  - error tracking

Writes are buffered in an in-memory queue and batch-flushed every 5 seconds
(or 50 events), so ``tracker.log()`` is non-blocking (~nanoseconds).

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

import atexit
import json
import logging
import queue
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

# Buffered writer constants
_FLUSH_INTERVAL = 5.0   # seconds between flushes
_FLUSH_BATCH_SIZE = 50   # max events per flush

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
    # SDSS GPU pod
    "sdss_gpu": {"per_call": 0.15},  # GPU pod processing cost estimate
    "sdss_serper": {"per_call": 0.004},  # Serper calls made by GPU pod
}

_INSERT_SQL = text("""
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
""")


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


# ── Buffered Writer ─────────────────────────────────────────────

class _BufferedWriter:
    """Queue-backed background writer for usage_log.

    ``put()`` is non-blocking (~nanoseconds).  A daemon thread wakes
    every ``_FLUSH_INTERVAL`` seconds and batch-INSERTs up to
    ``_FLUSH_BATCH_SIZE`` rows in a single transaction.
    """

    def __init__(self):
        self._queue: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._start_lock = threading.Lock()

    def _ensure_started(self):
        if self._started:
            return
        with self._start_lock:
            if self._started:
                return
            self._thread = threading.Thread(target=self._flush_loop, daemon=True, name="usage-writer")
            self._thread.start()
            self._started = True
            atexit.register(self.shutdown)

    def put(self, params: dict):
        self._ensure_started()
        self._queue.put(params)
        if self._queue.qsize() > 500:
            logger.warning("Usage tracking queue depth > 500 — possible backpressure")

    def _flush_loop(self):
        while not self._stop.is_set():
            self._stop.wait(timeout=_FLUSH_INTERVAL)
            self._drain()

    def _drain(self):
        batch = []
        while len(batch) < _FLUSH_BATCH_SIZE:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self._write_batch(batch)

    def _write_batch(self, batch: list[dict]):
        try:
            engine = _get_engine()
            with engine.begin() as conn:
                for params in batch:
                    conn.execute(_INSERT_SQL, params)
        except Exception as e:
            logger.warning("Usage tracking batch write failed (%d rows): %s", len(batch), e)

    def shutdown(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        # Drain remaining events synchronously
        self._drain()


_writer = _BufferedWriter()


# ── Usage Tracker ───────────────────────────────────────────────

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
        """Enqueue a usage log entry for background batch-write."""
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
            _writer.put({
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
                "metadata": json.dumps(metadata) if metadata else None,
            })
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
