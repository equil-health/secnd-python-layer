"""Pulse search router — picks v1 (legacy) or v2 (ToolUniverse) at runtime.

This is the ONLY module tasks.py talks to. v1 stays untouched. v2 is opt-in
via settings.PULSE_VERSION.

Behaviour:
    PULSE_VERSION="v1"  → call legacy scan_for_articles. Default.
    PULSE_VERSION="v2"  → call v2.search; on empty/failure, optionally
                          fall back to v1 if PULSE_V2_FALLBACK_TO_V1=True.
    PULSE_VERSION="shadow" → run v1 for the user, run v2 in a side thread
                             for telemetry only (results discarded).

Return shape is identical in every mode (the v1 article dict).
"""

from __future__ import annotations

import logging
import threading

from ..config import settings
from .scanner import scan_for_articles as _v1_scan

logger = logging.getLogger(__name__)


def _shadow_run_v2(**kwargs) -> None:
    """Fire-and-forget v2 invocation for shadow mode. Logs counts, discards results."""
    try:
        from .v2 import search as v2_search

        results = v2_search(**kwargs)
        logger.info(
            f"Pulse SHADOW v2: would have returned {len(results)} articles "
            f"(specialty={kwargs.get('specialty')})"
        )
    except Exception as e:
        logger.warning(f"Pulse SHADOW v2 failed: {e}")


def search(
    *,
    specialty: str,
    topics: list[str],
    mesh_terms: list[str] | None = None,
    enabled_journals: list[str] | None = None,
    days_back: int | None = None,
    max_articles: int | None = None,
    skip_cache: bool = False,
) -> list[dict]:
    version = (settings.PULSE_VERSION or "v1").strip().lower()
    kwargs = dict(
        specialty=specialty,
        topics=topics,
        mesh_terms=mesh_terms,
        enabled_journals=enabled_journals,
        days_back=days_back,
        max_articles=max_articles,
        skip_cache=skip_cache,
    )

    if version == "v2":
        try:
            from .v2 import search as v2_search

            results = v2_search(**kwargs)
            if results:
                return results
            logger.warning("Pulse v2 returned no results")
        except Exception as e:
            logger.error(f"Pulse v2 raised: {e}")

        if settings.PULSE_V2_FALLBACK_TO_V1:
            logger.info("Pulse v2: falling back to v1")
            return _v1_scan(**kwargs)
        return []

    if version == "shadow":
        # Run v2 in a daemon thread so it can't slow down the digest task.
        t = threading.Thread(target=_shadow_run_v2, kwargs=kwargs, daemon=True)
        t.start()
        return _v1_scan(**kwargs)

    # Default: v1
    return _v1_scan(**kwargs)
