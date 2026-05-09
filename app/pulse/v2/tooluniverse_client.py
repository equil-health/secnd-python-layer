"""Lazy, singleton wrapper around ToolUniverse.

We import tooluniverse only when v2 is actually invoked, so v1 deployments
don't need the package installed. Failures degrade to a no-op client that
returns []  — the caller treats this as "this source had no results".
"""

from __future__ import annotations

import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_tu = None
_import_failed = False


def get_tu():
    """Return a ToolUniverse instance, or None if the package can't be loaded."""
    global _tu, _import_failed
    if _tu is not None:
        return _tu
    if _import_failed:
        return None

    with _lock:
        if _tu is not None:
            return _tu
        if _import_failed:
            return None
        try:
            from tooluniverse import ToolUniverse  # type: ignore

            _tu = ToolUniverse()
            # Some versions require explicit load; guard with hasattr to stay version-agnostic.
            if hasattr(_tu, "load_tools"):
                try:
                    _tu.load_tools()
                except Exception as e:  # pragma: no cover
                    logger.warning(f"ToolUniverse.load_tools() failed (non-fatal): {e}")
            logger.info("ToolUniverse client initialised")
            return _tu
        except Exception as e:
            _import_failed = True
            logger.warning(
                f"ToolUniverse not available — v2 adapters will return empty: {e}"
            )
            return None


def run_tool(name: str, arguments: dict[str, Any]) -> Any:
    """Call tu.run({'name': ..., 'arguments': ...}) with defensive error handling.

    Returns None on any failure (missing package, network error, schema error).
    The caller decides how to react (fall back to another adapter, log, etc.).
    """
    tu = get_tu()
    if tu is None:
        return None
    try:
        return tu.run({"name": name, "arguments": arguments})
    except Exception as e:
        logger.warning(f"ToolUniverse tool '{name}' failed: {e}")
        return None
