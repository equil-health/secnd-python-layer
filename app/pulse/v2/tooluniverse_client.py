"""Lazy, singleton wrapper around ToolUniverse.

We import tooluniverse only when v2 is actually invoked, so v1 deployments
don't need the package installed. Failures degrade to a no-op client that
returns []  — the caller treats this as "this source had no results".
"""

from __future__ import annotations

import logging
import threading
import time
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
        logger.warning("PULSE_DEBUG get_tu: previous import failed, returning None")
        return None

    with _lock:
        if _tu is not None:
            return _tu
        if _import_failed:
            return None
        try:
            logger.warning("PULSE_DEBUG get_tu: importing tooluniverse…")
            from tooluniverse import ToolUniverse  # type: ignore

            logger.warning("PULSE_DEBUG get_tu: instantiating ToolUniverse()")
            _tu = ToolUniverse()
            # Some versions require explicit load; guard with hasattr to stay version-agnostic.
            if hasattr(_tu, "load_tools"):
                try:
                    logger.warning("PULSE_DEBUG get_tu: calling load_tools()")
                    _tu.load_tools()
                    logger.warning("PULSE_DEBUG get_tu: load_tools() returned OK")
                except Exception as e:  # pragma: no cover
                    logger.warning(f"PULSE_DEBUG get_tu: load_tools() failed (non-fatal): {e!r}")
            try:
                n = len(getattr(_tu, "all_tools", []) or [])
            except Exception:
                n = -1
            logger.warning(f"PULSE_DEBUG get_tu: ToolUniverse client initialised, all_tools={n}")
            return _tu
        except Exception as e:
            _import_failed = True
            logger.warning(
                f"PULSE_DEBUG get_tu: ToolUniverse not available — v2 adapters will return empty: {e!r}"
            )
            return None


def run_tool(name: str, arguments: dict[str, Any]) -> Any:
    """Call tu.run({'name': ..., 'arguments': ...}) with defensive error handling.

    Returns None on any failure (missing package, network error, schema error).
    The caller decides how to react (fall back to another adapter, log, etc.).
    """
    tu = get_tu()
    if tu is None:
        logger.warning(f"PULSE_DEBUG run_tool[{name}]: tu is None, returning None")
        return None
    try:
        arg_keys = list(arguments.keys()) if isinstance(arguments, dict) else type(arguments).__name__
        logger.warning(f"PULSE_DEBUG run_tool[{name}]: invoking with arg_keys={arg_keys}")
        t0 = time.monotonic()
        resp = tu.run({"name": name, "arguments": arguments})
        dt = time.monotonic() - t0
        if resp is None:
            shape = "None"
        elif isinstance(resp, list):
            shape = f"list(len={len(resp)})"
        elif isinstance(resp, dict):
            shape = f"dict(keys={list(resp.keys())[:8]})"
        else:
            shape = type(resp).__name__
        logger.warning(f"PULSE_DEBUG run_tool[{name}]: returned in {dt:.2f}s, shape={shape}")
        # If the response looks like an error envelope (status != success,
        # or contains an `error` key, or `data` is empty), dump it so we can
        # see why OpenAlex / SemanticScholar etc. return 0 records.
        if isinstance(resp, dict):
            status = resp.get("status")
            err = resp.get("error")
            data = resp.get("data")
            data_empty = data is None or (hasattr(data, "__len__") and len(data) == 0)
            if err or (status and str(status).lower() not in ("success", "ok", "200")) or data_empty:
                preview = {
                    k: (str(v)[:300] if not isinstance(v, (dict, list)) else v)
                    for k, v in list(resp.items())[:6]
                }
                logger.warning(
                    f"PULSE_DEBUG run_tool[{name}]: SUSPICIOUS response "
                    f"status={status!r} error={err!r} data_empty={data_empty} "
                    f"preview={preview}"
                )
        return resp
    except Exception as e:
        logger.warning(f"PULSE_DEBUG run_tool[{name}]: raised {type(e).__name__}: {e!r}")
        return None


def get_tool_schema(name: str) -> dict | None:
    """Return the registered schema dict for a tool, or None if unknown.

    Used by adapters to discover the actual parameter names of a tool at
    runtime (instead of guessing 'query' vs 'search_term' vs 'keywords')."""
    tu = get_tu()
    if tu is None:
        logger.warning(f"PULSE_DEBUG get_tool_schema[{name}]: tu is None")
        return None
    try:
        all_tools = getattr(tu, "all_tools", []) or []
        for t in all_tools:
            if isinstance(t, dict) and t.get("name") == name:
                logger.warning(f"PULSE_DEBUG get_tool_schema[{name}]: FOUND")
                return t
        logger.warning(
            f"PULSE_DEBUG get_tool_schema[{name}]: NOT FOUND in registry of {len(all_tools)} tools"
        )
    except Exception as e:
        logger.warning(f"PULSE_DEBUG get_tool_schema[{name}]: raised {type(e).__name__}: {e!r}")
    return None
