"""Pulse v2 — pluggable multi-source literature search via ToolUniverse.

v1 (legacy) lives in ../scanner.py + ../abstract_fetcher.py and is unchanged.
v2 is opt-in via settings.PULSE_VERSION ("v1" | "v2") and settings.PULSE_V2_*.

Public entry point: search(...) — same signature/return shape as v1's scan_for_articles.
"""

from .search import search  # noqa: F401
