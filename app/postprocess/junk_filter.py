"""Filter non-URL references — ported from v5 lines 860-870."""


def filter_junk_refs(references: list[dict]) -> list[dict]:
    """Remove references that aren't valid HTTP URLs.

    Also filters STORM internal file references that leaked into the bibliography.
    """
    junk_patterns = [
        "url_to_info", "url_to_unified", "conversation_log",
        "storm_gen_", "direct_gen_", "raw_search",
    ]

    clean = []
    for ref in references:
        url = ref.get("url", "")
        if not url.startswith("http"):
            continue
        if any(junk in url.lower() for junk in junk_patterns):
            continue
        clean.append(ref)

    return clean
