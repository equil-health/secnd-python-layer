"""Unified bibliography and citation remapping — ported from v5 lines 849-986."""

import re


def build_unified_bibliography(
    serper_refs: list,
    storm_url_info: dict,
) -> tuple[list, dict, dict]:
    """Merge Serper search results and STORM's url_to_info into one bibliography.

    Returns:
        unique_refs: deduplicated reference list [{id, title, url, snippet}]
        storm_remap: {storm_internal_num: final_bibliography_num}
        old_to_new: {old_ref_id: new_sequential_id}
    """
    all_refs = list(serper_refs)
    storm_remap = {}

    # STORM junk URL patterns
    junk_patterns = [
        "url_to_info", "url_to_unified", "conversation_log",
        "storm_gen_", "direct_gen_", "raw_search",
    ]

    # Add STORM refs, skipping junk
    storm_num = 1
    for url, info in storm_url_info.items():
        # Junk filter: skip non-URLs and STORM internal files
        if not url.startswith("http"):
            storm_num += 1
            continue
        if any(junk in url.lower() for junk in junk_patterns):
            storm_num += 1
            continue

        title = ""
        snippet = ""
        if isinstance(info, dict):
            title = info.get("title", info.get("name", ""))
            snippets = info.get("snippets", info.get("snippet", ""))
            if isinstance(snippets, list) and snippets:
                snippet = snippets[0][:200] if isinstance(snippets[0], str) else ""
            elif isinstance(snippets, dict):
                snippet = str(list(snippets.values())[0])[:200] if snippets else ""
            elif isinstance(snippets, str):
                snippet = snippets[:200]

        ref_id = len(all_refs) + 1
        all_refs.append({
            "id": ref_id,
            "title": title or url.split("/")[-1].replace("-", " "),
            "url": url,
            "snippet": snippet,
        })
        storm_remap[storm_num] = ref_id
        storm_num += 1

    # Deduplicate by URL
    seen_urls: set[str] = set()
    unique: list[dict] = []
    old_to_new: dict[int, int] = {}
    for ref in all_refs:
        if ref["url"] not in seen_urls and ref["url"].startswith("http"):
            seen_urls.add(ref["url"])
            old_id = ref["id"]
            ref["id"] = len(unique) + 1
            old_to_new[old_id] = ref["id"]
            unique.append(ref)

    # Remap storm numbers through dedup
    final_storm_remap = {}
    for s_num, old_id in storm_remap.items():
        if old_id in old_to_new:
            final_storm_remap[s_num] = old_to_new[old_id]

    return unique, final_storm_remap, old_to_new


def remap_citations_in_text(text: str, remap: dict) -> str:
    """Replace [n] citation numbers in text using remap dict.

    Handles consecutive citations like [1][2][3].
    """
    return re.sub(
        r"(\[\d+\](?:\[\d+\])*)",
        lambda m: "".join(
            f"[{remap.get(int(n), int(n))}]"
            for n in re.findall(r"\d+", m.group(0))
        ),
        text,
    )
