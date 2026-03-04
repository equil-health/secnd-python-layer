"""Co-STORM execution wrapper with transparent fallback to STORM.

Attempts to use Co-STORM (collaborative STORM) from knowledge_storm.
Falls back to standard STORM if Co-STORM is unavailable.
Same return dict shape as storm_runner.run_storm().
"""

import os
import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from ..config import settings

# Try to import Co-STORM; flag availability
_COSTORM_AVAILABLE = False
try:
    from knowledge_storm import CoSTORMRunner
    _COSTORM_AVAILABLE = True
except ImportError:
    pass


def _extract_costorm_refs(runner) -> dict:
    """Extract references from a Co-STORM runner's knowledge base.

    Co-STORM stores references in its internal knowledge base rather than
    writing url_to_info.json to disk.  This function attempts multiple
    extraction strategies to build a {url: info_dict} mapping.

    Returns an empty dict if extraction fails.
    """
    if runner is None:
        return {}

    url_info = {}

    try:
        # Strategy 1: runner.to_dict() → knowledge_base → info_uuid_to_info_dict
        if hasattr(runner, "to_dict"):
            data = runner.to_dict()
            kb = data.get("knowledge_base", {})
            info_dict = kb.get("info_uuid_to_info_dict", {})
            for _uuid, info in info_dict.items():
                if isinstance(info, dict):
                    url = info.get("url", info.get("source_url", ""))
                    if url and url.startswith("http"):
                        url_info[url] = {
                            "title": info.get("title", info.get("name", "")),
                            "snippets": info.get("snippets", info.get("snippet", [])),
                        }
            if url_info:
                return url_info
    except Exception:
        pass

    try:
        # Strategy 2: direct knowledge_base attribute
        if hasattr(runner, "knowledge_base"):
            kb = runner.knowledge_base
            if hasattr(kb, "info_uuid_to_info_dict"):
                for _uuid, info in kb.info_uuid_to_info_dict.items():
                    if isinstance(info, dict):
                        url = info.get("url", info.get("source_url", ""))
                    elif hasattr(info, "url"):
                        url = info.url
                    else:
                        continue
                    if url and url.startswith("http"):
                        title = info.get("title", "") if isinstance(info, dict) else getattr(info, "title", "")
                        snippets = info.get("snippets", []) if isinstance(info, dict) else getattr(info, "snippets", [])
                        url_info[url] = {"title": title, "snippets": snippets}
            if url_info:
                return url_info
    except Exception:
        pass

    try:
        # Strategy 3: runner.rm (retrieval module) may track URLs
        if hasattr(runner, "rm") and hasattr(runner.rm, "url_to_info"):
            for url, info in runner.rm.url_to_info.items():
                if url.startswith("http"):
                    url_info[url] = info if isinstance(info, dict) else {"title": str(info)}
            if url_info:
                return url_info
    except Exception:
        pass

    return {}


def run_costorm(
    topic: str,
    output_dir: str | None = None,
    serper_api_key: str | None = None,
) -> dict:
    """Run Co-STORM collaborative research on a topic.

    If Co-STORM is unavailable, transparently delegates to standard STORM.

    Returns dict with keys:
        article, url_to_info, error, search_backend, engine
    """
    if not _COSTORM_AVAILABLE:
        print("[costorm_runner] Co-STORM not available, falling back to STORM", file=sys.stderr)
        from .storm_runner import run_storm
        result = run_storm(
            topic=topic,
            output_dir=output_dir or settings.COSTORM_OUTPUT_DIR,
            serper_api_key=serper_api_key,
        )
        result["engine"] = "storm_fallback"
        return result

    # Co-STORM is available — run it
    serper_api_key = serper_api_key or settings.SERPER_API_KEY
    output_dir = output_dir or settings.COSTORM_OUTPUT_DIR
    timeout_seconds = settings.COSTORM_TIMEOUT_SECONDS

    if len(topic) > 100:
        topic = topic[:100].rsplit(" ", 1)[0]

    os.makedirs(output_dir, exist_ok=True)

    # Pick search backend (reuse storm_runner logic)
    from .storm_runner import _pick_search_rm
    rm, search_backend = _pick_search_rm(serper_api_key)
    if rm is None:
        return {
            "article": "",
            "url_to_info": {},
            "error": "All search backends unavailable",
            "search_backend": "none",
            "engine": "costorm",
        }

    # Build Co-STORM runner
    from knowledge_storm.lm import LitellmModel
    from knowledge_storm.storm_wiki import STORMWikiLMConfigs

    gemini_lm = LitellmModel(
        model="gemini/gemini-2.5-flash",
        api_key=settings.GEMINI_API_KEY,
        max_tokens=4096,
        temperature=0.7,
    )

    lm_configs = STORMWikiLMConfigs()
    lm_configs.set_conv_simulator_lm(gemini_lm)
    lm_configs.set_question_asker_lm(gemini_lm)
    lm_configs.set_outline_gen_lm(gemini_lm)
    lm_configs.set_article_gen_lm(gemini_lm)
    lm_configs.set_article_polish_lm(gemini_lm)

    storm_article = ""
    storm_error = None
    url_to_info = {}
    runner_instance = None

    def _run_inner_with_ref():
        nonlocal runner_instance
        runner = CoSTORMRunner(
            lm_configs=lm_configs,
            rm=rm,
            output_dir=output_dir,
        )
        runner_instance = runner
        return runner.run(topic=topic)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_inner_with_ref)
            try:
                result = future.result(timeout=timeout_seconds)
            except FuturesTimeoutError:
                storm_error = f"Co-STORM timed out after {timeout_seconds}s (backend: {search_backend})"
                result = None

        if result is not None:
            if hasattr(result, "article") and result.article:
                storm_article = result.article
            else:
                # Scan output dir for generated article files
                for search_dir in [
                    os.path.join(output_dir, topic.replace(" ", "_")),
                    output_dir,
                ]:
                    for fname in ["storm_gen_article_polished.txt", "storm_gen_article.txt"]:
                        fpath = os.path.join(search_dir, fname)
                        if os.path.exists(fpath):
                            with open(fpath) as f:
                                storm_article = f.read()
                            break
                    if storm_article:
                        break

    except Exception as e:
        storm_error = str(e)
        traceback.print_exc()

    # Try to recover partial article on error/timeout
    if not storm_article:
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if "article" in f.lower():
                    try:
                        with open(os.path.join(root, f)) as fp:
                            storm_article = fp.read()
                    except Exception:
                        pass
                    if storm_article:
                        break
            if storm_article:
                break

    # Extract references from Co-STORM runner's knowledge base
    # Co-STORM does NOT write url_to_info.json — refs live in the runner object
    url_to_info = _extract_costorm_refs(runner_instance)

    # Fallback: try url_to_info.json on disk (in case STORM fallback was used
    # or a future Co-STORM version writes it)
    if not url_to_info:
        for root, dirs, files in os.walk(output_dir):
            if "url_to_info.json" in files:
                try:
                    with open(os.path.join(root, "url_to_info.json")) as f:
                        url_to_info = json.load(f)
                except Exception:
                    pass
                break

    return {
        "article": storm_article,
        "url_to_info": url_to_info,
        "error": storm_error,
        "search_backend": search_backend,
        "engine": "costorm",
    }
