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
        model="gemini/gemini-2.0-flash",
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

    def _run_inner():
        runner = CoSTORMRunner(
            lm_configs=lm_configs,
            rm=rm,
            output_dir=output_dir,
        )
        return runner.run(topic=topic)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_inner)
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

    # Read url_to_info.json
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
