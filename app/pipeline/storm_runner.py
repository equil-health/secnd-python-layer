"""STORM execution wrapper with search fallback and timeout protection."""

import os
import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from ..config import settings


def _check_google_cse_health() -> bool:
    """Quick health check — returns True if Google CSE credentials are configured."""
    return bool(settings.GOOGLE_SEARCH_API_KEY and settings.GOOGLE_CSE_ID)


def _pick_search_rm(serper_api_key: str):
    """Pick the best available search RM: Google CSE → Serper → DuckDuckGo.

    Returns (rm_instance, rm_name).
    """
    # 1. Serper (preferred)
    if serper_api_key:
        try:
            from .serper import check_serper_health
            if check_serper_health():
                from knowledge_storm.rm import SerperRM
                rm = SerperRM(
                    serper_search_api_key=serper_api_key,
                    query_params={"autocorrect": True, "num": 10},
                )
                return rm, "serper"
            else:
                print("[storm_runner] Serper health check failed, skipping", file=sys.stderr)
        except Exception as e:
            print(f"[storm_runner] Serper init failed: {e}", file=sys.stderr)

    # 2. Google Custom Search (backup)
    if _check_google_cse_health():
        try:
            from knowledge_storm.rm import GoogleSearch
            rm = GoogleSearch(
                google_search_api_key=settings.GOOGLE_SEARCH_API_KEY,
                google_cse_id=settings.GOOGLE_CSE_ID,
                k=3,
            )
            return rm, "google_cse"
        except Exception as e:
            print(f"[storm_runner] Google CSE init failed: {e}", file=sys.stderr)

    # 3. DuckDuckGo (free fallback, no key needed)
    try:
        from knowledge_storm.rm import DuckDuckGoSearchRM
        rm = DuckDuckGoSearchRM(k=3, safe_search="Off")
        print("[storm_runner] Using DuckDuckGo fallback search", file=sys.stderr)
        return rm, "duckduckgo"
    except Exception as e:
        print(f"[storm_runner] DuckDuckGo init failed: {e}", file=sys.stderr)

    return None, "none"


def _run_storm_inner(runner, topic: str) -> object:
    """Execute runner.run() — intended to be called inside a thread for timeout."""
    return runner.run(
        topic=topic,
        do_research=True,
        do_generate_outline=True,
        do_generate_article=True,
        do_polish_article=True,
    )


def run_storm(
    topic: str,
    output_dir: str,
    project_id: str | None = None,
    location: str | None = None,
    credentials_json: str | None = None,
    serper_api_key: str | None = None,
) -> dict:
    """Run STORM deep research on a topic.

    Search priority: Google CSE → Serper → DuckDuckGo.
    Wraps execution in a timeout (default 180s) to prevent hangs.

    Returns dict with keys: article, url_to_info, error, search_backend.
    """
    project_id = project_id or settings.GCP_PROJECT_ID
    location = location or settings.GCP_LOCATION
    serper_api_key = serper_api_key or settings.SERPER_API_KEY
    timeout_seconds = settings.STORM_TIMEOUT_SECONDS

    # Read credentials JSON from service account file
    if credentials_json is None:
        with open(settings.GCP_SERVICE_ACCOUNT_FILE) as f:
            credentials_json = f.read()

    # Truncate topic for folder safety
    if len(topic) > 100:
        topic = topic[:100].rsplit(" ", 1)[0]

    os.makedirs(output_dir, exist_ok=True)

    # --- Pick search backend with fallback ---
    rm, search_backend = _pick_search_rm(serper_api_key)
    if rm is None:
        return {
            "article": "",
            "url_to_info": {},
            "error": "All search backends unavailable (Google CSE, Serper, DuckDuckGo)",
            "search_backend": "none",
        }

    print(f"[storm_runner] Using search backend: {search_backend}", file=sys.stderr)

    # --- Build STORM runner ---
    from knowledge_storm import STORMWikiRunnerArguments, STORMWikiRunner
    from knowledge_storm.lm import LitellmModel
    from knowledge_storm.storm_wiki import STORMWikiLMConfigs

    gemini_lm = LitellmModel(
        model="vertex_ai/gemini-2.0-flash",
        vertex_project=project_id,
        vertex_location=location,
        vertex_credentials=credentials_json,
        max_tokens=4096,
        temperature=0.7,
    )

    lm_configs = STORMWikiLMConfigs()
    lm_configs.set_conv_simulator_lm(gemini_lm)
    lm_configs.set_question_asker_lm(gemini_lm)
    lm_configs.set_outline_gen_lm(gemini_lm)
    lm_configs.set_article_gen_lm(gemini_lm)
    lm_configs.set_article_polish_lm(gemini_lm)

    engine_args = STORMWikiRunnerArguments(
        output_dir=output_dir,
        search_top_k=settings.STORM_SEARCH_TOP_K,
    )
    runner = STORMWikiRunner(engine_args, lm_configs, rm)

    # --- Execute with timeout ---
    storm_article = ""
    storm_error = None
    url_to_info = {}

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_run_storm_inner, runner, topic)
            try:
                result = future.result(timeout=timeout_seconds)
            except FuturesTimeoutError:
                storm_error = f"STORM timed out after {timeout_seconds}s (backend: {search_backend})"
                print(f"[storm_runner] {storm_error}", file=sys.stderr)
                # Don't wait for thread — Celery hard limit will clean up
                result = None

        # Extract article from result
        if result is not None:
            if hasattr(result, "article") and result.article:
                storm_article = result.article
            else:
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

    # Try to recover partial article even on error/timeout
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
    }
