"""STORM execution wrapper — ported from v5 lines 746-901."""

import os
import json
import shutil
import traceback

from ..config import settings


def run_storm(
    topic: str,
    output_dir: str,
    project_id: str | None = None,
    location: str | None = None,
    credentials_json: str | None = None,
    serper_api_key: str | None = None,
) -> dict:
    """Run STORM deep research on a diagnostic topic.

    Returns dict with keys: article, url_to_info, error.
    """
    project_id = project_id or settings.GCP_PROJECT_ID
    location = location or settings.GCP_LOCATION
    serper_api_key = serper_api_key or settings.SERPER_API_KEY

    # Read credentials JSON from service account file
    if credentials_json is None:
        with open(settings.GCP_SERVICE_ACCOUNT_FILE) as f:
            credentials_json = f.read()

    # Truncate topic for folder safety
    if len(topic) > 100:
        topic = topic[:100].rsplit(" ", 1)[0]

    # Ensure output dir
    os.makedirs(output_dir, exist_ok=True)

    from knowledge_storm import STORMWikiRunnerArguments, STORMWikiRunner
    from knowledge_storm.lm import LitellmModel
    from knowledge_storm.storm_wiki import STORMWikiLMConfigs
    from knowledge_storm.rm import SerperRM

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

    rm = SerperRM(
        serper_search_api_key=serper_api_key,
        query_params={"autocorrect": True, "num": 10},
    )

    engine_args = STORMWikiRunnerArguments(
        output_dir=output_dir,
        search_top_k=settings.STORM_SEARCH_TOP_K,
    )
    runner = STORMWikiRunner(engine_args, lm_configs, rm)

    storm_article = ""
    storm_error = None
    url_to_info = {}

    try:
        result = runner.run(
            topic=topic,
            do_research=True,
            do_generate_outline=True,
            do_generate_article=True,
            do_polish_article=True,
        )

        # Extract article
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

        # Try to recover any article from output dir
        for root, dirs, files in os.walk(output_dir):
            for f in files:
                if "article" in f.lower() and not storm_article:
                    with open(os.path.join(root, f)) as fp:
                        storm_article = fp.read()

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
    }
