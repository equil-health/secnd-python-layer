"""TL;DR generator — Gemini-powered 3-sentence clinical summaries."""

import logging

from ..pipeline.gemini import call_gemini

logger = logging.getLogger(__name__)

TLDR_PROMPT_TEMPLATE = """Summarize the following medical research article for a Board-Certified Physician.
Focus on clinical outcomes, p-values, and patient cohort size.
Return exactly 3 sentences. No bullet points, no headers.

Title: {title}
Journal: {journal}
Authors: {authors}

Abstract:
{abstract}"""


def generate_tldr(article: dict) -> str:
    """Generate a 3-sentence physician-level TL;DR for a single article."""
    abstract = article.get("abstract", "")
    if not abstract:
        return "No abstract available for summarization."

    authors_str = ", ".join(article.get("authors", [])[:5])
    if len(article.get("authors", [])) > 5:
        authors_str += " et al."

    prompt = TLDR_PROMPT_TEMPLATE.format(
        title=article.get("title", ""),
        journal=article.get("journal", ""),
        authors=authors_str,
        abstract=abstract,
    )

    try:
        return call_gemini(prompt, max_tokens=512, temperature=0.2)
    except Exception as e:
        logger.error(f"TL;DR generation failed for PMID {article.get('pmid', '?')}: {e}")
        return f"Summary generation failed: {str(e)[:100]}"


def generate_batch_tldrs(articles: list[dict]) -> list[dict]:
    """Generate TL;DRs for a batch of articles sequentially.

    Returns the same article dicts with 'tldr' field populated.
    """
    for article in articles:
        article["tldr"] = generate_tldr(article)
    return articles
