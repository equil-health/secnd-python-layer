"""Scanner — article discovery via PubMed E-Search + MeSH terms."""

import logging
from datetime import datetime, timedelta, timezone

from ..config import settings
from .abstract_fetcher import AbstractFetcher
from .journal_registry import grade_evidence, JOURNAL_REGISTRY

logger = logging.getLogger(__name__)

# Specialty → MeSH terms mapping
SPECIALTY_MESH = {
    "Cardiology": ["Cardiovascular Diseases", "Heart Failure", "Myocardial Infarction", "Arrhythmias, Cardiac"],
    "Neurology": ["Nervous System Diseases", "Stroke", "Epilepsy", "Neurodegenerative Diseases"],
    "Oncology": ["Neoplasms", "Antineoplastic Agents", "Immunotherapy", "Radiation Oncology"],
    "Pulmonology": ["Lung Diseases", "Pulmonary Disease, Chronic Obstructive", "Asthma", "Pneumonia"],
    "Endocrinology": ["Endocrine System Diseases", "Diabetes Mellitus", "Thyroid Diseases", "Obesity"],
    "Rheumatology": ["Rheumatic Diseases", "Arthritis, Rheumatoid", "Lupus Erythematosus, Systemic"],
    "Gastroenterology": ["Gastrointestinal Diseases", "Inflammatory Bowel Diseases", "Liver Diseases"],
    "Nephrology": ["Kidney Diseases", "Renal Insufficiency, Chronic", "Glomerulonephritis"],
    "Infectious Disease": ["Communicable Diseases", "Anti-Bacterial Agents", "HIV Infections", "Sepsis"],
    "Hematology": ["Hematologic Diseases", "Anemia", "Leukemia", "Blood Coagulation Disorders"],
    "Psychiatry": ["Mental Disorders", "Depressive Disorder", "Schizophrenia", "Anxiety Disorders"],
    "Dermatology": ["Skin Diseases", "Psoriasis", "Dermatitis", "Melanoma"],
    "Ophthalmology": ["Eye Diseases", "Glaucoma", "Macular Degeneration", "Diabetic Retinopathy"],
    "Pediatrics": ["Pediatrics", "Child Development", "Infant, Newborn, Diseases", "Congenital Abnormalities"],
    "Emergency Medicine": ["Emergency Medicine", "Wounds and Injuries", "Critical Care", "Resuscitation"],
}


def build_pubmed_query(specialty: str, topics: list[str], mesh_terms: list[str] | None = None) -> str:
    """Build a PubMed search query combining MeSH terms and free-text topics.

    Strategy: (MeSH for specialty) AND (free-text topics OR'd)
    """
    parts = []

    # MeSH terms for the specialty
    specialty_mesh = SPECIALTY_MESH.get(specialty, [])
    if mesh_terms:
        specialty_mesh = list(set(specialty_mesh + mesh_terms))

    if specialty_mesh:
        mesh_clauses = [f'"{term}"[MeSH Terms]' for term in specialty_mesh]
        parts.append(f"({' OR '.join(mesh_clauses)})")

    # Free-text topics
    if topics:
        topic_clauses = [f'"{topic}"[Title/Abstract]' for topic in topics]
        parts.append(f"({' OR '.join(topic_clauses)})")

    if not parts:
        # Fallback: just search by specialty name
        return f'"{specialty}"[MeSH Terms]'

    return " AND ".join(parts)


def _compute_relevance_score(article: dict, topics: list[str]) -> float:
    """Compute a simple relevance score (0-1) based on topic matches in title/abstract."""
    if not topics:
        return 0.5

    text = f"{article.get('title', '')} {article.get('abstract', '')}".lower()
    matches = sum(1 for t in topics if t.lower() in text)
    score = min(matches / max(len(topics), 1), 1.0)

    # Boost for having abstract
    if article.get("abstract"):
        score = min(score + 0.1, 1.0)

    return round(score, 2)


def scan_for_articles(
    specialty: str,
    topics: list[str],
    mesh_terms: list[str] | None = None,
    enabled_journals: list[str] | None = None,
    days_back: int | None = None,
    max_articles: int | None = None,
    skip_cache: bool = False,
) -> list[dict]:
    """Orchestrate article discovery: search + fetch + grade + score.

    Returns list of article dicts enriched with evidence_grade and relevance_score.
    """
    if days_back is None:
        days_back = settings.PULSE_SCAN_DAYS_BACK
    if max_articles is None:
        max_articles = settings.PULSE_MAX_ARTICLES_PER_DIGEST

    # Build query
    query = build_pubmed_query(specialty, topics, mesh_terms)

    # If journals are specified, add journal filter
    if enabled_journals:
        journal_names = []
        for j_key in enabled_journals:
            j_info = JOURNAL_REGISTRY.get(j_key)
            if j_info:
                journal_names.append(f'"{j_info["name"]}"[Journal]')
        if journal_names:
            query = f"({query}) AND ({' OR '.join(journal_names)})"

    logger.info(f"Pulse scan — specialty={specialty}, topics={topics}, skip_cache={skip_cache}")
    logger.info(f"Pulse scan query: {query}")

    # Date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days_back)
    date_start = start_date.strftime("%Y/%m/%d")
    date_end = end_date.strftime("%Y/%m/%d")

    # Search
    fetcher = AbstractFetcher()
    pmids = fetcher.search_pubmed(query, date_start, date_end, max_results=max_articles * 2, skip_cache=skip_cache)
    logger.info(f"Pulse scan — got {len(pmids)} PMIDs (skip_cache={skip_cache})")

    if not pmids:
        logger.info("No PMIDs found for query")
        return []

    # Fetch articles
    articles = fetcher.fetch_pubmed_articles(pmids[:max_articles * 2])

    # Try Crossref for articles missing abstracts
    for article in articles:
        if not article.get("abstract") and article.get("doi"):
            crossref = fetcher.fetch_crossref(article["doi"])
            if crossref and crossref.get("abstract"):
                article["abstract"] = crossref["abstract"]

    # Enrich with evidence grading and relevance scoring
    for article in articles:
        article["evidence_grade"] = grade_evidence(article.get("pub_types", []))
        article["relevance_score"] = _compute_relevance_score(article, topics)

    # Sort by relevance, then take top N
    articles.sort(key=lambda a: a.get("relevance_score", 0), reverse=True)
    articles = articles[:max_articles]

    logger.info(f"Pulse scan returned {len(articles)} articles")
    return articles
