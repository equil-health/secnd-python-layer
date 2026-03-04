"""Celery tasks for Pulse — medical literature digest generation."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from celery.exceptions import SoftTimeLimitExceeded
from celery_app import app
from ..config import settings
from ..models.pulse import PulsePreference, PulseDigest, PulseArticle

logger = logging.getLogger(__name__)

# Sync engine for Celery tasks (same pattern as pipeline/tasks.py)
_sync_engine = None
_SessionLocal = None


def _get_sync_session() -> Session:
    global _sync_engine, _SessionLocal
    if _sync_engine is None:
        # Convert async URL to sync — same pattern as pipeline/tasks.py
        db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2").replace("postgresql://", "postgresql+psycopg2://")
        if "psycopg2+psycopg2" in db_url:
            db_url = db_url.replace("psycopg2+psycopg2", "psycopg2")
        _sync_engine = create_engine(db_url)
        _SessionLocal = sessionmaker(bind=_sync_engine)
    return _SessionLocal()


@app.task(bind=True, name="pulse.generate_all_digests", soft_time_limit=240)
def generate_all_digests(self, frequency_filter: str = "daily"):
    """Fan-out task: find all enabled preferences matching frequency and dispatch per-user tasks."""
    if not settings.PULSE_ENABLED:
        logger.info("Pulse is disabled via PULSE_ENABLED=false, skipping")
        return {"status": "skipped", "reason": "pulse_disabled"}

    session = _get_sync_session()
    try:
        prefs = session.execute(
            select(PulsePreference).where(
                PulsePreference.is_enabled == True,
                PulsePreference.frequency == frequency_filter,
            )
        ).scalars().all()

        dispatched = 0
        for i, pref in enumerate(prefs):
            generate_pulse_digest.apply_async(
                args=[str(pref.user_id)],
                countdown=i * 2,  # stagger by 2s to respect PubMed rate limits
            )
            dispatched += 1

        logger.info(f"Pulse: dispatched {dispatched} {frequency_filter} digest tasks")
        return {"status": "dispatched", "count": dispatched, "frequency": frequency_filter}
    finally:
        session.close()


@app.task(bind=True, name="pulse.generate_pulse_digest", soft_time_limit=240)
def generate_pulse_digest(self, user_id: str):
    """Generate a full Pulse digest for a single user.

    Pipeline: load prefs → scan PubMed → fetch abstracts → generate TL;DRs → save to DB.
    """
    if not settings.PULSE_ENABLED:
        return {"status": "skipped", "reason": "pulse_disabled"}

    session = _get_sync_session()
    digest = None
    try:
        # Load user preferences
        pref = session.execute(
            select(PulsePreference).where(PulsePreference.user_id == user_id)
        ).scalar_one_or_none()

        if not pref:
            logger.warning(f"Pulse: no preferences found for user {user_id}")
            return {"status": "skipped", "reason": "no_preferences"}

        if not pref.is_enabled:
            return {"status": "skipped", "reason": "disabled"}

        # Create digest record
        now = datetime.now(timezone.utc)
        date_range_start = now - timedelta(days=settings.PULSE_SCAN_DAYS_BACK)
        digest = PulseDigest(
            user_id=user_id,
            status="generating",
            specialty_used=pref.specialty,
            topics_used=pref.topics,
            date_range_start=date_range_start,
            date_range_end=now,
        )
        session.add(digest)
        session.commit()

        # Scan for articles
        from .scanner import scan_for_articles
        articles = scan_for_articles(
            specialty=pref.specialty,
            topics=pref.topics or [],
            mesh_terms=pref.mesh_terms,
            enabled_journals=pref.enabled_journals,
            days_back=settings.PULSE_SCAN_DAYS_BACK,
            max_articles=settings.PULSE_MAX_ARTICLES_PER_DIGEST,
        )

        if not articles:
            digest.status = "complete"
            digest.article_count = 0
            digest.generated_at = datetime.now(timezone.utc)
            session.commit()
            return {"status": "complete", "articles": 0}

        # Generate TL;DRs
        from .tldr_generator import generate_batch_tldrs
        articles = generate_batch_tldrs(articles)

        # Save articles to DB
        for article_data in articles:
            pub_date = None
            if article_data.get("published_date"):
                try:
                    pub_date = datetime.strptime(article_data["published_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    pub_date = None

            article = PulseArticle(
                digest_id=digest.id,
                title=article_data.get("title", ""),
                authors=article_data.get("authors", []),
                journal=article_data.get("journal", ""),
                doi=article_data.get("doi", ""),
                pmid=article_data.get("pmid", ""),
                published_date=pub_date,
                abstract=article_data.get("abstract", ""),
                article_url=article_data.get("article_url", ""),
                tldr=article_data.get("tldr", ""),
                evidence_grade=article_data.get("evidence_grade", ""),
                relevance_score=article_data.get("relevance_score", 0),
                source="pubmed",
                access_strategy=article_data.get("access_strategy", "proxy_via_pubmed"),
            )
            session.add(article)

        digest.status = "complete"
        digest.article_count = len(articles)
        digest.generated_at = datetime.now(timezone.utc)
        session.commit()

        logger.info(f"Pulse: digest {digest.id} complete with {len(articles)} articles for user {user_id}")
        return {"status": "complete", "digest_id": str(digest.id), "articles": len(articles)}

    except SoftTimeLimitExceeded:
        logger.warning(f"Pulse: digest generation timed out for user {user_id}")
        if digest:
            digest.status = "failed"
            digest.error_message = "Task timed out"
            session.commit()
        raise
    except Exception as e:
        logger.error(f"Pulse: digest generation failed for user {user_id}: {e}")
        if digest:
            digest.status = "failed"
            digest.error_message = str(e)[:500]
            session.commit()
        raise
    finally:
        session.close()
