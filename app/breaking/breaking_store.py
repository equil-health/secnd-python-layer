"""Breaking Step B4 — Store headlines to Redis + PostgreSQL.

Redis: 12h TTL for fast API reads.
PostgreSQL: permanent storage, deduped by date+specialty on re-runs.
Retraction double-check before write (defensive).
"""

import json
import logging
from datetime import date

import redis
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session, sessionmaker

from ..config import settings
from ..models.breaking import BreakingHeadline

logger = logging.getLogger(__name__)

_redis = redis.Redis.from_url(settings.REDIS_URL)

# Sync engine for Celery tasks (same pattern as pipeline/tasks.py)
_sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
if _sync_url.startswith("postgresql://"):
    pass  # already correct
elif not "+psycopg2" in _sync_url:
    _sync_url = _sync_url.replace("postgresql://", "postgresql+psycopg2://")

_sync_engine = create_engine(_sync_url)
SyncSession = sessionmaker(bind=_sync_engine)


def store_headlines(all_headlines: dict[str, list[dict]], today: date):
    """B4: Write headlines to Redis (12h TTL) + PostgreSQL.

    Args:
        all_headlines: {specialty: [headline_dict, ...]}
        today: Current date for dedup
    """
    redis_ttl = settings.BREAKING_REDIS_TTL_HOURS * 3600

    # Redis — fast read cache
    try:
        for specialty, headlines in all_headlines.items():
            redis_key = f"breaking:{today.isoformat()}:{specialty}"
            _redis.setex(redis_key, redis_ttl, json.dumps(headlines))
        logger.info(f"[Breaking B4] Redis: stored {len(all_headlines)} specialties")
    except redis.ConnectionError as e:
        logger.error(f"[Breaking B4] Redis write failed: {e}")

    # PostgreSQL — permanent storage
    total_stored = 0
    with SyncSession() as db:
        for specialty, headlines in all_headlines.items():
            # Delete today's existing records for this specialty (re-run safety)
            db.execute(
                delete(BreakingHeadline).where(
                    BreakingHeadline.date == today,
                    BreakingHeadline.specialty == specialty,
                )
            )

            for h in headlines:
                # Defensive: skip retracted even if verify_breaking_sources missed it
                if h.get("is_retracted"):
                    logger.error(
                        f"[Breaking B4] Retracted headline reached store — filtered. "
                        f"URL: {h.get('url', 'unknown')}"
                    )
                    continue

                db.add(BreakingHeadline(
                    date=today,
                    specialty=specialty,
                    title=h.get("title", ""),
                    url=h.get("url", ""),
                    source=h.get("source"),
                    snippet=h.get("snippet"),
                    urgency_tier=h.get("urgency_tier", "NEW"),
                    urgency_reason=h.get("urgency_reason"),
                    rank_score=h.get("rank_score", 50),
                    rank_position=h.get("rank_position", 0),
                    research_topic=h.get("research_topic"),
                    published_at=h.get("published_at"),
                    is_verified=h.get("is_verified", False),
                    citation_count=h.get("citation_count"),
                    quality_tier=h.get("quality_tier"),
                    is_retracted=False,  # confirmed non-retracted by this point
                ))
                total_stored += 1

            db.commit()

    logger.info(f"[Breaking B4] PostgreSQL: stored {total_stored} headlines across {len(all_headlines)} specialties")


def get_headlines_from_redis(today: date, specialties: list[str]) -> dict[str, list[dict]] | None:
    """Read today's headlines from Redis cache.

    Returns None if any specialty is missing (fallback to PostgreSQL).
    """
    result = {}
    try:
        for sp in specialties:
            redis_key = f"breaking:{today.isoformat()}:{sp}"
            data = _redis.get(redis_key)
            if data:
                result[sp] = json.loads(data)
            else:
                return None  # cache miss — fallback to DB
        return result
    except redis.ConnectionError:
        return None


def get_headlines_from_db(today: date, specialties: list[str]) -> dict[str, list[dict]]:
    """Read today's headlines from PostgreSQL."""
    result = {sp: [] for sp in specialties}

    with SyncSession() as db:
        rows = (
            db.query(BreakingHeadline)
            .filter(
                BreakingHeadline.date == today,
                BreakingHeadline.specialty.in_(specialties),
            )
            .order_by(BreakingHeadline.rank_position)
            .all()
        )

        for row in rows:
            result[row.specialty].append({
                "id": str(row.id),
                "date": row.date.isoformat(),
                "specialty": row.specialty,
                "title": row.title,
                "url": row.url,
                "source": row.source,
                "snippet": row.snippet,
                "urgency_tier": row.urgency_tier,
                "urgency_reason": row.urgency_reason,
                "rank_score": row.rank_score,
                "rank_position": row.rank_position,
                "research_topic": row.research_topic,
                "published_at": row.published_at,
                "is_verified": row.is_verified,
                "citation_count": row.citation_count,
                "quality_tier": row.quality_tier,
                "is_retracted": row.is_retracted,
            })

    return result


def get_headlines_for_doctor(
    doctor_specialties: list[str],
    today: date,
) -> dict[str, list[dict]]:
    """Get today's headlines for a doctor's specialties.

    Tries Redis first, falls back to PostgreSQL.
    """
    # Try Redis
    cached = get_headlines_from_redis(today, doctor_specialties)
    if cached is not None:
        return cached

    # Fallback to PostgreSQL
    return get_headlines_from_db(today, doctor_specialties)
