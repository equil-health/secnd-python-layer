"""Breaking Celery tasks — daily refresh (5-step v5.0) + monthly reset."""

import logging
from datetime import date, datetime

from celery import current_app as app
from celery.exceptions import SoftTimeLimitExceeded

from .breaking_fetcher import fetch_breaking_headlines, active_specialties
from .breaking_ranker import rank_headlines, verify_breaking_sources, assign_urgency
from .breaking_store import store_headlines

logger = logging.getLogger(__name__)


@app.task(bind=True, name="breaking.daily_refresh",
          soft_time_limit=120, time_limit=180)
def breaking_daily_refresh(self):
    """Daily 05:00 IST Breaking pipeline — 5 steps (v5.0).

    B1 → B2 → B2.5 → B3 → B4
    """
    today = date.today()
    all_headlines = {}
    total_alerts = 0

    logger.info(f"[Breaking] Starting daily refresh for {today}")

    try:
        for specialty in active_specialties():
            # B1 — Fetch
            raw = fetch_breaking_headlines(specialty, skip_cache=True)

            # B2 — Semantic dedup + Gemini rank
            ranked = rank_headlines(raw, specialty)

            # B2.5 — OpenAlex verify (v5.0)
            verified = verify_breaking_sources(ranked)

            # B3 — Urgency classification
            classified = assign_urgency(verified, specialty)

            all_headlines[specialty] = classified
            total_alerts += sum(
                1 for h in classified if h.get("urgency_tier") == "ALERT"
            )

        # B4 — Store
        store_headlines(all_headlines, today)

        # Push notifications (optional — requires firebase setup)
        try:
            _send_push_notifications(all_headlines)
        except Exception as e:
            logger.warning(f"[Breaking] Push notifications failed (non-fatal): {e}")

        total = sum(len(v) for v in all_headlines.values())
        logger.info(
            f"[Breaking] Daily refresh complete: {total} headlines, "
            f"{total_alerts} ALERTs across {len(all_headlines)} specialties"
        )

        return {
            "status": "complete",
            "date": today.isoformat(),
            "total_headlines": total,
            "alert_count": total_alerts,
            "specialties": len(all_headlines),
        }

    except SoftTimeLimitExceeded:
        logger.error("[Breaking] Daily refresh hit soft time limit")
        # Store whatever we have so far
        if all_headlines:
            store_headlines(all_headlines, today)
        raise

    except Exception as e:
        logger.error(f"[Breaking] Daily refresh failed: {e}", exc_info=True)
        raise


@app.task(bind=True, name="breaking.reset_monthly_free_reports")
def reset_monthly_free_reports(self):
    """Reset free_reports_used for all trial doctors on 1st of month."""
    from dateutil.relativedelta import relativedelta
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ..config import settings
    from ..models.breaking import DoctorPreferences

    sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
    engine = create_engine(sync_url)
    Session = sessionmaker(bind=engine)

    today = date.today()
    next_reset = (today.replace(day=1) + relativedelta(months=1))

    with Session() as db:
        count = (
            db.query(DoctorPreferences)
            .filter(
                DoctorPreferences.subscription_tier == None,  # noqa: E711
                DoctorPreferences.trial_ends_at > datetime.utcnow(),
            )
            .update({
                "free_reports_used": 0,
                "free_reports_reset": next_reset,
            })
        )
        db.commit()

    logger.info(f"[Breaking] Monthly reset: {count} doctors reset, next reset {next_reset}")
    return {"status": "complete", "reset_count": count, "next_reset": next_reset.isoformat()}


def _send_push_notifications(all_headlines: dict[str, list[dict]]):
    """Send FCM push notifications after headlines are stored.

    ALERTs → high priority, immediate delivery.
    No ALERTs → normal priority daily digest.
    """
    try:
        from ..notifications import send_breaking_notifications
        send_breaking_notifications(all_headlines)
    except ImportError:
        logger.debug("[Breaking] notifications module not available — skipping push")
