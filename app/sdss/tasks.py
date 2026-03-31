"""Celery tasks for SDSS — async second opinion via GPU pod."""

import logging
from datetime import datetime, timezone

import requests as http_requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from celery.exceptions import SoftTimeLimitExceeded
from celery_app import app
from ..config import settings
from ..models.sdss_task import SdssTask

logger = logging.getLogger(__name__)

_sync_engine = None
_SessionLocal = None


def _get_sync_session() -> Session:
    global _sync_engine, _SessionLocal
    if _sync_engine is None:
        db_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2").replace("postgresql://", "postgresql+psycopg2://")
        if "psycopg2+psycopg2" in db_url:
            db_url = db_url.replace("psycopg2+psycopg2", "psycopg2")
        _sync_engine = create_engine(db_url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_sync_engine, expire_on_commit=True)
    return _SessionLocal()


NGROK_HEADERS = {
    "Content-Type": "application/json",
    "ngrok-skip-browser-warning": "true",
}


@app.task(bind=True, name="sdss.run_analysis", soft_time_limit=900, time_limit=960)
def run_analysis(self, task_id: str):
    """Call GPU pod for second opinion and store result in DB."""
    session = _get_sync_session()
    task = None
    try:
        task = session.execute(
            select(SdssTask).where(SdssTask.id == task_id)
        ).scalar_one_or_none()

        if not task:
            logger.error(f"SdssTask {task_id} not found")
            return {"status": "error", "detail": "task not found"}

        task.status = "processing"
        session.commit()

        base_url = settings.SDSS_BASE_URL.rstrip("/")

        if task.mode == "medgemma":
            url = f"{base_url}/query"
            payload = {"query": task.case_text, "india_context": task.india_context}
        else:
            url = f"{base_url}/second_opinion"
            payload = {"case_text": task.case_text, "mode": task.mode}

        logger.info(f"SDSS task {task_id}: calling {url} (mode={task.mode})")

        resp = http_requests.post(url, json=payload, headers=NGROK_HEADERS, timeout=840)
        resp.raise_for_status()

        task.result = resp.json()
        task.status = "complete"
        task.completed_at = datetime.now(timezone.utc)
        session.commit()

        logger.info(f"SDSS task {task_id}: complete")
        return {"status": "complete", "task_id": task_id}

    except SoftTimeLimitExceeded:
        logger.error(f"SDSS task {task_id}: soft time limit exceeded")
        if task:
            task.status = "failed"
            task.error = "Analysis timed out (15 min limit). The GPU pod may be overloaded."
            task.completed_at = datetime.now(timezone.utc)
            session.commit()
        raise

    except (http_requests.exceptions.ConnectionError, http_requests.exceptions.Timeout) as e:
        logger.warning(f"SDSS task {task_id}: connection error — {e}")
        if task:
            task.status = "pending"
            session.commit()
        raise self.retry(countdown=30, max_retries=1, exc=e)

    except http_requests.exceptions.HTTPError as e:
        logger.error(f"SDSS task {task_id}: HTTP error — {e}")
        if task:
            task.status = "failed"
            task.error = f"GPU pod returned error: {e.response.status_code} {e.response.text[:500]}"
            task.completed_at = datetime.now(timezone.utc)
            session.commit()

    except Exception as e:
        logger.error(f"SDSS task {task_id}: unexpected error — {e}", exc_info=True)
        if task:
            task.status = "failed"
            task.error = str(e)[:1000]
            task.completed_at = datetime.now(timezone.utc)
            session.commit()

    finally:
        session.close()
