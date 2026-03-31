"""Celery tasks for SDSS — submit case to GPU pod with callback URL.

The GPU pod accepts POST /second_opinion/start and returns a task_id
instantly. When analysis completes (5-10 min), the pod POSTs the result
back to our webhook endpoint. No polling, no long HTTP requests through ngrok.
"""

import logging
from datetime import datetime, timezone

import requests as http_requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

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

SUBMIT_TIMEOUT = 30  # seconds — the /start endpoint responds instantly


@app.task(bind=True, name="sdss.run_analysis", soft_time_limit=60, time_limit=90)
def run_analysis(self, task_id: str):
    """Submit case to GPU pod with callback URL. Completes in <2 seconds."""
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
        callback_url = f"{settings.BACKEND_PUBLIC_URL.rstrip('/')}/webhook/sdss/{task_id}"

        url = f"{base_url}/second_opinion/start"
        payload = {
            "case_text": task.case_text,
            "mode": task.mode,
            "callback_url": callback_url,
        }

        logger.info(f"SDSS task {task_id}: submitting to {url} (mode={task.mode}, callback={callback_url})")

        resp = http_requests.post(url, json=payload, headers=NGROK_HEADERS, timeout=SUBMIT_TIMEOUT)
        resp.raise_for_status()
        submit_data = resp.json()

        task.pod_task_id = submit_data.get("task_id")
        session.commit()

        logger.info(f"SDSS task {task_id}: GPU pod accepted, pod_task_id={task.pod_task_id}")
        return {"status": "submitted", "task_id": task_id, "pod_task_id": task.pod_task_id}

    except (http_requests.exceptions.ConnectionError, http_requests.exceptions.Timeout) as e:
        logger.warning(f"SDSS task {task_id}: GPU pod unreachable — {e}")
        if task:
            task.status = "failed"
            task.error = "GPU pod is offline or unreachable. Please try again later."
            task.completed_at = datetime.now(timezone.utc)
            session.commit()

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
