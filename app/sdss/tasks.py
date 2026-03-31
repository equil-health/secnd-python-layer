"""Celery tasks for SDSS — async second opinion via GPU pod.

The GPU pod itself is now async: POST /second_opinion returns a task_id
immediately, and GET /task/{pod_task_id} returns status/result. This
Celery task submits to the pod then polls every 10s until complete/failed.
No single HTTP request through ngrok lasts more than a few seconds.
"""

import logging
import time
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

POLL_HEADERS = {
    "ngrok-skip-browser-warning": "true",
}

POLL_INTERVAL = 10  # seconds between polls
SUBMIT_TIMEOUT = 30  # seconds for the initial submit call
POLL_TIMEOUT = 15    # seconds per poll request


@app.task(bind=True, name="sdss.run_analysis", soft_time_limit=900, time_limit=960)
def run_analysis(self, task_id: str):
    """Submit to GPU pod, then poll for result. Store in DB when done."""
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

        # ── Step 1: Submit to GPU pod (returns task_id immediately) ──
        if task.mode == "medgemma":
            url = f"{base_url}/query"
            payload = {"query": task.case_text, "india_context": task.india_context}
        else:
            url = f"{base_url}/second_opinion"
            payload = {"case_text": task.case_text, "mode": task.mode}

        logger.info(f"SDSS task {task_id}: submitting to {url} (mode={task.mode})")

        resp = http_requests.post(url, json=payload, headers=NGROK_HEADERS, timeout=SUBMIT_TIMEOUT)
        resp.raise_for_status()
        submit_data = resp.json()
        pod_task_id = submit_data["task_id"]

        logger.info(f"SDSS task {task_id}: GPU pod accepted, pod_task_id={pod_task_id}")

        # ── Step 2: Poll GPU pod until complete/failed ───────────────
        poll_url = f"{base_url}/task/{pod_task_id}"

        while True:
            time.sleep(POLL_INTERVAL)

            poll_resp = http_requests.get(poll_url, headers=POLL_HEADERS, timeout=POLL_TIMEOUT)
            poll_resp.raise_for_status()
            pod_status = poll_resp.json()

            if pod_status["status"] == "complete":
                task.result = pod_status["result"]
                task.status = "complete"
                task.completed_at = datetime.now(timezone.utc)
                session.commit()
                logger.info(f"SDSS task {task_id}: complete")
                return {"status": "complete", "task_id": task_id}

            elif pod_status["status"] == "failed":
                task.status = "failed"
                task.error = pod_status.get("error", "GPU pod analysis failed")
                task.completed_at = datetime.now(timezone.utc)
                session.commit()
                logger.error(f"SDSS task {task_id}: GPU pod failed — {task.error}")
                return {"status": "failed", "task_id": task_id}

            # Still processing — loop continues
            logger.debug(f"SDSS task {task_id}: still processing on GPU pod")

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
            task.status = "failed"
            task.error = f"Could not reach GPU pod: {str(e)[:500]}"
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
