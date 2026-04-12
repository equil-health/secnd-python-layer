"""Celery tasks for SDSS — submit case to GPU pod with callback URL.

The GPU pod accepts POST /second_opinion/start and returns a task_id
instantly. When analysis completes (5-10 min), the pod POSTs the result
back to our webhook endpoint. A polling fallback task ensures results
are captured even if the webhook fails.
"""

import json
import logging
import time
from datetime import datetime, timezone

import redis
import requests as http_requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from celery_app import app
from ..config import settings
from ..models.sdss_task import SdssTask
from ..usage_tracker import tracker

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


def _publish_redis(task_id: str, message: dict):
    """Publish to Redis for WebSocket clients."""
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.publish(f"sdss:{task_id}", json.dumps(message, default=str))
        r.close()
    except Exception as e:
        logger.error(f"Redis publish failed for sdss:{task_id}: {e}")


def _log_audit_data(task_id: str, user_id: str, audit_data: dict):
    """Log GPU pod audit metadata to usage_log."""
    if not audit_data or not isinstance(audit_data, dict):
        return

    tracker.log(
        "sdss", "sdss_gpu", "gpu_processing",
        user_id=user_id,
        model=audit_data.get("model"),
        status="success",
        duration_ms=audit_data.get("total_duration_ms"),
        input_tokens=audit_data.get("input_tokens"),
        output_tokens=audit_data.get("output_tokens"),
        metadata={
            "task_id": task_id,
            "stage_timings": audit_data.get("stage_timings"),
            "serper_queries": audit_data.get("serper_queries"),
            "total_llm_calls": audit_data.get("total_llm_calls"),
        },
    )

    serper_count = audit_data.get("serper_queries")
    if isinstance(serper_count, int) and serper_count > 0:
        tracker.log("sdss", "sdss_serper", "gpu_serper_calls",
                    user_id=user_id, num_results=serper_count,
                    metadata={"task_id": task_id})
    elif isinstance(serper_count, list) and len(serper_count) > 0:
        tracker.log("sdss", "sdss_serper", "gpu_serper_calls",
                    user_id=user_id, num_results=len(serper_count),
                    request_summary="; ".join(str(q) for q in serper_count[:10])[:500],
                    metadata={"task_id": task_id})


# ── Main submission task ───────────────────────────────────────

@app.task(bind=True, name="sdss.run_analysis", soft_time_limit=60, time_limit=90)
def run_analysis(self, task_id: str):
    """Submit case to GPU pod with callback URL. Completes in <2 seconds."""
    session = _get_sync_session()
    task = None
    start = time.time()
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

        # Build callback URL — gracefully handle missing BACKEND_PUBLIC_URL
        callback_url = None
        if settings.BACKEND_PUBLIC_URL:
            callback_url = f"{settings.BACKEND_PUBLIC_URL.rstrip('/')}/webhook/sdss/{task_id}"
        else:
            logger.warning(f"BACKEND_PUBLIC_URL not set — webhook disabled for task {task_id}, relying on polling")

        url = f"{base_url}/second_opinion/start"
        payload = {
            "case_text": task.case_text,
            "mode": task.mode,
        }
        if callback_url:
            payload["callback_url"] = callback_url
        if task.images:
            payload["images"] = task.images  # [{filename, content_type, data}]

        logger.info(f"SDSS task {task_id}: submitting to {url} (mode={task.mode}, callback={callback_url})")

        resp = http_requests.post(url, json=payload, headers=NGROK_HEADERS, timeout=SUBMIT_TIMEOUT)
        resp.raise_for_status()
        submit_data = resp.json()

        task.pod_task_id = submit_data.get("task_id")
        session.commit()

        # Audit: log successful dispatch
        tracker.log(
            "sdss", "sdss_gpu", "dispatch",
            user_id=str(task.user_id),
            request_summary=f"task={task_id} mode={task.mode}",
            status="success",
            duration_ms=int((time.time() - start) * 1000),
            input_chars=len(task.case_text),
            metadata={"pod_task_id": task.pod_task_id, "mode": task.mode, "has_callback": callback_url is not None},
        )

        # Schedule polling fallback — starts checking after 5 min
        if task.pod_task_id:
            poll_gpu_result.apply_async(
                args=[task_id, task.pod_task_id],
                countdown=300,
            )

        logger.info(f"SDSS task {task_id}: GPU pod accepted, pod_task_id={task.pod_task_id}")
        return {"status": "submitted", "task_id": task_id, "pod_task_id": task.pod_task_id}

    except (http_requests.exceptions.ConnectionError, http_requests.exceptions.Timeout) as e:
        logger.warning(f"SDSS task {task_id}: GPU pod unreachable — {e}")
        if task:
            task.status = "failed"
            task.error = "GPU pod is offline or unreachable. Please try again later."
            task.completed_at = datetime.now(timezone.utc)
            session.commit()
        tracker.log(
            "sdss", "sdss_gpu", "dispatch",
            user_id=str(task.user_id) if task else None,
            request_summary=f"task={task_id}",
            status="error",
            error_message=str(e)[:500],
            duration_ms=int((time.time() - start) * 1000),
            input_chars=len(task.case_text) if task else 0,
        )

    except http_requests.exceptions.HTTPError as e:
        logger.error(f"SDSS task {task_id}: HTTP error — {e}")
        if task:
            task.status = "failed"
            task.error = f"GPU pod returned error: {e.response.status_code} {e.response.text[:500]}"
            task.completed_at = datetime.now(timezone.utc)
            session.commit()
        tracker.log(
            "sdss", "sdss_gpu", "dispatch",
            user_id=str(task.user_id) if task else None,
            request_summary=f"task={task_id}",
            status="error",
            error_message=str(e)[:500],
            duration_ms=int((time.time() - start) * 1000),
            input_chars=len(task.case_text) if task else 0,
        )

    except Exception as e:
        logger.error(f"SDSS task {task_id}: unexpected error — {e}", exc_info=True)
        if task:
            task.status = "failed"
            task.error = str(e)[:1000]
            task.completed_at = datetime.now(timezone.utc)
            session.commit()
        tracker.log(
            "sdss", "sdss_gpu", "dispatch",
            user_id=str(task.user_id) if task else None,
            request_summary=f"task={task_id}",
            status="error",
            error_message=str(e)[:500],
            duration_ms=int((time.time() - start) * 1000),
        )

    finally:
        session.close()


# ── Polling fallback task ──────────────────────────────────────

@app.task(bind=True, name="sdss.poll_gpu_result", max_retries=60, default_retry_delay=30)
def poll_gpu_result(self, task_id: str, pod_task_id: str):
    """Poll GPU pod for result if webhook hasn't arrived.

    Starts 5 min after dispatch (countdown=300). Checks every 30s.
    Max 60 retries = 30 minutes of polling, then gives up.
    """
    session = _get_sync_session()
    try:
        task = session.execute(
            select(SdssTask).where(SdssTask.id == task_id)
        ).scalar_one_or_none()

        if not task:
            return {"status": "task_not_found"}

        # If webhook already delivered the result, exit
        if task.status in ("complete", "failed"):
            return {"status": "already_resolved", "task_status": task.status}

        base_url = settings.SDSS_BASE_URL.rstrip("/")
        resp = http_requests.get(
            f"{base_url}/task/{pod_task_id}",
            headers=NGROK_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        pod_status = data.get("status")

        if pod_status == "complete":
            result = data.get("result")
            if result is None:
                # Flat shape: clinical fields at top level alongside "status"
                result = {k: v for k, v in data.items() if k not in ("status", "error", "task_id")}
            audit_data = None
            if isinstance(result, dict):
                audit_data = result.pop("_audit", None)

            task.status = "complete"
            task.result = result
            task.completed_at = datetime.now(timezone.utc)
            session.commit()

            _publish_redis(task_id, {"type": "complete", "task_id": task_id, "result": result})

            tracker.log(
                "sdss", "sdss_gateway", "poll_result_received",
                user_id=str(task.user_id),
                request_summary=f"task={task_id}",
                status="success",
                metadata={"task_id": task_id, "source": "poll", "retry_num": self.request.retries},
            )
            if audit_data:
                _log_audit_data(task_id, str(task.user_id), audit_data)

            logger.info(f"SDSS task {task_id}: result retrieved via polling (retry {self.request.retries})")
            return {"status": "complete"}

        elif pod_status == "failed":
            task.status = "failed"
            task.error = data.get("error", "GPU pod analysis failed")
            task.completed_at = datetime.now(timezone.utc)
            session.commit()

            _publish_redis(task_id, {"type": "error", "task_id": task_id, "error": task.error})

            tracker.log(
                "sdss", "sdss_gateway", "poll_result_received",
                user_id=str(task.user_id),
                request_summary=f"task={task_id}",
                status="error",
                error_message=task.error,
                metadata={"task_id": task_id, "source": "poll"},
            )

            logger.info(f"SDSS task {task_id}: GPU pod reported failure via polling")
            return {"status": "failed"}

        else:
            # Still processing — retry
            raise self.retry()

    except (http_requests.exceptions.ConnectionError, http_requests.exceptions.Timeout) as e:
        logger.warning(f"SDSS poll {task_id}: GPU pod unreachable — {e}")
        raise self.retry(exc=e)

    except http_requests.exceptions.HTTPError as e:
        logger.warning(f"SDSS poll {task_id}: HTTP error — {e}")
        raise self.retry(exc=e)

    finally:
        session.close()
