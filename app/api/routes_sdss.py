"""SDSS routes — async second opinion via GPU pod with webhook callback."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import UUID

import redis
import requests as http_requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.sdss_task import SdssTask
from ..models.schemas import SdssSubmitRequest, SdssSubmitResponse, SdssTaskResponse
from ..models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sdss", tags=["sdss"])

# Also mount webhook under /webhook (no /api prefix) for cleaner callback URLs
webhook_router = APIRouter(tags=["sdss-webhook"])


# ── Submit endpoint ─────────────────────────────────────────────

@router.post("/submit", status_code=201, response_model=SdssSubmitResponse)
async def sdss_submit(
    body: SdssSubmitRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Submit a case for async SDSS analysis. Returns task_id immediately."""
    task = SdssTask(
        user_id=user.id,
        case_text=body.case_text,
        mode=body.mode,
        india_context=body.india_context,
        status="pending",
    )
    db.add(task)
    await db.flush()

    from ..sdss.tasks import run_analysis
    run_analysis.delay(str(task.id))

    await db.commit()
    return SdssSubmitResponse(task_id=task.id)


# ── Poll endpoint (fallback if WebSocket unavailable) ───────────

@router.get("/task/{task_id}", response_model=SdssTaskResponse)
async def sdss_task_status(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Poll for SDSS task status and result."""
    result = await db.execute(
        select(SdssTask).where(SdssTask.id == task_id)
    )
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.user_id != user.id:
        raise HTTPException(status_code=404, detail="Task not found")

    now = datetime.now(timezone.utc)
    if task.completed_at and task.created_at:
        elapsed = (task.completed_at - task.created_at).total_seconds()
    elif task.created_at:
        elapsed = (now - task.created_at).total_seconds()
    else:
        elapsed = None

    return SdssTaskResponse(
        task_id=task.id,
        status=task.status,
        result=task.result,
        error=task.error,
        elapsed_seconds=round(elapsed, 1) if elapsed is not None else None,
        created_at=task.created_at,
        completed_at=task.completed_at,
    )


# ── Health proxy ────────────────────────────────────────────────

@router.get("/health")
async def sdss_health():
    """Proxy health check to GPU pod."""
    base_url = settings.SDSS_BASE_URL
    if not base_url:
        return {"status": "offline", "detail": "SDSS_BASE_URL not configured"}

    def _check():
        try:
            resp = http_requests.get(
                f"{base_url.rstrip('/')}/health",
                headers={"ngrok-skip-browser-warning": "true"},
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    result = await asyncio.to_thread(_check)
    if result is None:
        return {"status": "offline"}
    return result


# ── Webhook receiver (called by GPU pod) ────────────────────────

class WebhookPayload(BaseModel):
    task_id: str
    status: str  # "complete" or "failed"
    result: Optional[dict] = None
    error: Optional[str] = None


@webhook_router.post("/webhook/sdss/{task_id}")
async def sdss_webhook(
    task_id: UUID,
    payload: WebhookPayload,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Webhook called by GPU pod when analysis completes or fails."""
    # Validate shared secret if configured
    secret = settings.SDSS_WEBHOOK_SECRET
    if secret:
        header_secret = request.headers.get("X-SECND-Secret", "")
        if header_secret != secret:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    result = await db.execute(
        select(SdssTask).where(SdssTask.id == task_id)
    )
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    now = datetime.now(timezone.utc)

    if payload.status == "complete":
        task.status = "complete"
        task.result = payload.result
        task.completed_at = now
        ws_message = {"type": "complete", "task_id": str(task_id), "result": payload.result}
    elif payload.status == "failed":
        task.status = "failed"
        task.error = payload.error or "GPU pod analysis failed"
        task.completed_at = now
        ws_message = {"type": "error", "task_id": str(task_id), "error": task.error}
    else:
        raise HTTPException(status_code=400, detail=f"Unknown status: {payload.status}")

    await db.commit()

    # Publish to Redis so WebSocket clients get notified
    try:
        r = redis.Redis.from_url(settings.REDIS_URL)
        r.publish(f"sdss:{task_id}", json.dumps(ws_message, default=str))
        r.close()
    except Exception as e:
        logger.error(f"Redis publish failed for sdss:{task_id}: {e}")

    logger.info(f"SDSS webhook received for task {task_id}: status={payload.status}")
    return {"status": "ok"}
