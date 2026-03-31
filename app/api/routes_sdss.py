"""SDSS routes — async second opinion via GPU pod."""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

import requests as http_requests
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.sdss_task import SdssTask
from ..models.schemas import SdssSubmitRequest, SdssSubmitResponse, SdssTaskResponse
from ..models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sdss", tags=["sdss"])


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
