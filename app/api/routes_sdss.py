"""SDSS routes — async second opinion via GPU pod with webhook callback."""

import asyncio
import base64
import json
import logging
import os
import uuid as uuid_mod
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import redis
import requests as http_requests
from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.sdss_task import SdssTask
from ..models.schemas import SdssSubmitRequest, SdssSubmitResponse, SdssTaskResponse
from ..models.user import User
from ..usage_tracker import tracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sdss", tags=["sdss"])

# Also mount webhook under /webhook (no /api prefix) for cleaner callback URLs
webhook_router = APIRouter(tags=["sdss-webhook"])


# ── Helpers ────────────────────────────────────────────────────

def _publish_redis(task_id, message: dict):
    """Publish a message to Redis for WebSocket clients."""
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

    # Main GPU processing entry
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
            "kg_triplets_checked": audit_data.get("kg_triplets_checked"),
            "hallucinations_detected": audit_data.get("hallucinations_detected"),
        },
    )

    # Separate serper usage entry (for cost tracking)
    serper_count = audit_data.get("serper_queries")
    if isinstance(serper_count, int) and serper_count > 0:
        tracker.log(
            "sdss", "sdss_serper", "gpu_serper_calls",
            user_id=user_id,
            num_results=serper_count,
            metadata={"task_id": task_id},
        )
    elif isinstance(serper_count, list) and len(serper_count) > 0:
        # serper_queries might be a list of query strings
        tracker.log(
            "sdss", "sdss_serper", "gpu_serper_calls",
            user_id=user_id,
            num_results=len(serper_count),
            request_summary="; ".join(str(q) for q in serper_count[:10])[:500],
            metadata={"task_id": task_id},
        )


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

    # Audit: log submission
    tracker.log(
        "sdss", "sdss_gateway", "submit",
        user_id=str(user.id),
        request_summary=body.case_text[:500],
        status="success",
        input_chars=len(body.case_text),
        metadata={"mode": body.mode, "india_context": body.india_context, "task_id": str(task.id)},
    )

    return SdssSubmitResponse(task_id=task.id)


# ── Submit with files endpoint ─────────────────────────────────

ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
    "image/jpeg",
    "image/png",
}


@router.post("/submit-with-files", status_code=201, response_model=SdssSubmitResponse)
async def sdss_submit_with_files(
    case_text: str = Form(""),
    mode: str = Form("standard"),
    india_context: bool = Form(False),
    files: List[UploadFile] = File(default=[]),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Submit a case with optional file attachments for SDSS analysis.

    - Images (JPG, PNG) are base64-encoded and sent to the GPU pod for
      multimodal analysis.
    - Documents (PDF, DOCX) are text-extracted and appended to case_text.
    """
    # Validate files
    for f in files:
        if f.content_type not in ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {f.filename} ({f.content_type}). Allowed: PDF, DOCX, JPG, PNG.",
            )

    # Split files: images → base64 for GPU pod, documents → text extraction on DO
    image_payloads = []   # sent to GPU pod as base64 in "images" array
    extracted_parts = []  # text-extracted and appended to case_text
    upload_dir = Path(settings.UPLOAD_DIR)
    saved_paths = []

    for f in files:
        content = await f.read()

        if f.content_type.startswith("image/"):
            # Encode image as base64 for multimodal GPU pod
            b64 = base64.b64encode(content).decode("ascii")
            image_payloads.append({
                "filename": f.filename,
                "content_type": f.content_type,
                "data": b64,
            })
        else:
            # Document — extract text on DO server, append to case_text
            try:
                task_dir = upload_dir / "sdss_temp"
                task_dir.mkdir(parents=True, exist_ok=True)
                ext = Path(f.filename).suffix
                saved_name = f"{uuid_mod.uuid4()}{ext}"
                saved_path = task_dir / saved_name
                saved_path.write_bytes(content)
                saved_paths.append(saved_path)

                from ..pipeline.file_processor import extract_text_from_file
                text = await asyncio.to_thread(extract_text_from_file, str(saved_path), f.content_type)
                if text and text.strip():
                    extracted_parts.append(f"--- Content from {f.filename} ---\n{text.strip()}")
            except Exception as e:
                logger.warning(f"Failed to extract text from {f.filename}: {e}")

    # Build combined case text (with extracted document text appended)
    combined_text = case_text.strip()
    if extracted_parts:
        combined_text = combined_text + "\n\n" + "\n\n".join(extracted_parts) if combined_text else "\n\n".join(extracted_parts)

    if not combined_text and not image_payloads:
        raise HTTPException(status_code=400, detail="Please provide case text or upload clinical images.")

    # Create task and dispatch
    task = SdssTask(
        user_id=user.id,
        case_text=combined_text or "",
        mode=mode,
        india_context=india_context,
        images=image_payloads if image_payloads else None,
        status="pending",
    )
    db.add(task)
    await db.flush()

    from ..sdss.tasks import run_analysis
    run_analysis.delay(str(task.id))

    await db.commit()

    # Clean up temp files
    for p in saved_paths:
        try:
            p.unlink(missing_ok=True)
        except Exception:
            pass

    # Audit
    tracker.log(
        "sdss", "sdss_gateway", "submit_with_files",
        user_id=str(user.id),
        request_summary=combined_text[:500] if combined_text else f"[{len(image_payloads)} images]",
        status="success",
        input_chars=len(combined_text),
        metadata={
            "mode": mode,
            "india_context": india_context,
            "task_id": str(task.id),
            "files_count": len(files),
            "images_count": len(image_payloads),
            "docs_count": len(extracted_parts),
            "file_names": [f.filename for f in files][:10],
            "extracted_chars": sum(len(p) for p in extracted_parts),
        },
    )

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


# ── List tasks endpoint (for history/archive) ─────────────────────

@router.get("/tasks", response_model=dict)
async def sdss_list_tasks(
    page: int = 1,
    per_page: int = 20,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all SDSS tasks for the current user (paginated, newest first)."""
    from sqlalchemy import func

    offset = (max(1, page) - 1) * per_page
    per_page = min(per_page, 100)

    # Count total
    count_q = select(func.count()).select_from(SdssTask).where(SdssTask.user_id == user.id)
    total = (await db.execute(count_q)).scalar() or 0

    # Fetch page
    q = (
        select(SdssTask)
        .where(SdssTask.user_id == user.id)
        .order_by(SdssTask.created_at.desc())
        .offset(offset)
        .limit(per_page)
    )
    rows = (await db.execute(q)).scalars().all()

    tasks = []
    now = datetime.now(timezone.utc)
    for t in rows:
        # Extract summary fields from result for list display
        result = t.result or {}
        elapsed = None
        if t.completed_at and t.created_at:
            elapsed = round((t.completed_at - t.created_at).total_seconds(), 1)
        elif t.created_at:
            elapsed = round((now - t.created_at).total_seconds(), 1)

        tasks.append({
            "task_id": str(t.id),
            "status": t.status,
            "mode": t.mode,
            "case_text_preview": (t.case_text or "")[:120],
            "top_diagnosis": result.get("top_diagnosis", ""),
            "primary_diagnosis": result.get("primary_diagnosis", ""),
            "has_critical_flags": result.get("has_critical_flags", False),
            "evidence_count": result.get("evidence_count", 0),
            "has_audit": t.audit_report is not None,
            "elapsed_seconds": elapsed,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "completed_at": t.completed_at.isoformat() if t.completed_at else None,
        })

    return {"tasks": tasks, "total": total, "page": page, "per_page": per_page}


# ── Single task with audit endpoint ───────────────────────────────

@router.get("/task/{task_id}/audit")
async def sdss_task_audit(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get the audit report for a specific SDSS task."""
    result = await db.execute(
        select(SdssTask).where(SdssTask.id == task_id)
    )
    task = result.scalar_one_or_none()
    if not task or task.user_id != user.id:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "has_audit": task.audit_report is not None,
        "audit_report": task.audit_report,
    }


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


@webhook_router.post("/webhook/sdss/{task_id}")
async def sdss_webhook(
    task_id: UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Webhook called by GPU pod when analysis completes or fails.

    Accepts any JSON body — the GPU pod may send the result in various
    shapes:
      1. {"status": "complete", "result": {...}}          (nested)
      2. {"status": "complete", "p2_differential": [...]} (flat — clinical fields at top level)
      3. {"task_id": "...", "status": "complete", ...}    (with or without task_id)
    """
    # Validate shared secret if configured
    secret = settings.SDSS_WEBHOOK_SECRET
    if secret:
        header_secret = request.headers.get("X-SECND-Secret", "")
        if header_secret != secret:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    result_row = await db.execute(
        select(SdssTask).where(SdssTask.id == task_id)
    )
    task = result_row.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    now = datetime.now(timezone.utc)
    pod_status = body.get("status", "complete")
    pod_error = body.get("error")

    # Extract clinical result — handle both nested and flat shapes
    clinical_result = body.get("result")
    if clinical_result is None:
        # Flat shape: clinical fields are at the top level alongside "status"
        # Copy body and remove meta keys to get pure clinical data
        clinical_result = {k: v for k, v in body.items() if k not in ("status", "error", "task_id")}

    # Extract _audit data before storing — save full audit to dedicated column
    audit_data = None
    if isinstance(clinical_result, dict):
        audit_data = clinical_result.pop("_audit", None)

    if pod_status == "complete":
        task.status = "complete"
        task.result = clinical_result
        task.audit_report = audit_data  # Full pipeline audit trail
        task.completed_at = now
        ws_message = {"type": "complete", "task_id": str(task_id), "result": clinical_result}
    elif pod_status == "failed":
        task.status = "failed"
        task.error = pod_error or "GPU pod analysis failed"
        task.completed_at = now
        ws_message = {"type": "error", "task_id": str(task_id), "error": task.error}
    else:
        # Unknown status — treat as complete if there's any data
        if clinical_result and any(k in clinical_result for k in ("p1_differential", "p2_differential", "synthesis")):
            task.status = "complete"
            task.result = clinical_result
            task.completed_at = now
            ws_message = {"type": "complete", "task_id": str(task_id), "result": clinical_result}
            logger.warning(f"SDSS webhook {task_id}: unknown status '{pod_status}', treating as complete (has clinical data)")
        else:
            raise HTTPException(status_code=400, detail=f"Unknown status: {pod_status}")

    await db.commit()

    # Publish to Redis so WebSocket clients get notified
    _publish_redis(task_id, ws_message)

    # Audit: log webhook receipt
    tracker.log(
        "sdss", "sdss_gateway", "webhook_received",
        user_id=str(task.user_id),
        request_summary=f"task={task_id} status={pod_status}",
        status="success" if task.status == "complete" else "error",
        error_message=pod_error if task.status == "failed" else None,
        metadata={"task_id": str(task_id), "has_audit": audit_data is not None, "body_keys": list(body.keys())[:20]},
    )

    # Audit: log GPU resource usage from _audit data
    if audit_data:
        _log_audit_data(str(task_id), str(task.user_id), audit_data)

    logger.info(f"SDSS webhook received for task {task_id}: status={pod_status}, result_keys={list((clinical_result or {}).keys())[:10]}")
    return {"status": "ok"}
