"""Audio submission route — MedASR transcription pipeline.

POST /api/cases/audio accepts an audio file, transcribes it via MedASR,
structures the transcript via Gemini, creates a Case, then dispatches
the existing diagnosis pipeline.

Steps 1-3 (audio-specific) run synchronously so the transcript preview
is available immediately via WebSocket. Steps 4-11 run async via Celery.
"""

import json
import os
import uuid as _uuid

import redis
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..db.database import get_db
from ..models.case import Case, CaseAttachment
from ..models.report import PipelineRun

router = APIRouter(prefix="/api/cases", tags=["audio"])

ALLOWED_AUDIO_TYPES = {"wav", "mp3", "m4a", "webm", "flac", "ogg"}

_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(settings.REDIS_URL)
    return _redis_client


def _broadcast(case_id: str, message: dict):
    """Publish pipeline status update via Redis pub/sub."""
    message["case_id"] = case_id
    _get_redis().publish(f"pipeline:{case_id}", json.dumps(message, default=str))


@router.post("/audio", status_code=201)
async def submit_audio(
    audio: UploadFile = File(...),
    question: str = Form(None),
    mode: str = Form("standard"),
    db: AsyncSession = Depends(get_db),
):
    """Submit an audio file for MedASR transcription + diagnosis pipeline.

    Flow:
    1. Validate & save audio file
    2. MedASR transcription (sync, ~2-5s)
    3. Gemini structuring (sync, ~3-5s)
    4-11. Existing diagnosis pipeline (async via Celery)
    """
    # --- Validate file type ---
    filename = audio.filename or "audio.wav"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ALLOWED_AUDIO_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported audio format '{ext}'. Allowed: {', '.join(sorted(ALLOWED_AUDIO_TYPES))}",
        )

    audio_bytes = await audio.read()
    if len(audio_bytes) > settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Audio file too large")

    # --- Create Case (status=processing) ---
    case_id = _uuid.uuid4()
    case = Case(
        id=case_id,
        presenting_complaint="[Audio case — transcription pending]",
        status="processing",
        pipeline_type="diagnosis",
        diagnosis_mode=mode if mode in ("standard", "zebra") else "standard",
    )
    db.add(case)
    await db.flush()

    case_id_str = str(case_id)

    # --- Create PipelineRun with 11 steps (3 audio + 8 existing) ---
    pipeline_run = PipelineRun(
        case_id=case_id,
        status="running",
        total_steps=11,
        steps=[],
    )
    db.add(pipeline_run)

    # --- Save audio to disk ---
    upload_dir = os.path.join(settings.UPLOAD_DIR, case_id_str)
    os.makedirs(upload_dir, exist_ok=True)
    stored_filename = f"audio.{ext}"
    stored_path = os.path.join(upload_dir, stored_filename)
    with open(stored_path, "wb") as f:
        f.write(audio_bytes)

    # --- CaseAttachment record ---
    attachment = CaseAttachment(
        case_id=case_id,
        original_filename=filename,
        stored_path=stored_path,
        content_type=audio.content_type or f"audio/{ext}",
        file_size=len(audio_bytes),
    )
    db.add(attachment)
    await db.commit()

    # --- Step 1: Audio received ---
    _broadcast(case_id_str, {
        "type": "step_update", "step": 1,
        "label": "Audio received", "status": "done",
    })

    # --- Step 2: MedASR transcription (synchronous) ---
    _broadcast(case_id_str, {
        "type": "step_update", "step": 2,
        "label": "MedASR transcribing...", "status": "running",
    })

    try:
        from ..pipeline.medasr import transcribe_bytes as _transcribe
        transcript = _transcribe(audio_bytes, ext)
    except Exception as e:
        _broadcast(case_id_str, {
            "type": "error",
            "error": f"MedASR transcription failed: {str(e)[:200]}",
        })
        raise HTTPException(status_code=500, detail=f"Transcription failed: {e}")

    transcript_preview = transcript[:200]
    _broadcast(case_id_str, {
        "type": "step_update", "step": 2,
        "label": "MedASR transcribing...", "status": "done",
        "preview": transcript_preview,
    })

    # --- Step 3: Gemini structuring (synchronous) ---
    _broadcast(case_id_str, {
        "type": "step_update", "step": 3,
        "label": "Structuring transcript...", "status": "running",
    })

    try:
        from ..pipeline.audio_structurer import structure_transcript
        structured = structure_transcript(transcript)
    except Exception as e:
        _broadcast(case_id_str, {
            "type": "error",
            "error": f"Transcript structuring failed: {str(e)[:200]}",
        })
        raise HTTPException(status_code=500, detail=f"Structuring failed: {e}")

    summary_preview = structured.get("transcript_summary", "")[:200]
    _broadcast(case_id_str, {
        "type": "step_update", "step": 3,
        "label": "Structuring transcript...", "status": "done",
        "preview": summary_preview,
    })

    # --- Update Case with structured fields ---
    case.patient_age = structured.get("patient_age")
    case.patient_sex = structured.get("patient_sex")
    case.patient_ethnicity = structured.get("patient_ethnicity")
    case.presenting_complaint = structured.get("presenting_complaint", transcript[:2000])
    case.medical_history = structured.get("medical_history")
    case.medications = structured.get("medications")
    case.physical_exam = structured.get("physical_exam")
    case.imaging_reports = structured.get("imaging_reports")
    case.referring_diagnosis = structured.get("referring_diagnosis")
    case.specific_question = question or structured.get("specific_question")
    case.raw_case_text = transcript

    # Handle lab_results
    lab_results = structured.get("lab_results")
    if lab_results and isinstance(lab_results, list):
        case.lab_results = lab_results

    await db.commit()

    # --- Dispatch existing pipeline (steps 4-11 = existing steps 2-9) ---
    from ..pipeline.tasks import dispatch_pipeline
    dispatch_pipeline(case_id_str)

    return {
        "id": case_id_str,
        "status": "processing",
        "transcript_preview": transcript_preview,
        "structured_summary": summary_preview,
    }
