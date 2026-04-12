"""Chat routes — Open WebUI + MedGemma powered clinical chat with SDSS report context.

Includes inline SDSS analysis: clinicians can trigger a full second-opinion
pipeline from within the chat. The pipeline runs asynchronously via Celery;
the frontend polls for status and renders progress inline as a system message.

Voice transcription proxies to MedASR on the GPU pod.
"""

import asyncio
import base64
import json
import logging
from pathlib import Path
from typing import List, Optional
from uuid import UUID
import uuid as uuid_mod

import requests as http_requests
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..auth.security import get_current_user
from ..config import settings
from ..db.database import get_db
from ..models.sdss_task import SdssTask
from ..models.user import User
from ..usage_tracker import tracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/chat", tags=["chat"])

SYSTEM_PROMPT = """\
You are SECND Chat, a clinical AI assistant embedded in the SECND medical \
decision-support platform. You help healthcare professionals interpret and \
explore SDSS second-opinion reports.

Rules:
- Be evidence-based; cite guidelines, scoring systems, or landmark trials \
when relevant.
- Use structured formatting: headers, bullet points, numbered lists.
- When the clinician asks about a diagnosis, cover: key criteria, differential, \
red flags, recommended workup, and management considerations.
- If you are uncertain, say so explicitly rather than guessing.
- Always end with a brief reminder that your output is for informational purposes \
only and does not replace clinical judgment.
- Be concise — clinicians are time-constrained.
"""


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    stream: bool = True
    task_id: Optional[str] = None  # SDSS task ID to load report context


def _build_report_context(result: dict, case_text: str) -> str:
    """Build a context string from an SDSS report result."""
    parts = [
        "=== SDSS REPORT CONTEXT ===",
        f"\n## Original Case\n{case_text}",
    ]

    if result.get("top_diagnosis"):
        parts.append(f"\n## Top Diagnosis\n{result['top_diagnosis']}")

    if result.get("differential"):
        parts.append("\n## Differential Diagnosis")
        for dx in result["differential"]:
            if isinstance(dx, dict):
                name = dx.get("diagnosis") or dx.get("name", "")
                score = dx.get("confidence") or dx.get("score", "")
                parts.append(f"- {name} (confidence: {score})")
            else:
                parts.append(f"- {dx}")

    if result.get("synthesis"):
        parts.append(f"\n## Full Synthesis\n{result['synthesis']}")

    if result.get("safety_flags"):
        parts.append("\n## Safety Flags")
        for flag in result["safety_flags"]:
            parts.append(f"- {flag}")

    if result.get("evidence_refs"):
        parts.append("\n## Evidence References")
        for ref in result["evidence_refs"][:15]:
            if isinstance(ref, dict):
                title = ref.get("title", "")
                source = ref.get("source", "")
                parts.append(f"- {title} ({source})")
            else:
                parts.append(f"- {ref}")

    parts.append("\n=== END REPORT CONTEXT ===")
    return "\n".join(parts)


def _build_openai_messages(system: str, context: Optional[str], messages: list[ChatMessage]) -> list[dict]:
    """Build OpenAI-compatible messages array with system prompt + report context."""
    api_messages = [{"role": "system", "content": system}]

    if context:
        api_messages.append({
            "role": "system",
            "content": f"{context}\n\nUse the report above as context when answering the clinician's questions.",
        })

    for msg in messages:
        api_messages.append({"role": msg.role, "content": msg.content})

    return api_messages


@router.post("/completions")
async def chat_completions(
    body: ChatRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Stream chat completions via Open WebUI / MedGemma with SDSS report context."""
    base_url = settings.OPENWEBUI_BASE_URL
    api_key = settings.OPENWEBUI_API_KEY
    model = settings.CHAT_MODEL

    if not base_url:
        raise HTTPException(status_code=503, detail="Chat service not configured (missing OPENWEBUI_BASE_URL)")

    # Load report context from DB if task_id provided
    report_context = None
    if body.task_id:
        try:
            task_uuid = UUID(body.task_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid task_id")

        stmt = select(SdssTask).where(
            SdssTask.id == task_uuid,
            SdssTask.user_id == user.id,
        )
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()

        if task and task.result:
            report_context = _build_report_context(task.result, task.case_text or "")
        elif task and task.status != "complete":
            raise HTTPException(status_code=400, detail="Report is not ready yet")

    api_messages = _build_openai_messages(SYSTEM_PROMPT, report_context, body.messages)

    payload = {
        "model": model,
        "messages": api_messages,
        "stream": body.stream,
        "max_tokens": 4096,
        "temperature": 0.4,
    }

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    owui_url = f"{base_url.rstrip('/')}/api/chat/completions"

    if not body.stream:
        # Non-streaming
        try:
            resp = http_requests.post(owui_url, json=payload, headers=headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            tracker.log("chat", "openwebui", "chat_completion", model=model, status="success",
                        input_chars=sum(len(m.content) for m in body.messages), output_chars=len(text))
            return data
        except http_requests.exceptions.ConnectionError:
            raise HTTPException(status_code=503, detail="Chat service unavailable (Open WebUI offline)")
        except Exception as e:
            logger.exception("Chat non-stream error")
            raise HTTPException(status_code=502, detail=f"Chat service error: {str(e)}")

    # Streaming — proxy SSE from Open WebUI (already OpenAI-compatible)
    def stream_openwebui():
        try:
            with http_requests.post(owui_url, json=payload, headers=headers,
                                     timeout=120, stream=True) as resp:
                if resp.status_code != 200:
                    error_text = resp.text[:500]
                    logger.error("Open WebUI stream error: %s %s", resp.status_code, error_text)
                    err = json.dumps({"choices": [{"delta": {"content": f"Error: Chat service returned {resp.status_code}"}}]})
                    yield f"data: {err}\n\n"
                    yield "data: [DONE]\n\n"
                    return

                total_text = ""
                for line in resp.iter_lines(decode_unicode=True):
                    if not line:
                        continue
                    # Pass through SSE lines directly — Open WebUI already emits OpenAI format
                    if line.startswith("data: "):
                        raw = line[6:]
                        if raw.strip() == "[DONE]":
                            yield "data: [DONE]\n\n"
                            break
                        try:
                            chunk = json.loads(raw)
                            delta = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                            if delta:
                                total_text += delta
                        except (json.JSONDecodeError, IndexError, KeyError):
                            pass
                        yield f"{line}\n\n"

                tracker.log("chat", "openwebui", "chat_stream", model=model, status="success",
                            input_chars=sum(len(m.content) for m in body.messages), output_chars=len(total_text))

        except http_requests.ConnectionError:
            err = json.dumps({"choices": [{"delta": {"content": "Error: Chat service unavailable (Open WebUI offline)"}}]})
            yield f"data: {err}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            logger.exception("Chat stream error")
            err = json.dumps({"choices": [{"delta": {"content": f"Error: {str(e)}"}}]})
            yield f"data: {err}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        stream_openwebui(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ── Inline SDSS Analysis from Chat ──────────────────────────────

ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
    "image/jpeg",
    "image/png",
}


@router.post("/analyze", status_code=201)
async def chat_analyze(
    case_text: str = Form(""),
    mode: str = Form("standard"),
    files: List[UploadFile] = File(default=[]),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Trigger an SDSS analysis from within the chat, with optional files.

    Accepts multipart form with case_text, mode, and optional file attachments
    (PDF, DOCX, JPG, PNG). Images are base64-encoded for GPU pod; documents
    are text-extracted and appended to case_text.
    """
    # Validate files
    for f in files:
        if f.content_type not in ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {f.filename} ({f.content_type}). Allowed: PDF, DOCX, JPG, PNG.",
            )

    # Process files: images → base64, documents → text extraction
    image_payloads = []
    extracted_parts = []
    upload_dir = Path(settings.UPLOAD_DIR)
    saved_paths = []

    for f in files:
        content = await f.read()

        if f.content_type.startswith("image/"):
            b64 = base64.b64encode(content).decode("ascii")
            image_payloads.append({
                "filename": f.filename,
                "content_type": f.content_type,
                "data": b64,
            })
        else:
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

    # Build combined case text
    combined_text = case_text.strip()
    if extracted_parts:
        combined_text = combined_text + "\n\n" + "\n\n".join(extracted_parts) if combined_text else "\n\n".join(extracted_parts)

    if not combined_text and not image_payloads:
        raise HTTPException(status_code=400, detail="Please provide case text or upload clinical files.")

    # Create task and dispatch
    task = SdssTask(
        user_id=user.id,
        case_text=combined_text or "",
        mode=mode,
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

    tracker.log(
        "chat", "sdss_gateway", "chat_analyze",
        user_id=str(user.id),
        request_summary=(combined_text or "")[:500],
        status="success",
        input_chars=len(combined_text or ""),
        metadata={
            "mode": mode,
            "task_id": str(task.id),
            "files_count": len(files),
            "images_count": len(image_payloads),
            "docs_count": len(extracted_parts),
        },
    )

    return {"task_id": str(task.id), "status": "pending"}


# ── Voice Transcription (MedASR via GPU pod) ─────────────────

ALLOWED_AUDIO_TYPES = {"wav", "mp3", "m4a", "webm", "flac", "ogg"}

NGROK_HEADERS = {"ngrok-skip-browser-warning": "true"}


@router.post("/transcribe")
async def chat_transcribe(
    audio: UploadFile = File(...),
    user: User = Depends(get_current_user),
):
    """Transcribe audio via MedASR on the GPU pod.

    Accepts audio (wav, mp3, m4a, webm, flac, ogg), proxies to the GPU pod's
    /transcribe endpoint which runs MedASR, and returns the text for the
    clinician to review/edit before sending.
    """
    base_url = settings.SDSS_BASE_URL
    if not base_url:
        raise HTTPException(status_code=503, detail="GPU pod not configured (SDSS_BASE_URL)")

    filename = audio.filename or "recording.webm"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "webm"
    if ext not in ALLOWED_AUDIO_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported audio format '{ext}'. Allowed: {', '.join(sorted(ALLOWED_AUDIO_TYPES))}",
        )

    audio_bytes = await audio.read()
    if len(audio_bytes) > 25 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Audio file too large (max 25 MB)")

    import time
    start = time.time()

    def _proxy_transcribe():
        url = f"{base_url.rstrip('/')}/transcribe"
        files = {"audio": (filename, audio_bytes, audio.content_type or "audio/webm")}
        resp = http_requests.post(url, files=files, headers=NGROK_HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()

    try:
        result = await asyncio.to_thread(_proxy_transcribe)
    except http_requests.exceptions.ConnectionError:
        tracker.log("chat", "medasr", "transcribe", user_id=str(user.id),
                    status="error", error_message="GPU pod unreachable")
        raise HTTPException(status_code=503, detail="GPU pod is offline. Voice transcription unavailable.")
    except http_requests.exceptions.HTTPError as e:
        logger.error("MedASR proxy error: %s", e)
        tracker.log("chat", "medasr", "transcribe", user_id=str(user.id),
                    status="error", error_message=str(e)[:500])
        raise HTTPException(status_code=502, detail=f"Transcription failed: {e.response.text[:200]}")
    except Exception as e:
        logger.exception("MedASR transcription failed")
        tracker.log("chat", "medasr", "transcribe", user_id=str(user.id),
                    status="error", error_message=str(e)[:500])
        raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)[:200]}")

    duration_ms = int((time.time() - start) * 1000)
    text = result.get("text", "")

    tracker.log("chat", "medasr", "transcribe", user_id=str(user.id),
                status="success", duration_ms=duration_ms,
                input_chars=len(audio_bytes), output_chars=len(text),
                metadata={"gpu_duration_ms": result.get("duration_ms")})

    return {"text": text, "duration_ms": duration_ms}
