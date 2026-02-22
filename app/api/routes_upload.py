"""Upload endpoint — submit case text + file attachments."""

import os
import uuid
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, UploadFile, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..db.database import get_db
from ..models.case import Case, CaseAttachment
from ..models.report import PipelineRun
from ..models.schemas import CaseResponse
from ..pipeline.file_processor import extract_text_from_file

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cases", tags=["cases"])

ALLOWED_CONTENT_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
    "image/jpeg",
    "image/png",
}

MAX_BYTES = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024


@router.post("/submit-with-files", status_code=201, response_model=CaseResponse)
async def submit_with_files(
    case_text: str = Form(...),
    files: list[UploadFile] = File(default=[]),
    db: AsyncSession = Depends(get_db),
):
    """Submit a case with optional file attachments.

    Extracts text from uploaded files and appends to raw_case_text
    so the existing pipeline works unchanged.
    """
    # Validate files before processing
    for f in files:
        if f.content_type not in ALLOWED_CONTENT_TYPES:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {f.content_type}. Allowed: PDF, DOCX, JPG, PNG",
            )

    # Create case
    case = Case(
        presenting_complaint=case_text[:200],
        raw_case_text=case_text,
        status="processing",
    )
    db.add(case)
    await db.flush()  # get case.id

    # Process files
    extracted_texts = []
    upload_dir = Path(settings.UPLOAD_DIR) / str(case.id)
    upload_dir.mkdir(parents=True, exist_ok=True)

    for f in files:
        # Read file content
        content = await f.read()

        if len(content) > MAX_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"File {f.filename} exceeds {settings.MAX_UPLOAD_SIZE_MB}MB limit",
            )

        # Save to disk
        ext = Path(f.filename).suffix if f.filename else ""
        stored_name = f"{uuid.uuid4()}{ext}"
        stored_path = upload_dir / stored_name

        with open(stored_path, "wb") as fh:
            fh.write(content)

        # Extract text
        extracted = extract_text_from_file(str(stored_path), f.content_type)
        if extracted:
            extracted_texts.append(f"--- Content from {f.filename} ---\n{extracted}")

        # Create attachment record
        attachment = CaseAttachment(
            case_id=case.id,
            original_filename=f.filename or "unknown",
            stored_path=str(stored_path),
            content_type=f.content_type,
            file_size=len(content),
            extracted_text=extracted,
        )
        db.add(attachment)

    # Append extracted text to case
    if extracted_texts:
        case.raw_case_text = case_text + "\n\n" + "\n\n".join(extracted_texts)

    # Create pipeline run
    pipeline_run = PipelineRun(
        case_id=case.id,
        status="queued",
        total_steps=10,
        steps=[
            {"step": 1, "label": "Case accepted", "status": "done"},
        ],
    )
    db.add(pipeline_run)
    await db.commit()
    await db.refresh(case)

    # Dispatch pipeline
    from ..pipeline.tasks import dispatch_pipeline
    dispatch_pipeline(str(case.id))

    return case
