"""Extract text from uploaded files (PDF, DOCX, images)."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_text_from_file(path: str, content_type: str) -> str:
    """Route to the appropriate extractor based on content type."""
    path = Path(path)
    if not path.exists():
        logger.warning("File not found: %s", path)
        return ""

    try:
        if content_type == "application/pdf":
            return _extract_pdf(path)
        elif content_type in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",
        ):
            return _extract_docx(path)
        elif content_type.startswith("image/"):
            return _extract_image(path)
        else:
            logger.warning("Unsupported content type: %s", content_type)
            return ""
    except Exception:
        logger.exception("Failed to extract text from %s", path)
        return ""


def _extract_pdf(path: Path) -> str:
    from PyPDF2 import PdfReader

    reader = PdfReader(str(path))
    pages = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pages.append(text)
    return "\n\n".join(pages)


def _extract_docx(path: Path) -> str:
    from docx import Document

    doc = Document(str(path))
    return "\n\n".join(p.text for p in doc.paragraphs if p.text.strip())


def _extract_image(path: Path) -> str:
    from PIL import Image
    import pytesseract

    img = Image.open(path)
    return pytesseract.image_to_string(img)
