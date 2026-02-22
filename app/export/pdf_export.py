"""HTML to PDF via xhtml2pdf (no system dependencies needed)."""

import io
from xhtml2pdf import pisa

REPORT_CSS = """
body {
    font-family: Helvetica, Arial, sans-serif;
    max-width: 700px;
    margin: 0 auto;
    padding: 2rem;
    line-height: 1.6;
    font-size: 11pt;
}
h1 { color: #1a365d; font-size: 18pt; }
h2 { color: #2d3748; font-size: 14pt; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.5rem; }
h3 { color: #4a5568; font-size: 12pt; }
blockquote { border-left: 4px solid #e53e3e; padding: 0.5rem 1rem; background: #fff5f5; margin: 1rem 0; }
"""


def html_to_pdf(html_content: str) -> bytes:
    """Convert HTML report to PDF bytes."""
    full_html = f"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>{REPORT_CSS}</style>
</head><body>{html_content}</body></html>"""

    buffer = io.BytesIO()
    pisa.CreatePDF(io.StringIO(full_html), dest=buffer)
    return buffer.getvalue()
