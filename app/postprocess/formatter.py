"""MedGemma wall-of-text to markdown formatter — ported from v5 lines 291-320."""

import re


def format_medgemma(text: str) -> str:
    """Convert MedGemma's wall-of-text into readable markdown.

    MedGemma outputs numbered sections like:
      1. CASE SUMMARY:...2. EVALUATION...3. ALTERNATIVE...
    all as one continuous paragraph. This splits them into proper sections.
    """
    # Add line breaks before numbered section headers
    text = re.sub(
        r"(\d+)\.\s*(CASE SUMMARY|EVALUATION|ALTERNATIVE|THE ALBUMIN|"
        r"RECOMMENDED|PATIENT COMMUNICATION|OVERALL CLINICAL)",
        r"\n\n### \1. \2",
        text,
        flags=re.IGNORECASE,
    )

    # Add line breaks before sub-items like "a) Autoimmune" or "b) Hemochromatosis"
    text = re.sub(r"([a-f])\)\s+", r"\n- **\1)** ", text)

    # Add line breaks before evidence markers
    text = re.sub(
        r"\s*-\s*(EVIDENCE\s+(?:SUPPORTING|AGAINST)|MY ASSESSMENT|"
        r"SUPPORTING|AGAINST|TESTS TO DO|SHOULD BIOPSY|"
        r"YOUR OVERALL)",
        r"\n\n**\1**",
        text,
        flags=re.IGNORECASE,
    )

    # Clean up excessive whitespace
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    return text.strip()
