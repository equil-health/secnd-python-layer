"""MedGemma output deduplication — ported from v5 lines 208-288.

MedGemma 4B does chain-of-thought OUT LOUD, producing:
  [Complete analysis sections 1-6]
  ---
  **Self-Correction/Refinement during Analysis:**
  [Meta-commentary about its own reasoning]
  ---
  **Final Output:**
  **SECOND OPINION ANALYSIS**
  [Entire sections 1-6 REPEATED]

Strategy: Find the FIRST complete analysis and cut everything after it.
"""

import re

# All known MedGemma repetition patterns (accumulated v1-v5)
CUT_MARKERS = [
    "\n---\n**Self-Correction",
    "\n---**Self-Correction",
    "\n**Self-Correction",
    "\nThis analysis provides a structured approach",
    "\n---\n**Final Output",
    "\n**Final Output:**",
    "\n---\n**Final Check:",
    "\n---\n**Note:**",
    "\n---\n**Further Considerations",
    "\n---\n**Final Output Structure",
]

SECTION_HEADERS = [
    "1. CASE SUMMARY",
    "### 1. CASE SUMMARY",
    "**1. CASE SUMMARY",
]


def dedup_medgemma(text: str) -> str:
    """Remove MedGemma's chain-of-thought repetition.

    Pass 1: Cut at self-reflection / meta-commentary markers.
    Pass 2: Detect repeated section headers, keep first only.
    Pass 3: Clean trailing disclaimer and asterisk artifacts.
    """
    original_len = len(text)

    # Pass 1: Cut at self-reflection markers
    for marker in CUT_MARKERS:
        if marker in text:
            idx = text.index(marker)
            if idx > 500:  # only cut if substantial content before
                text = text[:idx].rstrip().rstrip("-").rstrip()
                break

    # Pass 2: Detect repeated section headers
    for marker in SECTION_HEADERS:
        positions = [m.start() for m in re.finditer(re.escape(marker), text)]
        if len(positions) >= 2:
            cut_point = positions[1]
            # Back up to clean break point
            preceding = text[max(0, cut_point - 200):cut_point]
            for sep in ["\n---\n", "\n\n---", "\n---"]:
                if sep in preceding:
                    cut_point = max(0, cut_point - 200) + preceding.rindex(sep)
                    break
            text = text[:cut_point].rstrip().rstrip("-").rstrip()
            break

    # Pass 3: Clean trailing artifacts
    text = re.sub(r"\n*\*\*Disclaimer\*?\*?[:\s].*$", "", text, flags=re.DOTALL)
    text = re.sub(r"\*\*\s*$", "", text).rstrip()

    return text
