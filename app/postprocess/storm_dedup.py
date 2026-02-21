"""Remove redundant STORM sections — ported from v5 lines 812-831."""

import re


def remove_redundant_sections(article: str) -> str:
    """Remove repetitive Introduction/Overview and short filler sections from STORM output.

    STORM generates semi-independent sections that often restate the same points.
    """
    # Remove "# Introduction / ## Overview of AIH and HCC" if it just restates summary
    intro_pattern = (
        r"\n# Introduction\n+## Overview of AIH and HCC\n+"
        r"### Autoimmune Hepatitis \(AIH\)\n.*?(?=\n# (?!Introduction))"
    )
    if re.search(intro_pattern, article, re.DOTALL):
        article = re.sub(intro_pattern, "\n", article, flags=re.DOTALL)

    # Remove "# Case Studies" section if it's just restating known info (short = filler)
    case_study_pattern = r"\n# Case Studies\n.*?(?=\n# |\n---|\Z)"
    case_match = re.search(case_study_pattern, article, re.DOTALL)
    if case_match and len(case_match.group(0)) < 800:
        article = re.sub(case_study_pattern, "\n", article, flags=re.DOTALL)

    # Clean up multiple consecutive newlines
    article = re.sub(r"\n{4,}", "\n\n\n", article)

    return article
