"""Audio transcript structurer — converts MedASR transcript to clinical JSON.

Uses Gemini Flash to extract structured clinical fields from a physician
dictation transcript.
"""

import json
import re

from .gemini import call_gemini


def structure_transcript(transcript: str) -> dict:
    """Call Gemini to extract structured clinical fields from a transcript.

    Returns a dict matching CaseSubmitStructured fields plus a
    `transcript_summary` field for UI display.
    """
    prompt = f"""You are a clinical data extraction system. Given a physician dictation transcript,
extract structured clinical fields. Return JSON only — no markdown fences.

{{
    "patient_age": number or null,
    "patient_sex": "male" or "female" or "other" or null,
    "patient_ethnicity": string or null,
    "presenting_complaint": string (the main reason for visit),
    "medical_history": string or null,
    "medications": string or null,
    "physical_exam": string or null,
    "lab_results": [{{"name": "...", "value": "...", "unit": "...", "flag": "H/L/N"}}] or null,
    "imaging_reports": string or null,
    "referring_diagnosis": string or null,
    "specific_question": string or null,
    "transcript_summary": string (2-3 sentence formatted summary of the case for display)
}}

PHYSICIAN DICTATION TRANSCRIPT:
{transcript}"""

    raw = call_gemini(prompt, max_tokens=2048, temperature=0.1)

    # Strip markdown fences if present
    raw = re.sub(r"^```json\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)

    try:
        structured = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: use transcript as presenting complaint
        structured = {
            "presenting_complaint": transcript[:2000],
            "transcript_summary": transcript[:300],
        }

    # Ensure required field exists
    if not structured.get("presenting_complaint"):
        structured["presenting_complaint"] = transcript[:2000]
    if not structured.get("transcript_summary"):
        structured["transcript_summary"] = transcript[:300]

    return structured
