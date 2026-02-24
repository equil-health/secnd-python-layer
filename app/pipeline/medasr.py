"""MedASR medical speech recognition module.

Lazy-loads Google's MedASR model and transcribes audio files.
Supports chunking for files longer than 2 minutes.
"""

import os
import tempfile

import torch
import librosa
import numpy as np
from transformers import AutoModel, AutoProcessor

from ..config import settings

# Ensure HF_TOKEN from .env is available to transformers library
if settings.HF_TOKEN and not os.environ.get("HF_TOKEN"):
    os.environ["HF_TOKEN"] = settings.HF_TOKEN

_processor = None
_model = None


def _load_model():
    """Load MedASR once, reuse across requests."""
    global _processor, _model
    if _model is None:
        model_path = settings.MEDASR_MODEL
        _processor = AutoProcessor.from_pretrained(model_path)
        _model = AutoModel.from_pretrained(model_path, trust_remote_code=True)
        _model.eval()
    return _processor, _model


def _transcribe_segment(audio: np.ndarray, processor, model) -> str:
    """Transcribe a single audio segment."""
    inputs = processor(audio, sampling_rate=16000, return_tensors="pt")
    with torch.no_grad():
        logits = model(**inputs).logits
    predicted_ids = torch.argmax(logits, dim=-1)
    return processor.batch_decode(predicted_ids)[0].strip()


def transcribe_audio(audio_path: str) -> str:
    """Transcribe a medical audio file.

    For files longer than MEDASR_CHUNK_SECONDS, splits into overlapping
    chunks (90s default with 5s overlap) and concatenates transcripts.
    """
    processor, model = _load_model()
    audio, sr = librosa.load(audio_path, sr=16000, mono=True)

    duration_s = len(audio) / sr
    chunk_seconds = settings.MEDASR_CHUNK_SECONDS
    overlap_seconds = 5

    # Short audio: transcribe directly
    if duration_s <= chunk_seconds + overlap_seconds:
        return _transcribe_segment(audio, processor, model)

    # Long audio: chunk with overlap
    chunk_samples = chunk_seconds * sr
    overlap_samples = overlap_seconds * sr
    step = chunk_samples - overlap_samples

    transcripts = []
    offset = 0
    while offset < len(audio):
        end = min(offset + chunk_samples, len(audio))
        segment = audio[offset:end]
        text = _transcribe_segment(segment, processor, model)
        transcripts.append(text)
        offset += step

    return " ".join(transcripts)


def transcribe_bytes(audio_bytes: bytes, ext: str = "wav") -> str:
    """Transcribe from uploaded file bytes."""
    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as f:
        f.write(audio_bytes)
        tmp = f.name
    try:
        return transcribe_audio(tmp)
    finally:
        os.unlink(tmp)
