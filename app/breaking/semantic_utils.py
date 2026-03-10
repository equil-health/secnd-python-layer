"""
Semantic matching utilities for SECND Pulse v2.

Used by: breaking_ranker.py, breaking_store.py, routes_breaking.py,
         domain_validator.py, claim_extractor.py (research pipeline)

Embedding model: text-embedding-004 via Google AI Studio REST API
                 (same GEMINI_API_KEY, no Vertex AI needed)
Storage: pgvector on existing PostgreSQL instance.
"""

import time
import logging
import requests
import numpy as np

from ..config import settings

logger = logging.getLogger(__name__)

EMBED_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "text-embedding-004:embedContent"
)
BATCH_EMBED_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "text-embedding-004:batchEmbedContents"
)


# ── Embedding generation ────────────────────────────────────────────

def get_embedding(text: str) -> list[float]:
    """Generate embedding for a single text string via Google AI Studio."""
    url = f"{EMBED_URL}?key={settings.GEMINI_API_KEY}"
    payload = {
        "model": "models/text-embedding-004",
        "content": {"parts": [{"text": text[:2000]}]},
    }

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 200:
                return resp.json()["embedding"]["values"]
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Embedding API error: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Embedding API exception: {e}")
        if attempt < 2:
            time.sleep(1)

    raise RuntimeError("Embedding API failed after 3 attempts")


def get_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Batch embedding generation — more efficient for multiple texts.

    Google AI Studio batch endpoint handles up to 100 texts per call.
    For larger batches, we chunk into groups of 100.
    """
    if not texts:
        return []

    results = []
    # Process in chunks of 100
    for i in range(0, len(texts), 100):
        chunk = texts[i:i + 100]
        chunk_results = _embed_batch_chunk(chunk)
        results.extend(chunk_results)

    return results


def _embed_batch_chunk(texts: list[str]) -> list[list[float]]:
    """Embed a chunk of up to 100 texts."""
    url = f"{BATCH_EMBED_URL}?key={settings.GEMINI_API_KEY}"
    payload = {
        "requests": [
            {
                "model": "models/text-embedding-004",
                "content": {"parts": [{"text": t[:2000]}]},
            }
            for t in texts
        ],
    }

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                return [e["values"] for e in data["embeddings"]]
            if resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            logger.error(f"Batch embed error: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Batch embed exception: {e}")
        if attempt < 2:
            time.sleep(1)

    # Fallback: embed one at a time
    logger.warning("Batch embed failed, falling back to individual embedding")
    return [get_embedding(t) for t in texts]


# ── Similarity computation ──────────────────────────────────────────

def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two embedding vectors. Returns 0.0–1.0."""
    va, vb = np.array(a), np.array(b)
    denom = np.linalg.norm(va) * np.linalg.norm(vb)
    return float(np.dot(va, vb) / denom) if denom > 0 else 0.0


# ── Breaking: semantic deduplication ─────────────────────────────────

def semantic_dedup(
    headlines: list[dict],
    threshold: float = 0.87,
) -> list[dict]:
    """Remove near-duplicate headlines by clustering title embeddings.

    One headline per story cluster, keeping the highest-quality source.
    Threshold 0.87 — conservatively high to avoid merging distinct stories.
    """
    if len(headlines) <= 1:
        return headlines

    titles = [h.get("title", "") for h in headlines]
    embeddings = get_embeddings_batch(titles)

    n = len(headlines)
    merged = set()
    result = []

    SOURCE_QUALITY = [
        "nejm.org", "thelancet.com", "bmj.com",
        "jamanetwork.com", "nature.com", "who.int",
        "fda.gov", "cdsco.gov.in",
    ]

    def source_rank(h):
        url = h.get("url", "").lower()
        for i, s in enumerate(SOURCE_QUALITY):
            if s in url:
                return i
        return len(SOURCE_QUALITY)

    for i in range(n):
        if i in merged:
            continue
        cluster = [i]
        for j in range(i + 1, n):
            if j in merged:
                continue
            sim = cosine_similarity(embeddings[i], embeddings[j])
            if sim >= threshold:
                cluster.append(j)
                merged.add(j)

        best = min(cluster, key=lambda idx: source_rank(headlines[idx]))
        result.append(headlines[best])

    return result


# ── Evidence: relevance pre-filter ───────────────────────────────────

def filter_evidence_by_relevance(
    claim_text: str,
    snippets: list[dict],
    threshold: float = 0.68,
) -> list[dict]:
    """Filter evidence snippets by semantic relevance to a claim.

    Threshold 0.68 — intentionally low; discarding relevant evidence
    is worse than keeping marginal evidence.
    """
    if not snippets:
        return snippets

    claim_emb = get_embedding(claim_text)
    snippet_texts = [s.get("snippet", s.get("title", "")) for s in snippets]
    snippet_embs = get_embeddings_batch(snippet_texts)

    return [
        s for s, emb in zip(snippets, snippet_embs)
        if cosine_similarity(claim_emb, emb) >= threshold
    ]


# ── Read-time semantic re-ranking (Breaking feed) ───────────────────

def semantic_rerank(
    headlines: list[dict],
    doctor_topic_embeddings: list[list[float]],
) -> list[dict]:
    """Re-order headlines by semantic similarity to doctor's topic profile.

    Rules:
    - ALERT headlines always sorted first (safety).
    - Within each tier, headlines sorted by max similarity to doctor's topics.
    """
    if not doctor_topic_embeddings or not headlines:
        return headlines

    headline_texts = [h.get("snippet", h.get("title", "")) for h in headlines]
    headline_embs = get_embeddings_batch(headline_texts)

    def relevance_score(emb):
        return max(
            cosine_similarity(emb, topic_emb)
            for topic_emb in doctor_topic_embeddings
        )

    scored = [
        (h, relevance_score(emb))
        for h, emb in zip(headlines, headline_embs)
    ]

    TIER_ORDER = {"ALERT": 0, "MAJOR": 1, "NEW": 2}

    scored.sort(key=lambda x: (
        TIER_ORDER.get(x[0].get("urgency_tier", "NEW"), 2),
        -x[1],
    ))

    return [h for h, _ in scored]
