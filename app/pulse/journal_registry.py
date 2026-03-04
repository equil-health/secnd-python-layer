"""Journal registry — access strategies, evidence grading, circuit breaker."""

import time
from enum import Enum

import redis

from ..config import settings

_redis = None


def _get_redis():
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(settings.REDIS_URL)
    return _redis

USER_AGENT = "SECND-Medical-Research-Bot/2.0 (+https://secnd.ai/bot-policy)"


class AccessStrategy(str, Enum):
    DIRECT = "direct"
    PROXY_VIA_PUBMED = "proxy_via_pubmed"
    THROTTLED = "throttled"
    METADATA_ONLY = "metadata_only"


JOURNAL_REGISTRY = {
    "NEJM": {
        "name": "New England Journal of Medicine",
        "issn": "0028-4793",
        "nlm_id": "0255562",
        "strategy": AccessStrategy.PROXY_VIA_PUBMED,
        "allowed_paths": ["/doi/abstract/"],
        "forbidden_paths": ["/doi/full/", "/doi/pdf/"],
    },
    "Lancet": {
        "name": "The Lancet",
        "issn": "0140-6736",
        "nlm_id": "2985213R",
        "strategy": AccessStrategy.PROXY_VIA_PUBMED,
        "allowed_paths": ["/article/"],
        "forbidden_paths": ["/action/showPdf"],
    },
    "JAMA": {
        "name": "JAMA",
        "issn": "0098-7484",
        "nlm_id": "7501160",
        "strategy": AccessStrategy.PROXY_VIA_PUBMED,
        "allowed_paths": ["/journals/jama/article-abstract/"],
        "forbidden_paths": ["/journals/jama/fullarticle/"],
    },
    "BMJ": {
        "name": "BMJ",
        "issn": "0959-8138",
        "nlm_id": "8900488",
        "strategy": AccessStrategy.DIRECT,
        "allowed_paths": ["/content/"],
        "forbidden_paths": [],
    },
    "Nature Medicine": {
        "name": "Nature Medicine",
        "issn": "1078-8956",
        "nlm_id": "9502015",
        "strategy": AccessStrategy.METADATA_ONLY,
        "allowed_paths": [],
        "forbidden_paths": ["/articles/"],
    },
    "Cochrane": {
        "name": "Cochrane Database of Systematic Reviews",
        "issn": "1469-493X",
        "nlm_id": "100909747",
        "strategy": AccessStrategy.DIRECT,
        "allowed_paths": ["/doi/"],
        "forbidden_paths": [],
    },
    "Annals of Internal Medicine": {
        "name": "Annals of Internal Medicine",
        "issn": "0003-4819",
        "nlm_id": "0372351",
        "strategy": AccessStrategy.PROXY_VIA_PUBMED,
        "allowed_paths": ["/doi/abs/"],
        "forbidden_paths": ["/doi/full/"],
    },
    "PubMed": {
        "name": "PubMed Central",
        "issn": "",
        "nlm_id": "",
        "strategy": AccessStrategy.DIRECT,
        "allowed_paths": ["/articles/"],
        "forbidden_paths": [],
    },
}


# Publication type → evidence grade mapping
PUBTYPE_TO_GRADE = {
    "Meta-Analysis": "Meta-Analysis",
    "Systematic Review": "Systematic Review",
    "Randomized Controlled Trial": "RCT",
    "Clinical Trial": "Clinical Trial",
    "Controlled Clinical Trial": "RCT",
    "Pragmatic Clinical Trial": "RCT",
    "Cohort Study": "Cohort Study",
    "Observational Study": "Cohort Study",
    "Case-Control Study": "Case-Control",
    "Practice Guideline": "Guideline",
    "Guideline": "Guideline",
    "Consensus Development Conference": "Guideline",
    "Review": "Review",
    "Case Reports": "Case Report",
    "Letter": "Expert Opinion",
    "Editorial": "Expert Opinion",
    "Comment": "Expert Opinion",
}

# Default grade for unknown publication types
DEFAULT_EVIDENCE_GRADE = "Ungraded"


def grade_evidence(pub_types: list[str]) -> str:
    """Return the highest evidence grade from a list of PubMed publication types."""
    grade_priority = [
        "Meta-Analysis", "Systematic Review", "RCT", "Clinical Trial",
        "Cohort Study", "Case-Control", "Guideline", "Review",
        "Case Report", "Expert Opinion",
    ]
    grades = []
    for pt in pub_types:
        g = PUBTYPE_TO_GRADE.get(pt)
        if g:
            grades.append(g)
    if not grades:
        return DEFAULT_EVIDENCE_GRADE
    # Return highest priority
    for g in grade_priority:
        if g in grades:
            return g
    return grades[0]


# ── Circuit breaker ──────────────────────────────────────────────

CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_WINDOW = 3600       # 1 hour
CIRCUIT_BREAKER_BLACKLIST_TTL = 43200  # 12 hours


def record_error(domain: str) -> None:
    """Record a fetch error for a domain. Blacklist if threshold exceeded."""
    r = _get_redis()
    key = f"pulse:cb:errors:{domain}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, CIRCUIT_BREAKER_WINDOW)
    results = pipe.execute()

    count = results[0]  # INCR returns the new value atomically
    if count >= CIRCUIT_BREAKER_THRESHOLD:
        r.setex(f"pulse:cb:blacklist:{domain}", CIRCUIT_BREAKER_BLACKLIST_TTL, "1")


def is_domain_blacklisted(domain: str) -> bool:
    """Check if a domain is currently blacklisted by the circuit breaker."""
    return bool(_get_redis().get(f"pulse:cb:blacklist:{domain}"))
