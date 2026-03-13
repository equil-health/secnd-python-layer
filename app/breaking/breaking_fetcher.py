"""Breaking Step B1 — Fetch headlines via Serper news search (v6.0).

Calls Serper news endpoint with 3-4 targeted clinical sub-queries per specialty.
Pools results before deduplication: ~60-80 raw headlines per specialty.
time_filter="d" restricts to last 24 hours.
"""

import hashlib
import json
import logging
import time
import requests
import redis

from ..config import settings
from ..usage_tracker import tracker

logger = logging.getLogger(__name__)

_redis = redis.Redis.from_url(settings.REDIS_URL)
CACHE_TTL = 3600  # 1 hour for breaking news (fresher than research)

# v6.0: SPECIALTY_SEARCH_TERMS replaced by SPECIALTY_SEARCH_QUERIES
# Each specialty has 3-4 targeted sub-queries covering distinct clinical domains.
# Run each as a separate Serper news call, pool results, then deduplicate.
SPECIALTY_SEARCH_QUERIES = {
    "Cardiology": [
        "heart failure HFrEF HFpEF trial results guidelines 2025 2026",
        "ACS STEMI NSTEMI PCI reperfusion outcomes RCT",
        "atrial fibrillation anticoagulation ablation trial results",
        "CDSCO FDA cardiovascular drug approval recall 2025 2026",
    ],
    "Nephrology": [
        "CKD IgA nephropathy glomerulonephritis RCT trial results 2025 2026",
        "KDIGO dialysis hemodialysis peritoneal guidelines update",
        "renal transplant immunosuppression rejection outcomes trial",
        "AKI acute kidney injury biomarker treatment clinical study",
    ],
    "Oncology": [
        "cancer drug approval FDA CDSCO oncology trial results 2025 2026",
        "chemotherapy immunotherapy PD-1 PD-L1 CAR-T clinical trial outcomes",
        "cancer drug safety recall black box warning withdrawal 2025 2026",
        "targeted therapy biomarker NSCLC breast colorectal RCT results",
    ],
    "Neurology": [
        "stroke thrombolysis thrombectomy outcomes RCT guidelines 2025 2026",
        "epilepsy seizure drug approval treatment trial results",
        "dementia Alzheimer Parkinson disease-modifying therapy trial",
        "multiple sclerosis MS biologics trial results ECTRIMS 2025 2026",
    ],
    "Hepatology": [
        "NASH MASLD MAFLD clinical trial drug approval 2025 2026",
        "hepatitis B C cirrhosis antiviral treatment outcomes study",
        "liver transplant outcomes immunosuppression AASLD guidelines update",
        "DILI drug-induced liver injury hepatotoxicity safety signal",
    ],
    "Pulmonology": [
        "COPD asthma biologics inhaler RCT trial results guidelines 2025 2026",
        "ILD interstitial lung disease fibrosis treatment trial outcomes",
        "tuberculosis TB NTEP drug-resistant treatment outcomes India",
        "pulmonary hypertension PAH drug approval trial results",
    ],
    "Endocrinology": [
        "type 2 diabetes GLP-1 SGLT2 cardiovascular outcomes RCT 2025 2026",
        "thyroid cancer hypothyroidism hyperthyroidism treatment trial",
        "adrenal pituitary Cushing acromegaly drug approval trial results",
        "CDSCO FDA diabetes drug approval recall 2025 2026",
    ],
    "Gastroenterology": [
        "IBD Crohn ulcerative colitis biologic trial results 2025 2026",
        "colorectal cancer screening colonoscopy outcomes study",
        "H pylori eradication antibiotic resistance treatment trial",
        "GERD Barrett oesophagus endoscopy drug approval 2025 2026",
    ],
    "General Medicine": [
        "CDSCO NMC drug approval clinical guideline India 2025 2026",
        "India clinical trial results infectious disease outcomes",
        "antimicrobial resistance AMR antibiotic stewardship India study",
        "ICMR WHO guideline update India clinical management 2025 2026",
    ],
    "Rheumatology": [
        "rheumatoid arthritis RA biologic JAK inhibitor trial results 2025 2026",
        "lupus SLE vasculitis treatment trial outcomes guidelines",
        "gout hyperuricaemia urate-lowering therapy RCT results",
        "spondyloarthritis psoriatic arthritis biologic drug approval trial",
    ],
    "Dermatology": [
        "psoriasis biologic IL-17 IL-23 trial results guidelines 2025 2026",
        "atopic dermatitis eczema dupilumab JAK inhibitor RCT outcomes",
        "melanoma immunotherapy checkpoint inhibitor trial results 2025 2026",
        "acne rosacea hidradenitis suppurativa treatment drug approval",
    ],
    "Emergency Medicine": [
        "emergency medicine resuscitation cardiac arrest ROSC trial 2025 2026",
        "sepsis septic shock early management bundle RCT outcomes guidelines",
        "trauma critical care hemorrhage transfusion protocol trial results",
        "emergency toxicology overdose antidote treatment guideline update",
    ],
    "Hematology": [
        "leukemia lymphoma myeloma CAR-T bispecific trial results 2025 2026",
        "sickle cell thalassemia gene therapy drug approval RCT outcomes",
        "anticoagulation DOAC VTE thrombosis treatment trial guidelines",
        "MDS myeloproliferative neoplasm drug approval trial results 2025 2026",
    ],
    "Infectious Disease": [
        "antimicrobial resistance AMR antibiotic new drug trial results 2025 2026",
        "HIV antiretroviral PrEP long-acting treatment trial outcomes",
        "tuberculosis TB MDR-TB XDR-TB drug treatment trial India 2025 2026",
        "dengue malaria vaccine antiviral treatment clinical study India",
    ],
    "Ophthalmology": [
        "AMD macular degeneration anti-VEGF intravitreal injection trial 2025 2026",
        "glaucoma IOP treatment drug approval RCT outcomes guidelines",
        "diabetic retinopathy macular edema treatment trial results",
        "cataract refractive surgery IOL outcomes study 2025 2026",
    ],
    "Pediatrics": [
        "pediatric vaccine immunization schedule update WHO India 2025 2026",
        "neonatal sepsis NICU treatment outcomes RCT clinical study",
        "childhood asthma allergy biologic treatment trial results",
        "pediatric oncology leukemia neuroblastoma drug approval trial 2025 2026",
    ],
    "Psychiatry": [
        "depression anxiety SSRI SNRI ketamine psilocybin trial results 2025 2026",
        "schizophrenia antipsychotic long-acting injectable RCT outcomes",
        "ADHD autism spectrum treatment drug approval clinical trial",
        "bipolar disorder lithium mood stabilizer treatment guidelines update",
    ],
}


# v7.0: Always-appended safety query per specialty.
# Ensures CDSCO recalls, black-box warnings, and ICMR alerts are never missed
# even when a doctor's declared topics are narrow.
SPECIALTY_SAFETY_QUERIES = {
    "Cardiology":        "CDSCO FDA cardiovascular drug recall safety warning 2025 2026",
    "Nephrology":        "CDSCO renal drug safety recall nephrotoxicity 2025 2026",
    "Oncology":          "CDSCO FDA oncology drug approval recall safety 2025 2026",
    "Neurology":         "CDSCO FDA neurology drug recall safety warning 2025 2026",
    "Hepatology":        "CDSCO hepatotoxicity drug recall liver safety 2025 2026",
    "Pulmonology":       "CDSCO inhaler COPD asthma drug recall safety 2025 2026",
    "Endocrinology":     "CDSCO FDA diabetes drug recall hypoglycaemia safety 2025 2026",
    "Gastroenterology":  "CDSCO GI drug recall safety IBD gastroenterology 2025 2026",
    "General Medicine":  "CDSCO India drug recall safety warning NMC guideline 2025 2026",
    "Rheumatology":      "CDSCO FDA biologic JAK inhibitor recall safety 2025 2026",
    "Dermatology":       "CDSCO FDA dermatology drug recall safety warning 2025 2026",
    "Emergency Medicine":"CDSCO emergency drug recall safety critical care 2025 2026",
    "Hematology":        "CDSCO FDA hematology drug recall safety warning 2025 2026",
    "Infectious Disease":"CDSCO antimicrobial antibiotic recall safety resistance 2025 2026",
    "Ophthalmology":     "CDSCO FDA ophthalmology drug recall safety warning 2025 2026",
    "Pediatrics":        "CDSCO pediatric drug recall safety warning India 2025 2026",
    "Psychiatry":        "CDSCO FDA psychiatry drug recall safety warning 2025 2026",
}


def active_specialties() -> list[str]:
    """Return list of active specialties for Breaking pipeline."""
    return list(SPECIALTY_SEARCH_QUERIES.keys())


def build_batch_queries_for_specialty(
    specialty: str,
    max_queries: int = 20,
) -> list[str]:
    """Build the union of search queries for one specialty across all active doctors.

    Called once per specialty in breaking_daily_refresh(), before the fetch loop.

    Design:
    - Collects generated_queries from every active doctor's specialty_topics for this specialty.
    - Deduplicates across doctors (exact string match).
    - Ranks by frequency — queries shared by more doctors are more important.
    - Caps at max_queries to keep Serper cost predictable.
    - Appends SPECIALTY_SAFETY_QUERIES[specialty] unconditionally.
    - Falls back to SPECIALTY_SEARCH_QUERIES if no doctor has declared topics.
    """
    from collections import Counter
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ..models.breaking import DoctorPreferences

    sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
    engine = create_engine(sync_url)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        active_prefs = (
            db.query(DoctorPreferences)
            .filter(
                DoctorPreferences.breaking_enabled == True,  # noqa: E712
                DoctorPreferences.specialty_topics.isnot(None),
            )
            .all()
        )

    # Collect all generated queries for this specialty across active doctors
    all_queries: list[str] = []
    has_any_topics = False

    for prefs in active_prefs:
        topics = prefs.specialty_topics or {}
        specialty_entries = topics.get(specialty, [])
        for entry in specialty_entries:
            generated = entry.get("generated_queries", [])
            all_queries.extend(generated)
            if generated:
                has_any_topics = True

    # Fallback: no doctor has declared topics for this specialty
    if not has_any_topics:
        fallback = list(SPECIALTY_SEARCH_QUERIES.get(specialty, []))
        safety = SPECIALTY_SAFETY_QUERIES.get(specialty)
        if safety and safety not in fallback:
            fallback.append(safety)
        logger.info(
            f"build_batch_queries: {specialty} — no doctor topics, "
            f"using fallback ({len(fallback)} queries)"
        )
        return fallback

    # Rank by frequency, deduplicate, cap
    query_counts = Counter(all_queries)
    ranked = [q for q, _ in query_counts.most_common(max_queries)]

    # Always append specialty safety query — outside the cap
    safety = SPECIALTY_SAFETY_QUERIES.get(specialty)
    if safety and safety not in ranked:
        ranked.append(safety)

    logger.info(
        f"build_batch_queries: {specialty} — {len(all_queries)} raw, "
        f"{len(query_counts)} unique, {len(ranked)} selected (cap={max_queries})"
    )
    return ranked


def fetch_breaking_headlines(
    specialty: str,
    doctor_queries: list[str] | None = None,
    skip_cache: bool = False,
) -> list[dict]:
    """Fetch raw headlines for one specialty using the provided query list.

    v7.0: Accepts doctor_queries built by build_batch_queries_for_specialty()
    in the batch runner. Falls back to SPECIALTY_SEARCH_QUERIES if the list
    is empty or None.

    Args:
        specialty: One of the 17 active specialties
        doctor_queries: List of search queries for this specialty's batch.
                        Built from the union of all active doctors' generated
                        queries. May be empty/None (-> fallback).
        skip_cache: Bypass Redis cache

    Returns:
        Pooled raw headlines with specialty and source_query tags.
        Typically 60-120 items before deduplication.
    """
    queries = doctor_queries if doctor_queries else SPECIALTY_SEARCH_QUERIES.get(specialty, [specialty])
    if isinstance(queries, str):
        queries = [queries]

    # Check cache
    cache_key = f"breaking:raw:{hashlib.md5(specialty.encode()).hexdigest()}"
    if not skip_cache:
        try:
            cached = _redis.get(cache_key)
            if cached:
                return json.loads(cached)
        except redis.ConnectionError:
            pass

    pooled = []
    seen_urls = set()
    start = time.time()
    status = "success"
    error_msg = None

    for query in queries:
        try:
            resp = requests.post(
                "https://google.serper.dev/news",
                json={
                    "q": query,
                    "num": 20,
                    "tbs": "qdr:d",  # last 24 hours
                },
                headers={
                    "X-API-KEY": settings.SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("news", []):
                    url = item.get("link", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    pooled.append({
                        "title": item.get("title", ""),
                        "url": url,
                        "source": item.get("source", ""),
                        "snippet": item.get("snippet", ""),
                        "published_at": item.get("date", ""),
                        "specialty": specialty,
                        "source_query": query,
                    })
            else:
                status = "error"
                error_msg = f"HTTP {resp.status_code} {resp.text[:200]}"
                logger.error(f"Serper news error for {specialty} query '{query}': {error_msg}")
        except Exception as e:
            status = "error"
            error_msg = str(e)[:200]
            logger.error(f"Serper news exception for {specialty}: {e}")

    tracker.log(
        "breaking", "serper_news", "fetch_headlines",
        request_summary=f"{specialty}: {len(queries)} queries"[:500],
        status=status,
        error_message=error_msg,
        duration_ms=int((time.time() - start) * 1000),
        num_results=len(pooled),
        metadata={"specialty": specialty},
    )

    # Cache
    try:
        _redis.setex(cache_key, CACHE_TTL, json.dumps(pooled))
    except redis.ConnectionError:
        pass

    logger.info(
        f"[Breaking B1] {specialty}: {len(queries)} queries -> "
        f"{len(pooled)} raw headlines"
    )
    return pooled
