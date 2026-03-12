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


def active_specialties() -> list[str]:
    """Return list of active specialties for Breaking pipeline."""
    return list(SPECIALTY_SEARCH_QUERIES.keys())


def fetch_breaking_headlines(
    specialty: str,
    skip_cache: bool = False,
) -> list[dict]:
    """Fetch raw headlines for one specialty using multiple targeted sub-queries.

    v6.0: Runs all sub-queries in SPECIALTY_SEARCH_QUERIES[specialty] as
    separate Serper news calls (20 results each). Results are pooled and
    tagged with specialty + source_query before returning. URL-level dedup
    only here; semantic dedup happens downstream.

    Args:
        specialty: One of the 17 active specialties
        skip_cache: Bypass Redis cache

    Returns:
        Pooled raw headlines with specialty and source_query tags.
        Typically 60-80 items before deduplication.
    """
    queries = SPECIALTY_SEARCH_QUERIES.get(specialty, [specialty])
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
