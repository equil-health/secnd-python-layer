"""Breaking Steps B2, B2.5, B3 — Source filter, semantic dedup, Gemini rank,
OpenAlex verify, urgency classification (v6.0).

B2:   filter_by_source_quality() -> semantic_dedup() -> RANK_PROMPT -> Gemini top 7
B2.5: OpenAlex verify on ranked headlines — filter retractions, enrich quality data
B3:   Gemini urgency classification (ALERT / MAJOR / NEW) with source credibility gate
"""

import json
import logging
import re
from urllib.parse import urlparse

from ..config import settings
from ..pipeline.gemini import call_gemini
from ..pipeline.openalex import OpenAlexVerifier
from .semantic_utils import semantic_dedup


def _repair_json(text: str) -> list[dict]:
    """Attempt to repair truncated JSON arrays from Gemini.

    Handles: unterminated strings, missing closing brackets, trailing commas.
    """
    text = text.strip()
    # Remove markdown fences
    if text.startswith("```"):
        text = text.split("\n", 1)[-1]
    if text.endswith("```"):
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    # Try direct parse first
    try:
        parsed = json.loads(text)
        # Handle {"selected": [...]} wrapper from RANK_PROMPT
        if isinstance(parsed, dict):
            if "selected" in parsed:
                return parsed["selected"]
            if "classifications" in parsed:
                return parsed["classifications"]
        return parsed
    except json.JSONDecodeError:
        pass

    # Find the last complete object (ends with })
    last_brace = text.rfind("}")
    if last_brace == -1:
        raise ValueError("No complete JSON object found")

    truncated = text[:last_brace + 1]
    # Ensure it ends as a valid array
    if not truncated.rstrip().endswith("]"):
        truncated = truncated.rstrip().rstrip(",") + "\n]"

    try:
        parsed = json.loads(truncated)
        if isinstance(parsed, dict) and "selected" in parsed:
            return parsed["selected"]
        return parsed
    except json.JSONDecodeError:
        pass

    # Last resort: extract individual objects with regex
    objects = re.findall(r'\{[^{}]+\}', text)
    results = []
    for obj_str in objects:
        try:
            results.append(json.loads(obj_str))
        except json.JSONDecodeError:
            continue
    if results:
        return results

    raise ValueError(f"Could not repair JSON: {text[:200]}")


logger = logging.getLogger(__name__)


# ── Source quality filter (v6.0) ──────────────────────────────────

# Blocklist: never reach Gemini ranking
SOURCE_BLOCKLIST_DOMAINS = {
    # Press release / market research wires
    "openpr.com", "prnewswire.com", "businesswire.com", "globenewswire.com",
    "einpresswire.com", "accesswire.com", "prnewswire.co.in",
    # Consumer tabloids and general media
    "nypost.com", "dailymail.co.uk", "mirror.co.uk", "thesun.co.uk",
    "foxnews.com", "huffpost.com", "buzzfeed.com",
    # Geography-irrelevant regional outlets for Indian physician context
    "propakistani.pk", "thedailystar.net",
    "iol.co.za", "dailymaverick.co.za",
    "thestar.com.my", "galencentre.org",
    # Awareness / patient advocacy / wellness content
    "healthline.com", "webmd.com", "everydayhealth.com",
}

# ── SOURCE_QUALITY_TIERS — v8.0 comprehensive rewrite ────────────────────────
# Covers all 19 specialties. Domains are matched as substrings of the full
# URL netloc so subdomain variants (e.g. academic.oup.com) match correctly.
#
# tier_1: Primary journals, regulatory bodies, and major society publishers
#         whose content is definitively practice-changing when it appears.
# tier_2: Strong specialty-specific journals and reliable clinical news outlets.
# tier_3: Acceptable secondary sources — quality science journalism and
#         reputable aggregators. Used when tier_1/tier_2 absent.
SOURCE_QUALITY_TIERS = {
    "tier_1": [
        # ── Mega-journals (all specialties) ───────────────────────────────
        "nejm.org",             # New England Journal of Medicine
        "thelancet.com",        # The Lancet (all sub-journals)
        "bmj.com",              # The BMJ + BMJ Open
        "jamanetwork.com",      # JAMA + JAMA Network journals
        "nature.com",           # Nature Medicine, Nature Reviews journals
        "science.org",          # Science / Science Translational Medicine
        "cell.com",             # Cell + Cell Press journals
        "annals.org",           # Annals of Internal Medicine
        # ── Indian & global regulators ────────────────────────────────────
        "cdsco.gov.in",         # Central Drugs Standard Control Organisation
        "mohfw.gov.in",         # Ministry of Health India
        "icmr.nic.in",          # Indian Council of Medical Research
        "ncdc.gov.in",          # National Centre for Disease Control India
        "who.int",              # World Health Organization
        "fda.gov",              # US FDA
        "ema.europa.eu",        # European Medicines Agency
        # ── Cardiology ────────────────────────────────────────────────────
        "ahajournals.org",      # Circulation, JAHA, Stroke, Hypertension
        "jacc.org",             # JACC family
        "europeanheartjournal.com",  # ESC flagship
        # ── Oncology ──────────────────────────────────────────────────────
        "jco.ascopubs.org",     # Journal of Clinical Oncology
        "annalsofoncology.org", # Annals of Oncology (ESMO)
        # ── Pulmonology ───────────────────────────────────────────────────
        "atsjournals.org",      # AJRCCM, AnnalsATS — ATS publisher
        "ersjournals.com",      # European Respiratory Journal
        # ── Nephrology ────────────────────────────────────────────────────
        "jasn.asnjournals.org", # Journal of the American Society of Nephrology
        "ajkd.org",             # American Journal of Kidney Diseases
        # ── Hematology ────────────────────────────────────────────────────
        "ashpublications.org",  # Blood + Blood Advances (ASH publisher)
        # ── Gastroenterology ──────────────────────────────────────────────
        "gastrojournal.org",    # Gastroenterology (AGA flagship)
        # ── Hepatology ────────────────────────────────────────────────────
        "journal-of-hepatology.eu",  # Journal of Hepatology (EASL)
        # ── Pediatrics ────────────────────────────────────────────────────
        "publications.aap.org", # Pediatrics (AAP)
        # ── Rheumatology ──────────────────────────────────────────────────
        "ard.bmj.com",          # Annals of the Rheumatic Diseases
    ],

    "tier_2": [
        # ── Cross-specialty clinical news ─────────────────────────────────
        "medpagetoday.com",
        "reuters.com",
        "apnews.com",
        # ── Cardiology ────────────────────────────────────────────────────
        "acc.org",              # American College of Cardiology
        "escardio.org",         # European Society of Cardiology
        # ── Oncology ──────────────────────────────────────────────────────
        "targetedoncology.com",
        "ascopost.com",
        "cancernetwork.com",
        "onclive.com",
        # ── Pulmonology ───────────────────────────────────────────────────
        "chest.journal.org",    # CHEST journal (ACCP)
        "thorax.bmj.com",       # Thorax (BMJ)
        "ersnet.org",           # ERS news
        # ── Nephrology ────────────────────────────────────────────────────
        "renalandurologynews.com",
        "kdigo.org",            # KDIGO guideline publications
        # ── Hematology ────────────────────────────────────────────────────
        "haematologica.org",    # Haematologica (EHA)
        "hematologyadvisor.com",
        "bloodadvances.org",
        # ── Gastroenterology / Hepatology ─────────────────────────────────
        "gut.bmj.com",          # Gut (BMJ)
        "cghjournal.org",       # Clinical Gastroenterology and Hepatology
        "aasld.org",            # AASLD guideline publications
        "natap.org",            # HIV/hepatitis treatment updates
        # ── Neurology ─────────────────────────────────────────────────────
        "neurology.org",        # Neurology (AAN)
        "jnnp.bmj.com",         # Journal of Neurology Neurosurgery Psychiatry
        "strokejournal.org",    # Stroke (AHA)
        "multiplesclerosis.net",
        # ── Endocrinology ─────────────────────────────────────────────────
        "diabetesjournals.org", # Diabetes Care + Diabetes (ADA)
        "endocrinenews.org",
        "jcem.endojournals.org",  # Journal of Clinical Endocrinology
        # ── Dermatology ───────────────────────────────────────────────────
        "jaad.org",             # Journal of the American Academy of Dermatology
        "bjdonline.com",        # British Journal of Dermatology
        "dermadvisor.com",
        "practicaldermatology.com",
        # ── Rheumatology ──────────────────────────────────────────────────
        "rheumatology.org",     # Rheumatology (BSR)
        "arthritis-research.com",
        "rmdopen.bmj.com",      # RMD Open
        "healio.com/rheumatology",
        # ── Infectious Disease ────────────────────────────────────────────
        "academic.oup.com/cid", # Clinical Infectious Diseases
        "journalofinfection.com",
        "thelancet.com/journals/laninf",  # Lancet Infectious Diseases
        "idsa.org",             # IDSA guidelines
        "antimicrobe.org",
        # ── Ophthalmology ─────────────────────────────────────────────────
        "ophthalmology.aaojournal.org",  # Ophthalmology (AAO)
        "iovs.arvojournals.org",         # IOVS
        "bjo.bmj.com",                   # British Journal of Ophthalmology
        "reviewofophthalmology.com",
        # ── Orthopaedics ──────────────────────────────────────────────────
        "jbjs.org",             # Journal of Bone and Joint Surgery
        "boneandjoint.org.uk",  # Bone & Joint Journal (BJJ)
        "clinicalorthop.org",   # Clinical Orthopaedics and Related Research
        "orthopaedicproceedings.org",
        # ── Pediatrics ────────────────────────────────────────────────────
        "indianpediatrics.net", # Indian Pediatrics (IAP)
        "archpedi.jamanetwork.com",  # JAMA Pediatrics
        "bmj.com/specialties/paediatrics",
        # ── Psychiatry ────────────────────────────────────────────────────
        "ajp.psychiatryonline.org",   # American Journal of Psychiatry
        "bjp.rcpsych.org",            # British Journal of Psychiatry
        "jamanetwork.com/journals/jamapsychiatry",
        "psychologytoday.com",
        # ── Gynaecology ───────────────────────────────────────────────────
        "ajog.org",             # American Journal of Obstetrics & Gynecology
        "bjog.org",             # BJOG (RCOG)
        "ijgo.org",             # International Journal of Gynaecology & Obstetrics
        "acog.org",             # ACOG practice bulletins
        "fogsi.org",            # Federation of Obstetric & Gynaecological Societies India
        # ── Emergency Medicine ────────────────────────────────────────────
        "annalsofem.com",       # Annals of Emergency Medicine (ACEP)
        "emergencymedicinenews.com",
        "acep.org",             # ACEP guidelines
        "emjournal.bmj.com",    # Emergency Medicine Journal (BMJ)
        # ── Indian specialty journals (all specialties) ───────────────────
        "japi.org",             # Journal of Association of Physicians of India
        "ijmr.org.in",          # Indian Journal of Medical Research
        "mgims.ac.in",          # MGIMS journal
        "jiaps.com",            # Journal of Indian Association of Paediatric Surgeons
    ],

    "tier_3": [
        # Acceptable science journalism and aggregators
        "sciencedaily.com",
        "medicalxpress.com",
        "healio.com",
        "mdedge.com",
        "healthday.com",
        "cnbc.com",
        # Indian business and general press for regulatory news
        "thehindu.com",
        "livemint.com",
        "economictimes.indiatimes.com",
        "business-standard.com",
    ],
}


def _extract_domain(url: str) -> str:
    """Extract bare domain from URL for blocklist/tier matching."""
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


def _get_source_tier(domain: str) -> str:
    """Return tier label for a domain."""
    for tier, domains in SOURCE_QUALITY_TIERS.items():
        if any(d in domain for d in domains):
            return tier
    return "unranked"


def filter_by_source_quality(headlines: list[dict]) -> list[dict]:
    """Remove blocklisted sources before ranking. Annotate remaining
    headlines with source_tier for Gemini context.

    Deterministic pre-filter — no model call, no latency impact.
    """
    filtered = []
    blocked_count = 0

    for h in headlines:
        domain = _extract_domain(h.get("url", ""))

        if any(blocked in domain for blocked in SOURCE_BLOCKLIST_DOMAINS):
            logger.info(
                f"Source blocked: {domain} | "
                f"{h.get('title', '')[:60]}"
            )
            blocked_count += 1
            continue

        h["source_tier"] = _get_source_tier(domain)
        filtered.append(h)

    logger.info(
        f"[Breaking B2] filter_by_source_quality: {blocked_count} blocked, "
        f"{len(filtered)} passed"
    )
    return filtered


# ── B2: Rank headlines (v6.0 — fully specified RANK_PROMPT) ───────

RANK_PROMPT = """You are a senior clinical editor curating daily medical intelligence for
Indian specialist physicians. Your audience is practising doctors who need
actionable clinical information, not general health news.

SPECIALTY: {specialty}
TODAY'S DATE: {today}

DOCTOR'S DECLARED TOPICS OF INTEREST:
{doctor_topics_context}

HEADLINES TO EVALUATE (each includes source_tier: tier_1/tier_2/tier_3/unranked):
{headlines_json}

Your task:
Select the best 5-7 headlines from the list above. Apply these rules strictly.

--- MUST INCLUDE (highest priority) ---
- New clinical trial results (Phase 2/3/4) directly relevant to {specialty}
- Drug approvals, safety warnings, recalls, or black box updates (FDA, CDSCO, EMA)
- Major guideline updates from recognised bodies (ACC/AHA/ESC/KDIGO/WHO/NMC/ICMR)
- Practice-changing systematic reviews or meta-analyses with named effect sizes

--- PREFER ---
- Headlines directly relevant to one or more of the DOCTOR'S DECLARED TOPICS above.
  If a headline matches a declared topic, it should rank above a general specialty
  headline of equal clinical quality. This is the primary personalisation signal.
- Sources with source_tier: tier_1 or tier_2
- Studies with named trial acronyms (CREDENCE, DAPA-CKD, EMPEROR-Reduced etc.)
- Findings with specific named endpoints and numeric effect sizes
- CDSCO, MOHFW, or ICMR communications for India-specific regulatory news

--- EXCLUDE — do not select regardless of apparent relevance ---
- Press releases and market research reports (openPR.com, PRNewsWire, BusinessWire,
  market size reports, "Top Companies in X" stories)
- Awareness day content: World Kidney Day, World Cancer Day, World Heart Day features,
  annual awareness editorials, patient advocacy posts — these are calendar content,
  not clinical intelligence
- Geography-irrelevant healthcare policy: government health programme announcements
  from Pakistan, Bangladesh, Sri Lanka, South Africa, Malaysia, or other non-Indian
  jurisdictions UNLESS the story concerns a drug, guideline, or safety signal directly
  used in Indian practice
- Tabloid or consumer health media: any source not in tier_1/tier_2/tier_3, or any
  source known for sensationalised health headlines regardless of tier annotation
- Opinion pieces and editorials unless authored by a named specialist society
- Duplicate topics: if two headlines cover the same clinical story, select only the
  higher-tier source

--- SNIPPET AND RESEARCH TOPIC ---
For each selected headline, generate:
1. snippet: 2 sentences. Explain WHY this matters to a {specialty} physician in India.
   If this headline is directly relevant to a declared topic, explicitly note why it
   matters for that topic. Focus on what it changes in practice.
   Do NOT restate the headline title. Do NOT write generic text.
2. research_topic: 1 precise clinical question for Deep Research pre-population.
   Write a specific question, not a restatement of the title.

--- RETURN FORMAT ---
Return JSON only — no preamble, no markdown fences:
{{
  "selected": [
    {{
      "original_index": <int>,
      "title": "exact original title",
      "url": "original url",
      "source": "original source",
      "snippet": "<2 sentences, specialty-specific clinical relevance>",
      "research_topic": "<precise clinical question>",
      "rank_score": 50-100,
      "rank_position": <1-7>
    }}
  ],
  "excluded": [
    {{
      "original_index": <int>,
      "excluded_reason": "<one of: press_release | awareness_day | geography_irrelevant | low_quality_source | duplicate_topic | not_clinically_actionable>"
    }}
  ]
}}
"""


def build_doctor_topics_context(
    specialty: str,
    topic_entries: list[dict],
) -> str:
    """Build the doctor_topics_context string for RANK_PROMPT injection.

    Args:
        specialty:     The specialty being ranked.
        topic_entries: List of topic dicts from doctor_preferences.specialty_topics[specialty].
                       Each: {"topic_text": "...", "generated_queries": [...]}

    Returns:
        Formatted string for injection into RANK_PROMPT.
        Returns a generic message if no topics are declared.
    """
    if not topic_entries:
        return (
            f"No specific topics declared. "
            f"Rank by general {specialty} clinical importance."
        )

    lines = ["This doctor has declared the following clinical topics of interest:"]
    for i, entry in enumerate(topic_entries, 1):
        lines.append(f"  {i}. {entry['topic_text']}")
    lines.append(
        "\nStrongly prefer headlines that are directly relevant to any of these topics. "
        "A headline matching a declared topic should rank above a general specialty "
        "headline of equal clinical quality."
    )
    return "\n".join(lines)


def build_batch_topics_context(specialty: str) -> str:
    """Build RANK_PROMPT context from the union of all active doctor topics
    for this specialty. Used in the batch pipeline where a single Gemini
    ranking call serves all doctors for that specialty.
    """
    from collections import Counter
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ..config import settings as _settings
    from ..models.breaking import DoctorPreferences

    sync_url = _settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
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

    topic_texts: list[str] = []
    for prefs in active_prefs:
        topics = prefs.specialty_topics or {}
        for entry in topics.get(specialty, []):
            text = entry.get("topic_text", "").strip()
            if text:
                topic_texts.append(text)

    if not topic_texts:
        return (
            f"No specific topics declared. "
            f"Rank by general {specialty} clinical importance."
        )

    # Show top topics by frequency, max 10 for prompt conciseness
    counted = Counter(topic_texts)
    top_topics = [t for t, _ in counted.most_common(10)]

    lines = [
        f"Active doctors in {specialty} have declared these topics of interest "
        f"(listed by popularity):"
    ]
    for i, topic in enumerate(top_topics, 1):
        count = counted[topic]
        lines.append(
            f"  {i}. {topic}"
            + (f" ({count} doctors)" if count > 1 else "")
        )
    lines.append(
        "\nStrongly prefer headlines relevant to any of these topics. "
        "Higher-frequency topics reflect broader doctor interest and should "
        "be weighted accordingly."
    )
    return "\n".join(lines)


def rank_headlines(
    raw_headlines: list[dict],
    specialty: str,
    top_n: int = 7,
    topics_context: str = "",
) -> list[dict]:
    """B2: Source filter -> Semantic dedup -> Gemini rank -> top N per specialty.

    v7.0 processing order:
    fetch (B1) -> filter_by_source_quality() -> semantic_dedup() -> RANK_PROMPT -> Gemini

    Args:
        raw_headlines: Raw headlines from B1 fetcher (typically 60-120)
        specialty: Medical specialty
        top_n: Number of headlines to select (default 7)
        topics_context: v7.0 doctor topics context string for RANK_PROMPT

    Returns:
        List of top_n ranked headline dicts with source_tier annotation
    """
    if not raw_headlines:
        return []

    # v6.0: Source quality filter before dedup
    filtered = filter_by_source_quality(raw_headlines)

    # Semantic deduplication on filtered pool
    deduped = semantic_dedup(filtered, threshold=settings.BREAKING_DEDUP_THRESHOLD)
    logger.info(
        f"[Breaking B2] {specialty}: {len(raw_headlines)} raw -> "
        f"{len(filtered)} after source filter -> "
        f"{len(deduped)} after dedup"
    )

    if len(deduped) <= top_n:
        for i, h in enumerate(deduped):
            h.setdefault("rank_score", 50)
            h.setdefault("rank_position", i + 1)
            h.setdefault("snippet", "")
            h.setdefault("research_topic", h.get("title", ""))
        return deduped

    # Format headlines with source_tier for Gemini
    from datetime import date
    headlines_for_gemini = [
        {
            "index": i,
            "title": h.get("title", ""),
            "url": h.get("url", ""),
            "source": h.get("source", ""),
            "source_tier": h.get("source_tier", "unranked"),
        }
        for i, h in enumerate(deduped)
    ]

    # v7.0: Inject doctor topics context into RANK_PROMPT
    context_str = topics_context or (
        f"No specific topics declared. "
        f"Rank by general {specialty} clinical importance."
    )

    prompt = RANK_PROMPT.format(
        specialty=specialty,
        today=str(date.today()),
        doctor_topics_context=context_str,
        headlines_json=json.dumps(headlines_for_gemini, indent=2),
    )

    try:
        result_text = call_gemini(prompt, max_tokens=8192, temperature=0.1, json_mode=True)
        ranked = _repair_json(result_text)

        # Log exclusion reasons for feed quality monitoring
        if isinstance(ranked, list):
            selected = ranked
        else:
            selected = ranked

        # Build enriched results from selected headlines
        enriched = []
        for i, r in enumerate(selected[:top_n]):
            # Match back to deduped list by original_index or url
            orig_idx = r.get("original_index")
            original = {}
            if orig_idx is not None and 0 <= orig_idx < len(deduped):
                original = deduped[orig_idx]
            else:
                # Fallback: match by url
                url_map = {h["url"]: h for h in deduped if h.get("url")}
                original = url_map.get(r.get("url"), {})

            merged = {**original, **r}
            merged["rank_position"] = i + 1
            merged["specialty"] = specialty
            enriched.append(merged)

        logger.info(f"[Breaking B2] {specialty}: ranked {len(enriched)} headlines")
        return enriched

    except Exception as e:
        logger.error(f"[Breaking B2] Gemini rank failed for {specialty}: {e}")
        for i, h in enumerate(deduped[:top_n]):
            h["rank_position"] = i + 1
            h["rank_score"] = 50
            h["specialty"] = specialty
        return deduped[:top_n]


# ── B2.5: OpenAlex verification ─────────────────────────────────────

def verify_breaking_sources(headlines: list[dict]) -> list[dict]:
    """B2.5: Run OpenAlex verification on ranked Breaking headlines.

    Enriches headlines linking to scholarly works (journal URLs).
    Headlines linking to news articles return is_verified=False — expected.
    Retractions are filtered — must not reach the doctor's feed.

    Args:
        headlines: Ranked headlines for one specialty (up to 7)

    Returns:
        Headlines with OpenAlex metadata. Retracted headlines removed.
    """
    verifier = OpenAlexVerifier(
        email=settings.OPENALEX_EMAIL,
        api_key=settings.OPENALEX_API_KEY,
    )
    enriched = []

    for h in headlines:
        result = verifier.verify_single(h.get("url", ""), h.get("title", ""))

        if result:
            h["is_verified"] = True
            h["citation_count"] = result.get("cited_by_count")
            h["quality_tier"] = result.get("quality_tier")
            h["is_retracted"] = result.get("is_retracted", False)
            h["journal"] = result.get("journal")
        else:
            h["is_verified"] = False
            h["citation_count"] = None
            h["quality_tier"] = None
            h["is_retracted"] = False

        if h.get("is_retracted"):
            logger.warning(
                f"[Breaking B2.5] Retracted source filtered. "
                f"URL: {h.get('url', 'unknown')} | Title: {h.get('title', '')[:80]}"
            )
            continue

        enriched.append(h)

    logger.info(
        f"[Breaking B2.5] {len(headlines)} in -> {len(enriched)} out "
        f"({len(headlines) - len(enriched)} retracted/filtered)"
    )
    return enriched


# ── B3: Urgency classification (v6.0 — source credibility gate) ───

URGENCY_PROMPT = """Assign an urgency tier to each headline for {specialty} physicians.

TIER DEFINITIONS:

ALERT — Drug recall, black box warning, trial stopped for patient harm,
  CDSCO/FDA/EMA safety communication, market withdrawal of a drug.
  CRITICAL CONSTRAINT: ALERT requires the story to originate from a
  regulatory body, a peer-reviewed journal, or a tier_1/tier_2 source.
  A consumer media, tabloid, or unranked source reporting alarming
  statistics (e.g. "increases risk by 400%") does NOT qualify as ALERT
  even if the language sounds urgent. Classify such headlines as NEW.
  If uncertain whether the source is authoritative, default to MAJOR or NEW.

MAJOR — Landmark Phase 3/4 RCT result with named trial and effect size,
  major society guideline update (AHA/ESC/KDIGO/NMC/WHO), new first-in-class
  drug approval by CDSCO or FDA, large practice-changing meta-analysis.

NEW — Observational study, Phase 1/2 result, secondary analysis, expert
  commentary, device news, narrative review. Default when in doubt — do
  not escalate to MAJOR or ALERT without clear justification.

HEADLINES (each includes source_tier and OpenAlex fields from prior steps):
{headlines_with_source_tier}

For each headline return:
- urgency_tier: ALERT | MAJOR | NEW
- urgency_reason: one sentence explaining the classification
- source_credibility_note: if classifying ALERT, name the primary source
  that justifies this tier (regulatory body, journal, or outlet name).
  Return null for MAJOR and NEW.

Return JSON only:
{{
  "classifications": [
    {{
      "title": "exact title",
      "urgency_tier": "ALERT | MAJOR | NEW",
      "urgency_reason": "<one sentence>",
      "source_credibility_note": "<primary source name, or null>"
    }}
  ]
}}
"""


def assign_urgency(headlines: list[dict], specialty: str) -> list[dict]:
    """B3: Classify urgency tier for each headline via Gemini.

    v6.0: Includes source_tier and OpenAlex fields in the prompt so Gemini
    can apply the source credibility gate for ALERT classification.

    Args:
        headlines: Verified headlines from B2.5 (up to 7)
        specialty: Medical specialty

    Returns:
        Headlines with urgency_tier, urgency_reason, source_credibility_note added.
    """
    if not headlines:
        return []

    headlines_json = json.dumps(
        [
            {
                "title": h.get("title", ""),
                "snippet": h.get("snippet", ""),
                "source": h.get("source", ""),
                "url": h.get("url", ""),
                "source_tier": h.get("source_tier", "unranked"),
                "is_verified": h.get("is_verified", False),
                "citation_count": h.get("citation_count"),
                "quality_tier": h.get("quality_tier"),
            }
            for h in headlines
        ],
        indent=2,
    )

    prompt = URGENCY_PROMPT.format(
        specialty=specialty,
        headlines_with_source_tier=headlines_json,
    )

    try:
        result_text = call_gemini(prompt, max_tokens=4096, temperature=0.1, json_mode=True)
        classified = _repair_json(result_text)

        # Merge urgency back into headlines (match by title)
        title_map = {c["title"]: c for c in classified}
        for h in headlines:
            match = title_map.get(h.get("title", ""))
            if match:
                h["urgency_tier"] = match.get("urgency_tier", "NEW")
                h["urgency_reason"] = match.get("urgency_reason", "")
                h["source_credibility_note"] = match.get("source_credibility_note")
            else:
                h["urgency_tier"] = "NEW"
                h["urgency_reason"] = ""
                h["source_credibility_note"] = None

        alert_count = sum(1 for h in headlines if h.get("urgency_tier") == "ALERT")
        major_count = sum(1 for h in headlines if h.get("urgency_tier") == "MAJOR")
        logger.info(
            f"[Breaking B3] {specialty}: "
            f"{alert_count} ALERT, {major_count} MAJOR, "
            f"{len(headlines) - alert_count - major_count} NEW"
        )
        return headlines

    except Exception as e:
        logger.error(f"[Breaking B3] Urgency classification failed for {specialty}: {e}")
        for h in headlines:
            h.setdefault("urgency_tier", "NEW")
            h.setdefault("urgency_reason", "")
            h.setdefault("source_credibility_note", None)
        return headlines
