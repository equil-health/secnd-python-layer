#!/usr/bin/env python3
"""
Pulse Digest Diagnostic — run on server to find why results don't change.

Usage:
    cd /opt/storm_env/secnd-python-layer/script/backend
    source /opt/storm_env/bin/activate
    python test_pulse_query.py
"""
import os
import sys
import json
import hashlib
import time
import requests
import xml.etree.ElementTree as ET

# ── Load .env manually ──────────────────────────────────────────
env_path = os.path.join(os.path.dirname(__file__), ".env")
env_vars = {}
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env_vars[k.strip()] = v.strip().strip('"').strip("'")
    print(f"Loaded {len(env_vars)} vars from .env")
else:
    print("WARNING: No .env file found!")

NCBI_EMAIL = env_vars.get("NCBI_EMAIL", os.environ.get("NCBI_EMAIL", ""))
NCBI_API_KEY = env_vars.get("NCBI_API_KEY", os.environ.get("NCBI_API_KEY", ""))
REDIS_URL = env_vars.get("REDIS_URL", os.environ.get("REDIS_URL", ""))
DATABASE_URL = env_vars.get("DATABASE_URL", os.environ.get("DATABASE_URL", ""))
PULSE_SCAN_DAYS_BACK = int(env_vars.get("PULSE_SCAN_DAYS_BACK", "7"))

PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

print("\n" + "=" * 60)
print("STEP 1: Environment Check")
print("=" * 60)
print(f"  NCBI_EMAIL:    {'[SET] ' + NCBI_EMAIL if NCBI_EMAIL else '[MISSING!] PubMed will reject requests'}")
print(f"  NCBI_API_KEY:  {'[SET] (10 req/s)' if NCBI_API_KEY else '[NOT SET] (1 req/s, may be throttled)'}")
print(f"  REDIS_URL:     {'[SET]' if REDIS_URL else '[MISSING!]'}")
print(f"  DATABASE_URL:  {'[SET]' if DATABASE_URL else '[MISSING!]'}")
print(f"  SCAN_DAYS_BACK: {PULSE_SCAN_DAYS_BACK}")

if not NCBI_EMAIL:
    print("\n*** FATAL: NCBI_EMAIL is not set. PubMed E-Utilities REQUIRES this. ***")
    print("*** Add NCBI_EMAIL=your@email.com to your .env file ***")
    sys.exit(1)

# ── STEP 2: Check Redis circuit breaker ─────────────────────────
print("\n" + "=" * 60)
print("STEP 2: Redis — Circuit Breaker & Cache Check")
print("=" * 60)

redis_ok = False
try:
    import redis
    rc = redis.Redis.from_url(REDIS_URL)
    rc.ping()
    print("  Redis: connected OK")
    redis_ok = True

    # Check circuit breaker
    cb_blacklist = list(rc.scan_iter("pulse:cb:blacklist:*"))
    cb_errors = list(rc.scan_iter("pulse:cb:errors:*"))
    search_cache = list(rc.scan_iter("pulse:search:*"))
    article_cache = list(rc.scan_iter("pulse:article:*"))

    print(f"  Circuit breaker blacklists: {len(cb_blacklist)}")
    for k in cb_blacklist:
        ttl = rc.ttl(k)
        print(f"    -> {k.decode()} (expires in {ttl}s = {ttl//3600}h {(ttl%3600)//60}m)")

    print(f"  Circuit breaker error counters: {len(cb_errors)}")
    for k in cb_errors:
        val = rc.get(k)
        print(f"    -> {k.decode()} = {val.decode() if val else '?'} errors")

    print(f"  Cached E-Search queries: {len(search_cache)}")
    print(f"  Cached article metadata: {len(article_cache)}")

    if cb_blacklist:
        print("\n  *** PubMed is BLACKLISTED by circuit breaker! ***")
        print("  *** Clearing all circuit breaker keys now... ***")
        for k in cb_blacklist:
            rc.delete(k)
            print(f"    Deleted {k.decode()}")
        for k in cb_errors:
            rc.delete(k)
            print(f"    Deleted {k.decode()}")
        print("  Circuit breaker cleared!")

    # Also flush all pulse search caches to force fresh queries
    print("\n  Flushing all pulse search cache...")
    flushed = 0
    for k in search_cache:
        rc.delete(k)
        flushed += 1
    print(f"  Flushed {flushed} cached search results")

except ImportError:
    print("  WARNING: redis package not installed")
except Exception as e:
    print(f"  ERROR connecting to Redis: {e}")

# ── STEP 3: Direct PubMed E-Search test ─────────────────────────
print("\n" + "=" * 60)
print("STEP 3: Direct PubMed API Test (bypasses all app code)")
print("=" * 60)

from datetime import datetime, timedelta, timezone
end = datetime.now(timezone.utc)
start = end - timedelta(days=PULSE_SCAN_DAYS_BACK)
ds = start.strftime("%Y/%m/%d")
de = end.strftime("%Y/%m/%d")
print(f"  Date range: {ds} to {de}")

session = requests.Session()

def raw_pubmed_search(query_term, label):
    """Hit PubMed E-Search directly and return PMIDs."""
    params = {
        "tool": "secnd-test",
        "email": NCBI_EMAIL,
        "db": "pubmed",
        "term": query_term,
        "retmax": 10,
        "retmode": "json",
        "sort": "relevance",
        "datetype": "pdat",
        "mindate": ds,
        "maxdate": de,
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY

    try:
        resp = session.get(f"{PUBMED_BASE}/esearch.fcgi", params=params, timeout=30)
        print(f"\n  [{label}]")
        print(f"    HTTP status: {resp.status_code}")

        if resp.status_code != 200:
            print(f"    Response: {resp.text[:300]}")
            return []

        data = resp.json()
        result = data.get("esearchresult", {})
        pmids = result.get("idlist", [])
        count = result.get("count", "?")
        error = result.get("ERROR", "")
        warning = result.get("warninglist", {})

        if error:
            print(f"    *** PubMed ERROR: {error} ***")
        if warning:
            print(f"    PubMed warnings: {warning}")

        print(f"    Query: {query_term[:80]}...")
        print(f"    Total matches: {count}")
        print(f"    PMIDs returned: {len(pmids)}")
        if pmids:
            print(f"    First 5: {pmids[:5]}")
        return pmids

    except Exception as e:
        print(f"    *** REQUEST FAILED: {e} ***")
        return []


# Test with 3 very different queries
q_cardio = '("Cardiovascular Diseases"[MeSH Terms] OR "Heart Failure"[MeSH Terms]) AND ("SGLT2 inhibitors"[Title/Abstract])'
q_neuro = '("Nervous System Diseases"[MeSH Terms] OR "Stroke"[MeSH Terms]) AND ("thrombolysis"[Title/Abstract])'
q_onco = '("Neoplasms"[MeSH Terms] OR "Immunotherapy"[MeSH Terms]) AND ("checkpoint inhibitor"[Title/Abstract])'

time.sleep(0.5)  # rate limit
pmids_cardio = raw_pubmed_search(q_cardio, "Cardiology / SGLT2")
time.sleep(0.5)
pmids_neuro = raw_pubmed_search(q_neuro, "Neurology / thrombolysis")
time.sleep(0.5)
pmids_onco = raw_pubmed_search(q_onco, "Oncology / checkpoint inhibitor")

# ── STEP 4: Compare results ─────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 4: Result Comparison")
print("=" * 60)

if not pmids_cardio and not pmids_neuro and not pmids_onco:
    print("\n  *** ALL THREE SEARCHES RETURNED 0 RESULTS ***")
    print("  Possible causes:")
    print("    1. NCBI_EMAIL is set but invalid")
    print("    2. NCBI_API_KEY is set but invalid/expired")
    print("    3. PubMed is down or blocking your IP")
    print("    4. PULSE_SCAN_DAYS_BACK is too small (current: {})".format(PULSE_SCAN_DAYS_BACK))
    print("  Try increasing PULSE_SCAN_DAYS_BACK=30 in .env")
elif pmids_cardio == pmids_neuro == pmids_onco:
    print("\n  *** BUG: All 3 different queries returned IDENTICAL results ***")
    print("  This should never happen. PubMed may be ignoring the query.")
else:
    overlap_cn = set(pmids_cardio) & set(pmids_neuro)
    overlap_co = set(pmids_cardio) & set(pmids_onco)
    print(f"\n  Cardiology vs Neurology overlap: {len(overlap_cn)}/{max(len(pmids_cardio),1)} PMIDs")
    print(f"  Cardiology vs Oncology overlap:  {len(overlap_co)}/{max(len(pmids_cardio),1)} PMIDs")
    if not overlap_cn and not overlap_co:
        print("  GOOD: No overlap — different queries return different articles")
    print("\n  PubMed API is working correctly.")

# ── STEP 5: Check DB preferences ────────────────────────────────
print("\n" + "=" * 60)
print("STEP 5: Database — Check stored preferences")
print("=" * 60)

if DATABASE_URL:
    try:
        db_url = DATABASE_URL.replace("+asyncpg", "+psycopg2").replace("postgresql://", "postgresql+psycopg2://")
        if "psycopg2+psycopg2" in db_url:
            db_url = db_url.replace("psycopg2+psycopg2", "psycopg2")

        from sqlalchemy import create_engine, text
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # Check preferences
            rows = conn.execute(text(
                "SELECT pp.user_id, pp.specialty, pp.topics, pp.frequency, pp.is_enabled, u.email "
                "FROM pulse_preferences pp JOIN users u ON pp.user_id = u.id"
            )).fetchall()

            if not rows:
                print("  No pulse preferences found in DB!")
            else:
                for r in rows:
                    print(f"  User: {r[5]} (id: {str(r[0])[:8]}...)")
                    print(f"    Specialty: {r[1]}")
                    print(f"    Topics:    {r[2]}")
                    print(f"    Frequency: {r[3]}")
                    print(f"    Enabled:   {r[4]}")

            # Check recent digests
            digests = conn.execute(text(
                "SELECT id, status, article_count, specialty_used, topics_used, created_at "
                "FROM pulse_digests ORDER BY created_at DESC LIMIT 5"
            )).fetchall()

            print(f"\n  Recent digests ({len(digests)}):")
            for d in digests:
                print(f"    [{d[1]:10s}] {d[3] or '?':20s} | {d[4]} | {d[2]} articles | {d[5]}")

            # Check if recent digests have different articles
            if len(digests) >= 2 and digests[0][2] > 0 and digests[1][2] > 0:
                d1_id = digests[0][0]
                d2_id = digests[1][0]
                arts1 = conn.execute(text(
                    "SELECT pmid FROM pulse_articles WHERE digest_id = :did ORDER BY pmid"
                ), {"did": d1_id}).fetchall()
                arts2 = conn.execute(text(
                    "SELECT pmid FROM pulse_articles WHERE digest_id = :did ORDER BY pmid"
                ), {"did": d2_id}).fetchall()

                pmids1 = [a[0] for a in arts1]
                pmids2 = [a[0] for a in arts2]
                overlap = set(pmids1) & set(pmids2)
                print(f"\n  Comparing last 2 digests:")
                print(f"    Digest 1 ({str(d1_id)[:8]}): {len(pmids1)} articles, specialty={digests[0][3]}")
                print(f"    Digest 2 ({str(d2_id)[:8]}): {len(pmids2)} articles, specialty={digests[1][3]}")
                print(f"    Overlapping PMIDs: {len(overlap)}/{max(len(pmids1),1)}")
                if pmids1 and pmids1 == pmids2:
                    print("    *** IDENTICAL articles in both digests! ***")
                    if digests[0][3] == digests[1][3] and digests[0][4] == digests[1][4]:
                        print("    ...but specialty and topics are ALSO identical.")
                        print("    The preferences weren't actually changed between generations,")
                        print("    OR PubMed returns same top-10 for this query in this date range.")
                    else:
                        print("    ...specialty/topics DIFFER — this confirms a caching/pipeline bug")

        engine.dispose()
    except Exception as e:
        print(f"  DB check failed: {e}")
        import traceback
        traceback.print_exc()
else:
    print("  DATABASE_URL not set, skipping DB check")

# ── STEP 6: Test the actual app scanner (if possible) ───────────
print("\n" + "=" * 60)
print("STEP 6: App Scanner Test (uses app code path)")
print("=" * 60)

try:
    sys.path.insert(0, os.path.dirname(__file__))
    from app.pulse.scanner import build_pubmed_query, scan_for_articles, SPECIALTY_MESH

    q1 = build_pubmed_query("Cardiology", ["heart failure", "SGLT2"])
    q2 = build_pubmed_query("Neurology", ["stroke", "thrombolysis"])
    print(f"  Cardiology query: {q1[:80]}...")
    print(f"  Neurology query:  {q2[:80]}...")
    print(f"  Queries identical: {q1 == q2}")

    print("\n  Running scan_for_articles (Cardiology, skip_cache=True)...")
    arts1 = scan_for_articles("Cardiology", ["heart failure", "SGLT2"], skip_cache=True)
    print(f"  -> {len(arts1)} articles")
    for a in arts1[:3]:
        print(f"     {a.get('pmid', '?'):10s} | {a.get('title', '?')[:60]}")

    time.sleep(1)

    print("\n  Running scan_for_articles (Neurology, skip_cache=True)...")
    arts2 = scan_for_articles("Neurology", ["stroke", "thrombolysis"], skip_cache=True)
    print(f"  -> {len(arts2)} articles")
    for a in arts2[:3]:
        print(f"     {a.get('pmid', '?'):10s} | {a.get('title', '?')[:60]}")

    pmids1 = {a['pmid'] for a in arts1}
    pmids2 = {a['pmid'] for a in arts2}
    overlap = pmids1 & pmids2
    print(f"\n  Overlap: {len(overlap)}/{max(len(pmids1),1)}")

    if arts1 and arts2 and pmids1 == pmids2:
        print("  *** BUG CONFIRMED: scan_for_articles returns identical results for different inputs ***")
    elif not arts1 and not arts2:
        print("  *** Both returned 0 articles — check PULSE_SCAN_DAYS_BACK (try 30) ***")
    else:
        print("  GOOD: Different specialties return different articles through the app code")

except Exception as e:
    print(f"  App scanner test failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
