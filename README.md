# SECND Backend

FastAPI + Celery backend for the SECND medical second opinion platform. Orchestrates a multi-stage AI pipeline that analyzes clinical cases, verifies claims against medical literature, and compiles evidence-backed reports.

## Architecture

```
FastAPI (async)  ──>  Celery (5 queues)  ──>  Redis (pub/sub + cache)
      │                     │                        │
      ├─ REST API           ├─ MedGemma (Vertex AI)  ├─ WebSocket relay
      ├─ WebSocket          ├─ Gemini Flash          └─ Search cache
      └─ File upload        ├─ Serper.dev search
                            ├─ OpenAlex verification
                            ├─ STORM deep research
                            └─ Report compilation
                                    │
                              PostgreSQL (cases, reports)
```

## Pipelines

### Diagnosis Pipeline (10 steps)

| Step | Component | What it does |
|------|-----------|-------------|
| 1 | Case accepted | Save to DB, queue pipeline |
| 2 | MedGemma | Clinical second opinion analysis |
| 3 | Dedup + Format | Remove MedGemma chain-of-thought repetition |
| 4 | Hallucination Guard | Gemini validates tests/antibodies/guidelines |
| 5 | Claim Extractor | Gemini extracts verifiable clinical claims |
| 6 | Serper Search | Search medical literature for each claim |
| 7 | OpenAlex Verify | Verify citations against 250M+ scholarly works |
| 8 | Evidence Synthesis | Gemini synthesizes evidence for/against claims |
| 9 | STORM Research | Deep multi-perspective literature review |
| 10 | Report Compiler | Markdown + HTML + executive summary |

**Modes:** Standard (differential diagnosis) and Zebra (rare disease discovery).

### Research Pipeline (4 steps)

Topic analysis, STORM deep research, and report compilation. No patient data or MedGemma needed.

## Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Redis 7+
- GCP service account with Vertex AI access (MedGemma + Gemini endpoints)

## Setup

```bash
cd script/backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials (see Configuration below)

# Run database migrations
alembic upgrade head
```

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Required | Description |
|----------|----------|-------------|
| `GCP_PROJECT_ID` | Yes | Google Cloud project ID |
| `GCP_LOCATION` | Yes | Vertex AI region (e.g. `europe-west4`) |
| `GCP_SERVICE_ACCOUNT_FILE` | Yes | Path to GCP service account JSON |
| `MEDGEMMA_ENDPOINT_ID` | Yes | MedGemma dedicated endpoint ID |
| `MEDGEMMA_DEDICATED_DOMAIN` | Yes | MedGemma endpoint domain |
| `SERPER_API_KEY` | Yes | Serper.dev API key for medical search |
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `REDIS_URL` | Yes | Redis connection string |
| `GOOGLE_SEARCH_API_KEY` | No | Google Custom Search API key (STORM fallback) |
| `GOOGLE_CSE_ID` | No | Google Custom Search Engine ID |
| `OPENALEX_EMAIL` | No | Email for polite OpenAlex API access |

## Running

You need three processes running simultaneously:

```bash
# Terminal 1: FastAPI server
uvicorn app.main:app --reload --port 8000

# Terminal 2: Celery worker (all queues)
celery -A celery_app worker --loglevel=info -Q medgemma_q,gemini_q,search_q,storm_q,report_q

# Terminal 3: Redis (if not running as a service)
redis-server
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/cases` | Submit structured case for diagnosis |
| `POST` | `/api/cases/submit-with-files` | Submit case with file attachments |
| `POST` | `/api/cases/parse` | Parse free-text into structured fields |
| `POST` | `/api/cases/audio` | Submit audio for MedASR transcription |
| `POST` | `/api/research` | Submit research topic |
| `GET` | `/api/cases` | List all cases (paginated) |
| `GET` | `/api/cases/{id}` | Get case with pipeline status |
| `GET` | `/api/cases/{id}/report` | Get compiled report (JSON) |
| `GET` | `/api/cases/{id}/report/html` | Styled standalone HTML |
| `GET` | `/api/cases/{id}/report/pdf` | Download PDF |
| `GET` | `/api/cases/{id}/report/docx` | Download DOCX |
| `POST` | `/api/cases/{id}/followup` | Ask follow-up question |
| `WS` | `/ws/cases/{id}/status` | Real-time pipeline progress |
| `GET` | `/health` | Health check |

See [API_DOCS.md](API_DOCS.md) for full request/response schemas and examples.

## Project Structure

```
backend/
├── celery_app.py              # Celery config (5 task queues)
├── requirements.txt           # Python dependencies
├── .env.example               # Environment template
├── alembic.ini                # DB migration config
├── API_DOCS.md                # Full API reference
└── app/
    ├── main.py                # FastAPI app, CORS, route registration
    ├── config.py              # Pydantic Settings (reads .env)
    ├── api/
    │   ├── routes_cases.py    # Case CRUD + submission
    │   ├── routes_reports.py  # Report retrieval + export + follow-ups
    │   ├── routes_upload.py   # File upload handling
    │   ├── routes_research.py # Research pipeline endpoint
    │   ├── routes_audio.py    # Audio submission (MedASR)
    │   └── websocket.py       # WebSocket manager + Redis pub/sub relay
    ├── db/
    │   ├── database.py        # Async SQLAlchemy engine + session
    │   └── migrations/        # Alembic migration versions
    ├── models/
    │   ├── case.py            # Case + CaseAttachment models
    │   ├── report.py          # Report + PipelineRun + FollowUp models
    │   └── schemas.py         # Pydantic request/response schemas
    ├── pipeline/
    │   ├── tasks.py           # Celery task chain orchestration
    │   ├── medgemma.py        # MedGemma Vertex AI client
    │   ├── gemini.py          # Gemini Flash Vertex AI client
    │   ├── claim_extractor.py # Extract verifiable claims via Gemini
    │   ├── hallucination_guard.py # Fact-check MedGemma via Gemini
    │   ├── evidence_verifier.py   # Evidence synthesis via Gemini
    │   ├── serper.py          # Serper.dev search + Redis cache
    │   ├── openalex.py        # OpenAlex citation verifier
    │   ├── storm_runner.py    # STORM framework wrapper
    │   ├── medasr.py          # Medical speech recognition
    │   ├── audio_structurer.py # Audio transcript structuring
    │   └── file_processor.py  # PDF/DOCX/image text extraction
    ├── postprocess/
    │   ├── dedup.py           # MedGemma repetition removal
    │   ├── formatter.py       # Wall-of-text to markdown
    │   ├── citation_mapper.py # Unified bibliography + citation remapping
    │   ├── junk_filter.py     # Filter non-URL references
    │   ├── storm_dedup.py     # STORM section deduplication
    │   ├── summarizer.py      # Executive summary generation
    │   ├── report_compiler.py # Standard + zebra report compilation
    │   └── research_report_compiler.py # Research report compilation
    └── export/
        ├── pdf_export.py      # HTML to PDF (xhtml2pdf)
        └── docx_export.py     # Markdown to DOCX (python-docx)
```

## Celery Task Queues

| Queue | Tasks |
|-------|-------|
| `medgemma_q` | MedGemma clinical analysis |
| `gemini_q` | Claim extraction, hallucination guard, evidence synthesis |
| `search_q` | Serper search, OpenAlex citation verification |
| `storm_q` | STORM deep research |
| `report_q` | Output cleaning, report compilation |

## Kaggle Notebook

A standalone notebook that runs the full pipeline on Kaggle's free GPU is available at [`docs/secnd_pipeline.ipynb`](../docs/secnd_pipeline.ipynb). It replaces Vertex AI with local HuggingFace inference and removes the need for PostgreSQL/Redis/Celery.
