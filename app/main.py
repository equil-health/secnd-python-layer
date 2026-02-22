"""FastAPI app — main entry point."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .db.database import init_db
from .api.routes_cases import router as cases_router
from .api.routes_reports import router as reports_router
from .api.routes_upload import router as upload_router
from .api.routes_research import router as research_router
from .api.routes_audio import router as audio_router
from .api.websocket import ws_pipeline_status


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize database tables
    await init_db()
    yield
    # Shutdown: nothing to clean up


app = FastAPI(
    title="MedSecondOpinion",
    description="AI-powered medical second opinion pipeline",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REST routes
app.include_router(cases_router)
app.include_router(reports_router)
app.include_router(upload_router)
app.include_router(research_router)
app.include_router(audio_router)

# WebSocket
app.websocket("/ws/cases/{case_id}/status")(ws_pipeline_status)


@app.get("/health")
async def health():
    return {"status": "ok"}
