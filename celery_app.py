"""Celery configuration — per spec section 6.1."""

from celery import Celery
from app.config import settings

app = Celery("medsecondopinion")
app.config_from_object({
    "broker_url": settings.REDIS_URL,
    "result_backend": settings.REDIS_URL,
    "task_serializer": "json",
    "result_serializer": "json",
    "accept_content": ["json"],
    "task_track_started": True,
    "task_time_limit": 300,
    "task_soft_time_limit": 240,
    "task_routes": {
        "pipeline.analyze_case": {"queue": "medgemma_q"},
        "pipeline.clean_output": {"queue": "report_q"},
        "pipeline.validate_claims": {"queue": "gemini_q"},
        "pipeline.extract_claims": {"queue": "gemini_q"},
        "pipeline.search_evidence": {"queue": "search_q"},
        "pipeline.verify_citations": {"queue": "search_q"},
        "pipeline.synthesize_evidence": {"queue": "gemini_q"},
        "pipeline.storm_research": {"queue": "storm_q"},
        "pipeline.verify_storm_citations": {"queue": "search_q"},
        "pipeline.compile_report": {"queue": "report_q"},
        # Research pipeline v1
        "pipeline.research_generate_questions": {"queue": "gemini_q"},
        "pipeline.research_storm": {"queue": "storm_q"},
        "pipeline.research_compile_report": {"queue": "report_q"},
        # Research pipeline v2 (10-step)
        "pipeline.research_costorm": {"queue": "storm_q"},
        "pipeline.research_hallucination_guard": {"queue": "gemini_q"},
        "pipeline.research_extract_claims": {"queue": "gemini_q"},
        "pipeline.research_search_evidence": {"queue": "search_q"},
        "pipeline.research_verify_citations": {"queue": "search_q"},
        "pipeline.research_synthesize_evidence": {"queue": "gemini_q"},
        "pipeline.research_generate_summary": {"queue": "gemini_q"},
        "pipeline.research_compile_report_v2": {"queue": "report_q"},
    },
})

# Auto-discover tasks
app.autodiscover_tasks(["app.pipeline"])
