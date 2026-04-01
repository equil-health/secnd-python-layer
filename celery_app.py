"""Celery configuration — per spec section 6.1."""

from celery import Celery
from celery.schedules import crontab
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
        # Pulse — medical literature digest (legacy)
        "pulse.generate_all_digests": {"queue": "pulse_q"},
        "pulse.generate_pulse_digest": {"queue": "pulse_q"},
        # Breaking — daily headline pipeline (Pulse v2)
        "breaking.daily_refresh": {"queue": "breaking_q"},
        "breaking.reset_monthly_free_reports": {"queue": "breaking_q"},
        # SDSS — async second opinion via GPU pod
        "sdss.run_analysis": {"queue": "sdss_q"},
        "sdss.poll_gpu_result": {"queue": "sdss_q"},
    },
    "beat_schedule": {
        "pulse-daily-digests": {
            "task": "pulse.generate_all_digests",
            "schedule": crontab(hour=6, minute=0),
            "kwargs": {"frequency_filter": "daily"},
        },
        "pulse-weekly-digests": {
            "task": "pulse.generate_all_digests",
            "schedule": crontab(hour=6, minute=0, day_of_week="monday"),
            "kwargs": {"frequency_filter": "weekly"},
        },
        # Breaking: daily refresh at 05:00 IST (23:30 UTC previous day)
        "breaking-daily-refresh": {
            "task": "breaking.daily_refresh",
            "schedule": crontab(hour=23, minute=30),
            "options": {"queue": "breaking_q"},
        },
        # Monthly free report reset (1st of month, 00:01 IST)
        "breaking-monthly-reset": {
            "task": "breaking.reset_monthly_free_reports",
            "schedule": crontab(hour=18, minute=31, day_of_month=1),
            "options": {"queue": "breaking_q"},
        },
    },
})

# Auto-discover tasks
app.autodiscover_tasks(["app.pipeline", "app.pulse", "app.breaking", "app.sdss"])
