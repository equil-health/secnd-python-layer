import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from ..db.database import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    case_id = Column(UUID(as_uuid=True), ForeignKey("cases.id", ondelete="CASCADE"), nullable=False)

    # Execution state
    current_step = Column(Integer, nullable=False, default=0)
    total_steps = Column(Integer, nullable=False, default=10)
    status = Column(String(20), nullable=False, default="queued")
    error_message = Column(Text)

    # Step details
    steps = Column(JSONB, nullable=False, default=list)

    # Timing
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    case = relationship("Case", back_populates="pipeline_runs")


class Report(Base):
    __tablename__ = "reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    case_id = Column(UUID(as_uuid=True), ForeignKey("cases.id", ondelete="CASCADE"), nullable=False)
    pipeline_run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.id"))

    # Raw outputs from each stage
    medgemma_raw = Column(Text)
    medgemma_clean = Column(Text)
    hallucination_check = Column(JSONB)
    extracted_claims = Column(JSONB)
    evidence_results = Column(JSONB)
    evidence_synthesis = Column(Text)
    storm_article_raw = Column(Text)
    storm_article_clean = Column(Text)
    storm_url_to_info = Column(JSONB)

    # Compiled outputs
    references = Column(JSONB)
    executive_summary = Column(Text)
    report_markdown = Column(Text)
    report_html = Column(Text)

    # File references (GCS paths)
    pdf_path = Column(String(500))
    docx_path = Column(String(500))

    # Stats
    total_sources = Column(Integer, default=0)
    total_claims = Column(Integer, default=0)
    verification_stats = Column(JSONB)
    primary_diagnosis = Column(String(200))

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    case = relationship("Case", back_populates="reports")


class FollowUp(Base):
    __tablename__ = "followups"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    case_id = Column(UUID(as_uuid=True), ForeignKey("cases.id", ondelete="CASCADE"), nullable=False)
    report_id = Column(UUID(as_uuid=True), ForeignKey("reports.id", ondelete="CASCADE"), nullable=False)

    question = Column(Text, nullable=False)
    answer = Column(Text, nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    case = relationship("Case", back_populates="followups")
