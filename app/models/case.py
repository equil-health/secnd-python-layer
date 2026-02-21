import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Integer, Text, DateTime
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from ..db.database import Base


class Case(Base):
    __tablename__ = "cases"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Patient data (structured)
    patient_age = Column(Integer)
    patient_sex = Column(String(20))
    patient_ethnicity = Column(String(100))
    presenting_complaint = Column(Text, nullable=False)
    medical_history = Column(Text)
    medications = Column(Text)
    physical_exam = Column(Text)
    lab_results = Column(JSONB)
    imaging_reports = Column(Text)
    referring_diagnosis = Column(Text)
    specific_question = Column(Text)
    raw_case_text = Column(Text)

    # Status
    status = Column(String(20), nullable=False, default="submitted")

    # Metadata
    user_id = Column(UUID(as_uuid=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    # Relationships
    pipeline_runs = relationship("PipelineRun", back_populates="case", cascade="all, delete-orphan")
    reports = relationship("Report", back_populates="case", cascade="all, delete-orphan")
    followups = relationship("FollowUp", back_populates="case", cascade="all, delete-orphan")
