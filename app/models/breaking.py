"""Breaking pipeline models — headlines, doctor preferences, read tracking."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Integer, Float, Text, Date, DateTime,
    Boolean, ForeignKey,
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.orm import relationship

from ..db.database import Base


class BreakingHeadline(Base):
    __tablename__ = "breaking_headlines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    date = Column(Date, nullable=False, index=True)
    specialty = Column(String(100), nullable=False)
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    source = Column(String(200), nullable=True)
    snippet = Column(Text, nullable=True)
    urgency_tier = Column(String(10), nullable=False, default="NEW")
    urgency_reason = Column(Text, nullable=True)
    rank_score = Column(Integer, default=50)
    rank_position = Column(Integer, default=0)
    research_topic = Column(Text, nullable=True)
    published_at = Column(String(100), nullable=True)

    # OpenAlex verification (v5.0)
    is_verified = Column(Boolean, default=False)
    citation_count = Column(Integer, nullable=True)
    quality_tier = Column(String(20), nullable=True)
    is_retracted = Column(Boolean, default=False)

    created_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc))

    # Relationships
    reads = relationship("BreakingRead", back_populates="headline",
                         cascade="all, delete-orphan")


class DoctorPreferences(Base):
    __tablename__ = "doctor_preferences"

    doctor_id = Column(UUID(as_uuid=True),
                       ForeignKey("users.id", ondelete="CASCADE"),
                       primary_key=True)
    specialties = Column(ARRAY(String(100)), nullable=False, default=list)
    breaking_enabled = Column(Boolean, nullable=False, default=True)

    # Trial management
    trial_started_at = Column(DateTime(timezone=True), nullable=True)
    trial_ends_at = Column(DateTime(timezone=True), nullable=True)
    free_reports_used = Column(Integer, nullable=False, default=0)
    free_reports_limit = Column(Integer, nullable=False, default=4)
    free_reports_reset = Column(Date, nullable=True)
    subscription_tier = Column(String(50), nullable=True)

    # Push notifications
    push_token = Column(Text, nullable=True)
    push_platform = Column(String(10), nullable=True)  # "ios" or "android"

    # v7.0: Doctor-declared free-text topics with Gemini-expanded search queries
    # Structure: {"Cardiology": [{"topic_text": "...", "generated_queries": [...]}, ...], ...}
    specialty_topics = Column(JSONB, nullable=True, default=dict)

    created_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class BreakingRead(Base):
    __tablename__ = "breaking_reads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    doctor_id = Column(UUID(as_uuid=True),
                       ForeignKey("users.id", ondelete="CASCADE"),
                       nullable=False)
    headline_id = Column(UUID(as_uuid=True),
                         ForeignKey("breaking_headlines.id", ondelete="CASCADE"),
                         nullable=False)
    read_at = Column(DateTime(timezone=True), nullable=False,
                     default=lambda: datetime.now(timezone.utc))
    action = Column(String(50), nullable=False)  # 'opened' | 'deep_research' | 'dismissed'
    case_id = Column(UUID(as_uuid=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False,
                        default=lambda: datetime.now(timezone.utc))

    # Relationships
    headline = relationship("BreakingHeadline", back_populates="reads")
