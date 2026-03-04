"""Pulse models — medical literature digest preferences, digests, and articles."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Integer, Float, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship

from ..db.database import Base


class PulsePreference(Base):
    __tablename__ = "pulse_preferences"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False, index=True)
    specialty = Column(String(100), nullable=False)
    topics = Column(JSONB, nullable=False, default=list)
    mesh_terms = Column(JSONB, nullable=True, default=list)
    frequency = Column(String(20), nullable=False, default="weekly")  # "daily" or "weekly"
    is_enabled = Column(Boolean, default=True)
    enabled_journals = Column(JSONB, nullable=True, default=list)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))



class PulseDigest(Base):
    __tablename__ = "pulse_digests"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(20), nullable=False, default="pending")  # pending/generating/complete/failed
    article_count = Column(Integer, default=0)
    specialty_used = Column(String(100), nullable=True)
    topics_used = Column(JSONB, nullable=True)
    date_range_start = Column(DateTime(timezone=True), nullable=True)
    date_range_end = Column(DateTime(timezone=True), nullable=True)
    error_message = Column(Text, nullable=True)
    generated_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    articles = relationship("PulseArticle", back_populates="digest", cascade="all, delete-orphan")


class PulseArticle(Base):
    __tablename__ = "pulse_articles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    digest_id = Column(UUID(as_uuid=True), ForeignKey("pulse_digests.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(Text, nullable=False)
    authors = Column(JSONB, nullable=True, default=list)
    journal = Column(String(255), nullable=True)
    doi = Column(String(255), nullable=True)
    pmid = Column(String(20), nullable=True)
    published_date = Column(DateTime(timezone=True), nullable=True)
    abstract = Column(Text, nullable=True)
    article_url = Column(Text, nullable=True)
    tldr = Column(Text, nullable=True)
    evidence_grade = Column(String(50), nullable=True)
    relevance_score = Column(Float, nullable=True)
    source = Column(String(50), nullable=True, default="pubmed")
    access_strategy = Column(String(50), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    digest = relationship("PulseDigest", back_populates="articles")
