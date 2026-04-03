"""SdssTask model — async second opinion tasks dispatched to GPU pod."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB

from ..db.database import Base


class SdssTask(Base):
    __tablename__ = "sdss_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    case_text = Column(Text, nullable=False)
    mode = Column(String(20), nullable=False, default="standard")  # standard / zebra / medgemma
    india_context = Column(Boolean, nullable=False, default=False)
    pod_task_id = Column(String(100), nullable=True)  # GPU pod's own task ID
    images = Column(JSONB, nullable=True)  # [{filename, content_type, base64}]
    status = Column(String(20), nullable=False, default="pending")  # pending / processing / complete / failed
    result = Column(JSONB, nullable=True)
    audit_report = Column(JSONB, nullable=True)  # Full pipeline audit trail from GPU pod
    error = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)
