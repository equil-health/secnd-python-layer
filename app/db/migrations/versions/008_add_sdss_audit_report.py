"""Add audit_report JSONB column to sdss_tasks table.

Stores the full pipeline audit trail from the GPU pod, including
per-stage timings, raw outputs, token counts, and cost estimates.

Revision ID: 008_add_sdss_audit_report
Create Date: 2026-04-03
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "008_add_sdss_audit_report"
down_revision = "007_add_usage_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sdss_tasks",
        sa.Column("audit_report", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("sdss_tasks", "audit_report")
