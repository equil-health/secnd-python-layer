"""Add verification_stats JSONB column to reports table.

Revision ID: 002_add_verification_stats
Create Date: 2026-02-23
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers
revision = "002_add_verification_stats"
down_revision = "001_add_diagnosis_mode"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reports",
        sa.Column("verification_stats", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("reports", "verification_stats")
