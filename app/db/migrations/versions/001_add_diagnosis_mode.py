"""Add diagnosis_mode column to cases table.

Revision ID: 001_add_diagnosis_mode
Create Date: 2026-02-23
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "001_add_diagnosis_mode"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cases",
        sa.Column("diagnosis_mode", sa.String(20), server_default="standard", nullable=True),
    )


def downgrade() -> None:
    op.drop_column("cases", "diagnosis_mode")
