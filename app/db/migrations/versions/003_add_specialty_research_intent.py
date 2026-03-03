"""Add specialty and research_intent columns to cases table.

Revision ID: 003_add_specialty_research_intent
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "003_add_specialty_research_intent"
down_revision = "002_add_verification_stats"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cases",
        sa.Column("specialty", sa.String(100), nullable=True),
    )
    op.add_column(
        "cases",
        sa.Column("research_intent", sa.String(50), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("cases", "research_intent")
    op.drop_column("cases", "specialty")
