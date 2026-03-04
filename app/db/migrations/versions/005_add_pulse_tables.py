"""Add Pulse tables: pulse_preferences, pulse_digests, pulse_articles.

Revision ID: 005_add_pulse_tables
Create Date: 2026-03-04
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "005_add_pulse_tables"
down_revision = "004_add_users_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pulse_preferences",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"),
                  unique=True, nullable=False, index=True),
        sa.Column("specialty", sa.String(100), nullable=False),
        sa.Column("topics", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("mesh_terms", postgresql.JSONB(), nullable=True, server_default="[]"),
        sa.Column("frequency", sa.String(20), nullable=False, server_default="weekly"),
        sa.Column("is_enabled", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("enabled_journals", postgresql.JSONB(), nullable=True, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "pulse_digests",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="CASCADE"),
                  nullable=False, index=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("article_count", sa.Integer(), server_default=sa.text("0")),
        sa.Column("specialty_used", sa.String(100), nullable=True),
        sa.Column("topics_used", postgresql.JSONB(), nullable=True),
        sa.Column("date_range_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date_range_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "pulse_articles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("digest_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("pulse_digests.id", ondelete="CASCADE"),
                  nullable=False, index=True),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("authors", postgresql.JSONB(), nullable=True, server_default="[]"),
        sa.Column("journal", sa.String(255), nullable=True),
        sa.Column("doi", sa.String(255), nullable=True),
        sa.Column("pmid", sa.String(20), nullable=True),
        sa.Column("published_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("abstract", sa.Text(), nullable=True),
        sa.Column("article_url", sa.Text(), nullable=True),
        sa.Column("tldr", sa.Text(), nullable=True),
        sa.Column("evidence_grade", sa.String(50), nullable=True),
        sa.Column("relevance_score", sa.Float(), nullable=True),
        sa.Column("source", sa.String(50), nullable=True, server_default="pubmed"),
        sa.Column("access_strategy", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("pulse_articles")
    op.drop_table("pulse_digests")
    op.drop_table("pulse_preferences")
