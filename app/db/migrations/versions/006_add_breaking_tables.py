"""Add Breaking pipeline tables + pgvector extension.

Tables: breaking_headlines, doctor_preferences, breaking_reads, medical_topic_embeddings

Revision ID: 006_add_breaking_tables
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers
revision = "006_add_breaking_tables"
down_revision = "005_add_pulse_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension (requires superuser — run separately if needed)
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # ── breaking_headlines ──────────────────────────────────────────
    op.create_table(
        "breaking_headlines",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("specialty", sa.String(100), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("source", sa.String(200), nullable=True),
        sa.Column("snippet", sa.Text(), nullable=True),
        sa.Column("urgency_tier", sa.String(10), nullable=False, server_default="NEW"),
        sa.Column("urgency_reason", sa.Text(), nullable=True),
        sa.Column("rank_score", sa.Integer(), server_default=sa.text("50")),
        sa.Column("rank_position", sa.Integer(), server_default=sa.text("0")),
        sa.Column("research_topic", sa.Text(), nullable=True),
        sa.Column("published_at", sa.String(100), nullable=True),
        # OpenAlex verification fields (v5.0)
        sa.Column("is_verified", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("citation_count", sa.Integer(), nullable=True),
        sa.Column("quality_tier", sa.String(20), nullable=True),
        sa.Column("is_retracted", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    # Indexes for breaking_headlines
    op.create_index("idx_breaking_date_specialty", "breaking_headlines", ["date", "specialty"])
    op.create_index("idx_breaking_urgency", "breaking_headlines", ["date", "urgency_tier"])
    op.create_index("idx_breaking_rank", "breaking_headlines",
                    ["date", "specialty", "rank_position"])

    # ── doctor_preferences ──────────────────────────────────────────
    op.create_table(
        "doctor_preferences",
        sa.Column("doctor_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("specialties", postgresql.ARRAY(sa.String(100)), nullable=False,
                  server_default="{}"),
        sa.Column("breaking_enabled", sa.Boolean(), nullable=False,
                  server_default=sa.text("true")),
        sa.Column("trial_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("free_reports_used", sa.Integer(), nullable=False,
                  server_default=sa.text("0")),
        sa.Column("free_reports_limit", sa.Integer(), nullable=False,
                  server_default=sa.text("4")),
        sa.Column("free_reports_reset", sa.Date(), nullable=True),
        sa.Column("subscription_tier", sa.String(50), nullable=True),
        sa.Column("push_token", sa.Text(), nullable=True),
        sa.Column("push_platform", sa.String(10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    # ── breaking_reads ──────────────────────────────────────────────
    op.create_table(
        "breaking_reads",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("doctor_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("headline_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("breaking_headlines.id", ondelete="CASCADE"), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("action", sa.String(50), nullable=False),
        # case_id for deep_research actions
        sa.Column("case_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )

    op.create_index("idx_breaking_reads_doctor", "breaking_reads",
                    ["doctor_id", sa.text("read_at DESC")])
    op.create_index("idx_breaking_reads_headline", "breaking_reads",
                    ["headline_id", "action"])

    # ── medical_topic_embeddings ────────────────────────────────────
    op.create_table(
        "medical_topic_embeddings",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("topic", sa.Text(), nullable=False, unique=True),
        sa.Column("specialty", sa.String(100), nullable=True),
    )

    # Add vector columns separately (pgvector types via raw SQL)
    op.execute("""
        ALTER TABLE medical_topic_embeddings
        ADD COLUMN embedding vector(768) NOT NULL
    """)
    op.execute("""
        CREATE INDEX ON medical_topic_embeddings
        USING hnsw (embedding vector_cosine_ops)
    """)


def downgrade() -> None:
    op.drop_table("medical_topic_embeddings")
    op.drop_table("breaking_reads")
    op.drop_table("doctor_preferences")
    op.drop_table("breaking_headlines")
    op.execute("DROP EXTENSION IF EXISTS vector")
