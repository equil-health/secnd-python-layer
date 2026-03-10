"""Add usage_log table for API call tracking and auditing.

Revision ID: 007_add_usage_log
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "007_add_usage_log"
down_revision = "006_add_breaking_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usage_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        # Who
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("case_id", postgresql.UUID(as_uuid=True), nullable=True),
        # What
        sa.Column("module", sa.String(50), nullable=False),    # pipeline, breaking, pulse, research, admin
        sa.Column("service", sa.String(50), nullable=False),   # gemini, medgemma, serper, openalex, pubmed, embedding, crossref
        sa.Column("operation", sa.String(100), nullable=False), # call_gemini, search_serper, verify_citations, etc.
        # Request details
        sa.Column("request_summary", sa.Text(), nullable=True), # truncated prompt / query
        sa.Column("model", sa.String(100), nullable=True),      # gemini-2.5-flash, medgemma, text-embedding-004
        # Response details
        sa.Column("status", sa.String(20), nullable=False, server_default="success"),  # success, error, timeout, rate_limited
        sa.Column("error_message", sa.Text(), nullable=True),
        # Metrics
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("input_chars", sa.Integer(), nullable=True),
        sa.Column("output_chars", sa.Integer(), nullable=True),
        sa.Column("num_results", sa.Integer(), nullable=True),  # search results count, refs verified, etc.
        # Cost estimation
        sa.Column("estimated_cost_usd", sa.Float(), nullable=True),
        # Extra metadata
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
    )

    # Indexes for common queries
    op.create_index("idx_usage_timestamp", "usage_log", ["timestamp"])
    op.create_index("idx_usage_module_service", "usage_log", ["module", "service"])
    op.create_index("idx_usage_case_id", "usage_log", ["case_id"])
    op.create_index("idx_usage_user_id", "usage_log", ["user_id"])
    op.create_index("idx_usage_status", "usage_log", ["status"])


def downgrade() -> None:
    op.drop_table("usage_log")
