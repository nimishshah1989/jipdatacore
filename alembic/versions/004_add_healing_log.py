"""Add de_healing_log table for self-healing agent tracking.

Revision ID: 004_healing_log
Revises: 003_goldilocks
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa

revision = "004_healing_log"
down_revision = "003_goldilocks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_healing_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("date", sa.Date, nullable=False, index=True),
        sa.Column("stream_id", sa.String(100), nullable=False),
        sa.Column("pipeline_triggered", sa.String(100), nullable=False),
        sa.Column("action", sa.String(50), nullable=False),
        sa.Column("result", sa.String(50), nullable=False),
        sa.Column("retries", sa.Integer, nullable=False, server_default="0"),
        sa.Column("error_detail", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("de_healing_log")
