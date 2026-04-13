"""Add de_cron_run table for loud cron-run visibility.

Revision ID: 005_cron_run
Revises: 004_healing_log
Create Date: 2026-04-13
"""

from alembic import op
import sqlalchemy as sa

revision = "005_cron_run"
down_revision = "004_healing_log"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_cron_run",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("schedule_name", sa.String(100), nullable=False, index=True),
        sa.Column("business_date", sa.Date, nullable=True, index=True),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False, index=True),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Numeric(10, 3), nullable=True),
        sa.Column("http_code", sa.Integer, nullable=True),
        sa.Column("curl_exit_code", sa.Integer, nullable=True),
        sa.Column("status", sa.String(20), nullable=False),  # started|success|failed|timeout
        sa.Column("error_body", sa.Text, nullable=True),
        sa.Column("host", sa.String(100), nullable=True),
    )
    op.create_index(
        "ix_de_cron_run_sched_started",
        "de_cron_run",
        ["schedule_name", "started_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_de_cron_run_sched_started", table_name="de_cron_run")
    op.drop_table("de_cron_run")
