"""BSE corporate filings: announcements, corp actions, result calendar.

Revision ID: 016_bse_filings
Revises: 015_equity_fundamentals_history
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "016_bse_filings"
down_revision = "015_equity_fundamentals_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add bse_scripcode to de_instrument
    op.add_column(
        "de_instrument",
        sa.Column("bse_scripcode", sa.String(20), nullable=True),
    )
    op.create_index("ix_de_instrument_bse_scripcode", "de_instrument", ["bse_scripcode"], unique=True)

    # de_bse_announcements
    op.create_table(
        "de_bse_announcements",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id", sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("scripcode", sa.String(20), nullable=False),
        sa.Column("announcement_dt", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("headline", sa.Text, nullable=False),
        sa.Column("category", sa.String(100), nullable=True),
        sa.Column("subcategory", sa.String(100), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("attachment_url", sa.Text, nullable=True),
        sa.Column("dedup_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_bse_ann_instrument_id", "de_bse_announcements", ["instrument_id"])
    op.create_index("ix_bse_ann_inst_dt", "de_bse_announcements", ["instrument_id", sa.text("announcement_dt DESC")])
    op.create_index("ix_bse_ann_dt", "de_bse_announcements", [sa.text("announcement_dt DESC")])

    # de_bse_corp_actions
    op.create_table(
        "de_bse_corp_actions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id", sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("scripcode", sa.String(20), nullable=False),
        sa.Column("action_type", sa.String(30), nullable=False),
        sa.Column("ex_date", sa.Date, nullable=True),
        sa.Column("record_date", sa.Date, nullable=True),
        sa.Column("announced_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("purpose_code", sa.String(10), nullable=True),
        sa.Column("ratio", sa.Text, nullable=True),
        sa.Column("amount_per_share", sa.Numeric(18, 4), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("dedup_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_bse_ca_instrument_id", "de_bse_corp_actions", ["instrument_id"])
    op.create_index("ix_bse_ca_inst_ex", "de_bse_corp_actions", ["instrument_id", sa.text("ex_date DESC")])

    # de_bse_result_calendar
    op.create_table(
        "de_bse_result_calendar",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id", sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("scripcode", sa.String(20), nullable=False),
        sa.Column("result_date", sa.Date, nullable=False),
        sa.Column("period", sa.String(20), nullable=True),
        sa.Column("announced_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("dedup_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_bse_rc_instrument_id", "de_bse_result_calendar", ["instrument_id"])
    op.create_index("ix_bse_rc_inst_dt", "de_bse_result_calendar", ["instrument_id", "result_date"])


def downgrade() -> None:
    op.drop_table("de_bse_result_calendar")
    op.drop_table("de_bse_corp_actions")
    op.drop_table("de_bse_announcements")
    op.drop_index("ix_de_instrument_bse_scripcode", table_name="de_instrument")
    op.drop_column("de_instrument", "bse_scripcode")
