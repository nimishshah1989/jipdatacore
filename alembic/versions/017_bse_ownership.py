"""BSE ownership: shareholding, pledge, insider trades, SAST disclosures.

Revision ID: 017_bse_ownership
Revises: 016_bse_filings
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "017_bse_ownership"
down_revision = "016_bse_filings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_bse_shareholding",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("scripcode", sa.String(20), nullable=False),
        sa.Column("quarter_end", sa.Date, nullable=False),
        sa.Column("promoter_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("promoter_pledged_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("public_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("fii_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("dii_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("insurance_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("mutual_funds_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("retail_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("body_corporate_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("total_shareholders", sa.Integer, nullable=True),
        sa.Column("raw_json", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("instrument_id", "quarter_end", name="uq_bse_sh_inst_qtr"),
    )

    op.create_table(
        "de_bse_pledge_history",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("promoter_holding_qty", sa.BigInteger, nullable=True),
        sa.Column("promoter_pledged_qty", sa.BigInteger, nullable=True),
        sa.Column("pledged_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("total_shares", sa.BigInteger, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("instrument_id", "as_of_date", name="uq_bse_pledge_inst_dt"),
    )

    op.create_table(
        "de_bse_insider_trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("filer_name", sa.String(200), nullable=True),
        sa.Column("filer_category", sa.String(50), nullable=True),
        sa.Column("transaction_type", sa.String(20), nullable=True),
        sa.Column("qty", sa.BigInteger, nullable=True),
        sa.Column("value_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("transaction_date", sa.Date, nullable=True),
        sa.Column("acquisition_mode", sa.String(50), nullable=True),
        sa.Column("intimation_date", sa.Date, nullable=True),
        sa.Column("dedup_hash", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_bse_insider_inst_dt",
        "de_bse_insider_trades",
        ["instrument_id", sa.text("transaction_date DESC")],
    )

    op.create_table(
        "de_bse_sast_disclosures",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "instrument_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("acquirer_name", sa.String(300), nullable=True),
        sa.Column("acquirer_type", sa.String(50), nullable=True),
        sa.Column("pre_holding_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("post_holding_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("delta_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("transaction_date", sa.Date, nullable=True),
        sa.Column("disclosure_date", sa.Date, nullable=True),
        sa.Column("regulation", sa.String(50), nullable=True),
        sa.Column("dedup_hash", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_bse_sast_inst_dt",
        "de_bse_sast_disclosures",
        ["instrument_id", sa.text("disclosure_date DESC")],
    )


def downgrade() -> None:
    op.drop_table("de_bse_sast_disclosures")
    op.drop_table("de_bse_insider_trades")
    op.drop_table("de_bse_pledge_history")
    op.drop_table("de_bse_shareholding")
