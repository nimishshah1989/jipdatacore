"""Atlas-M0: de_etf_holdings — ETF portfolio holdings disclosure.

Adds the de_etf_holdings table for thematic ETF dominant-sector
classification (Atlas-M5). Sourced from Morningstar Direct via the same
service that already feeds de_mf_holdings; refreshed monthly.

Per-ATLAS_M0 spec:
  ticker (FK de_etf_master.ticker, String(30) — see note below)
  instrument_id (FK de_instrument.id, UUID)
  weight (Numeric(8,6) — decimal: 0.0512 = 5.12%)
  as_of_date / last_disclosed_date (DATE)
  PK (ticker, instrument_id, as_of_date)

Note on ticker length: the spec writes VARCHAR(32) but the canonical ETF
master (de_etf_master.ticker) is String(30) and is the FK target — we use
String(30) here to match. Same character ceiling in practice.

Revision ID: 019_atlas_m0_etf_holdings
Revises: 018_bse_ownership
Create Date: 2026-05-04
"""

from alembic import op
import sqlalchemy as sa

revision = "019_atlas_m0_etf_holdings"
down_revision = "018_bse_ownership"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_etf_holdings",
        sa.Column(
            "ticker",
            sa.String(30),
            sa.ForeignKey("de_etf_master.ticker", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "instrument_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("weight", sa.Numeric(8, 6), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("last_disclosed_date", sa.Date, nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint(
            "ticker", "instrument_id", "as_of_date", name="pk_de_etf_holdings"
        ),
        sa.CheckConstraint(
            "weight >= 0 AND weight <= 1",
            name="chk_etf_holdings_weight_range",
        ),
    )

    op.create_index(
        "idx_de_etf_holdings_ticker_date",
        "de_etf_holdings",
        ["ticker", sa.text("as_of_date DESC")],
    )
    op.create_index(
        "idx_de_etf_holdings_instrument",
        "de_etf_holdings",
        ["instrument_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_de_etf_holdings_instrument", table_name="de_etf_holdings")
    op.drop_index("idx_de_etf_holdings_ticker_date", table_name="de_etf_holdings")
    op.drop_table("de_etf_holdings")
