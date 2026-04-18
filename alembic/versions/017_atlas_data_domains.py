"""Atlas data domains — F&O, macro, flows, filings.

Adds 9 new tables covering: F&O bhavcopy, F&O ban list, participant OI,
G-Sec yields, RBI FX reference rates, RBI policy rates, insider trades,
bulk/block deals, and shareholding pattern.

Revision ID: 017_atlas
Revises: 016_bse_filings
Create Date: 2026-04-18
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "017_atlas"
down_revision = "016_bse_filings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── de_fo_bhavcopy ────────────────────────────────────────────────────
    op.create_table(
        "de_fo_bhavcopy",
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("symbol", sa.String(60), nullable=False),
        sa.Column("instrument_type", sa.String(10), nullable=False),
        sa.Column("expiry_date", sa.Date, nullable=False),
        sa.Column(
            "strike_price",
            sa.Numeric(18, 4),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "option_type",
            sa.String(2),
            nullable=False,
            server_default=sa.text("'--'"),
        ),
        sa.Column("open", sa.Numeric(18, 4), nullable=True),
        sa.Column("high", sa.Numeric(18, 4), nullable=True),
        sa.Column("low", sa.Numeric(18, 4), nullable=True),
        sa.Column("close", sa.Numeric(18, 4), nullable=True),
        sa.Column("settle_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("prev_close", sa.Numeric(18, 4), nullable=True),
        sa.Column("underlying_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("open_interest", sa.BigInteger, nullable=True),
        sa.Column("change_in_oi", sa.BigInteger, nullable=True),
        sa.Column("contracts_traded", sa.BigInteger, nullable=True),
        sa.Column("turnover_lakh", sa.Numeric(20, 4), nullable=True),
        sa.Column("num_trades", sa.BigInteger, nullable=True),
        sa.Column(
            "source", sa.String(50), nullable=False, server_default=sa.text("'NSE'")
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint(
            "trade_date",
            "symbol",
            "instrument_type",
            "expiry_date",
            "strike_price",
            "option_type",
        ),
    )
    op.create_index(
        "ix_de_fo_bhavcopy_trade_date", "de_fo_bhavcopy", ["trade_date"]
    )
    op.create_index(
        "ix_de_fo_bhavcopy_symbol_expiry",
        "de_fo_bhavcopy",
        ["symbol", "expiry_date"],
    )

    # ── de_fo_ban_list ────────────────────────────────────────────────────
    op.create_table(
        "de_fo_ban_list",
        sa.Column("business_date", sa.Date, nullable=False),
        sa.Column("symbol", sa.String(60), nullable=False),
        sa.Column("ban_count", sa.SmallInteger, nullable=True),
        sa.Column(
            "source", sa.String(50), nullable=False, server_default=sa.text("'NSE'")
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("business_date", "symbol"),
    )

    # ── de_participant_oi ─────────────────────────────────────────────────
    op.create_table(
        "de_participant_oi",
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("client_type", sa.String(20), nullable=False),
        sa.Column("future_index_long", sa.BigInteger, nullable=True),
        sa.Column("future_index_short", sa.BigInteger, nullable=True),
        sa.Column("future_stock_long", sa.BigInteger, nullable=True),
        sa.Column("future_stock_short", sa.BigInteger, nullable=True),
        sa.Column("option_index_call_long", sa.BigInteger, nullable=True),
        sa.Column("option_index_put_long", sa.BigInteger, nullable=True),
        sa.Column("option_index_call_short", sa.BigInteger, nullable=True),
        sa.Column("option_index_put_short", sa.BigInteger, nullable=True),
        sa.Column("option_stock_call_long", sa.BigInteger, nullable=True),
        sa.Column("option_stock_put_long", sa.BigInteger, nullable=True),
        sa.Column("option_stock_call_short", sa.BigInteger, nullable=True),
        sa.Column("option_stock_put_short", sa.BigInteger, nullable=True),
        sa.Column("total_long_contracts", sa.BigInteger, nullable=True),
        sa.Column("total_short_contracts", sa.BigInteger, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "client_type IN ('Client','DII','FII','Pro','TOTAL')",
            name="chk_participant_oi_client_type",
        ),
        sa.PrimaryKeyConstraint("trade_date", "client_type"),
    )

    # ── de_gsec_yield ─────────────────────────────────────────────────────
    op.create_table(
        "de_gsec_yield",
        sa.Column("yield_date", sa.Date, nullable=False),
        sa.Column("tenor", sa.String(10), nullable=False),
        sa.Column("yield_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column("security_name", sa.String(100), nullable=True),
        sa.Column(
            "source",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'CCIL'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("yield_date", "tenor"),
    )

    # ── de_rbi_fx_rate ────────────────────────────────────────────────────
    op.create_table(
        "de_rbi_fx_rate",
        sa.Column("rate_date", sa.Date, nullable=False),
        sa.Column("currency_pair", sa.String(10), nullable=False),
        sa.Column("reference_rate", sa.Numeric(12, 4), nullable=False),
        sa.Column(
            "source",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'FBIL'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("rate_date", "currency_pair"),
    )

    # ── de_rbi_policy_rate ────────────────────────────────────────────────
    op.create_table(
        "de_rbi_policy_rate",
        sa.Column("effective_date", sa.Date, nullable=False),
        sa.Column("rate_type", sa.String(30), nullable=False),
        sa.Column("rate_pct", sa.Numeric(8, 4), nullable=False),
        sa.Column(
            "source",
            sa.String(50),
            nullable=False,
            server_default=sa.text("'RBI'"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "rate_type IN ('REPO','REVERSE_REPO','MSF','BANK_RATE','CRR','SLR')",
            name="chk_rbi_policy_rate_type",
        ),
        sa.PrimaryKeyConstraint("effective_date", "rate_type"),
    )

    # ── de_insider_trades ─────────────────────────────────────────────────
    op.create_table(
        "de_insider_trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("disclosure_date", sa.Date, nullable=False),
        sa.Column("transaction_date", sa.Date, nullable=True),
        sa.Column("symbol", sa.String(60), nullable=False),
        sa.Column("company_name", sa.String(255), nullable=True),
        sa.Column("person_name", sa.String(200), nullable=False),
        sa.Column("person_category", sa.String(100), nullable=True),
        sa.Column("transaction_type", sa.String(20), nullable=False),
        sa.Column("quantity", sa.BigInteger, nullable=False),
        sa.Column("value_inr", sa.Numeric(20, 4), nullable=True),
        sa.Column("pre_holding_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("post_holding_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("exchange", sa.String(10), nullable=False),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("raw_payload", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "disclosure_date",
            "symbol",
            "person_name",
            "transaction_type",
            "quantity",
            "exchange",
            name="uq_insider_trades_natural_key",
        ),
        sa.CheckConstraint(
            "transaction_type IN ('BUY','SELL','PLEDGE','INVOCATION')",
            name="chk_insider_trades_txn_type",
        ),
        sa.CheckConstraint(
            "exchange IN ('NSE','BSE')", name="chk_insider_trades_exchange"
        ),
    )
    op.create_index(
        "ix_insider_trades_disclosure_date",
        "de_insider_trades",
        ["disclosure_date"],
    )
    op.create_index(
        "ix_insider_trades_symbol", "de_insider_trades", ["symbol"]
    )
    op.create_index(
        "ix_insider_trades_person_name", "de_insider_trades", ["person_name"]
    )

    # ── de_bulk_block_deals ───────────────────────────────────────────────
    op.create_table(
        "de_bulk_block_deals",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("deal_date", sa.Date, nullable=False),
        sa.Column("symbol", sa.String(60), nullable=False),
        sa.Column("company_name", sa.String(255), nullable=True),
        sa.Column("client_name", sa.String(255), nullable=False),
        sa.Column("deal_type", sa.String(10), nullable=False),
        sa.Column("transaction_type", sa.String(4), nullable=False),
        sa.Column("quantity", sa.BigInteger, nullable=False),
        sa.Column("traded_price", sa.Numeric(18, 4), nullable=True),
        sa.Column(
            "value_inr",
            sa.Numeric(20, 4),
            sa.Computed("quantity * traded_price", persisted=True),
            nullable=True,
        ),
        sa.Column("exchange", sa.String(10), nullable=False),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "deal_date",
            "symbol",
            "client_name",
            "deal_type",
            "transaction_type",
            "quantity",
            "exchange",
            name="uq_bulk_block_deal",
        ),
        sa.CheckConstraint(
            "deal_type IN ('BULK','BLOCK')", name="chk_bulk_block_deal_type"
        ),
        sa.CheckConstraint(
            "transaction_type IN ('BUY','SELL')",
            name="chk_bulk_block_transaction_type",
        ),
    )
    op.create_index(
        "ix_bulk_block_deals_date", "de_bulk_block_deals", ["deal_date"]
    )
    op.create_index(
        "ix_bulk_block_deals_symbol", "de_bulk_block_deals", ["symbol"]
    )
    op.create_index(
        "ix_bulk_block_deals_client", "de_bulk_block_deals", ["client_name"]
    )

    # ── de_shareholding_pattern ───────────────────────────────────────────
    op.create_table(
        "de_shareholding_pattern",
        sa.Column("symbol", sa.String(60), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("promoter_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("promoter_pledged_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("public_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("fii_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("dii_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("mf_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("insurance_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("banks_fi_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("retail_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("hni_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("other_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("total_shares", sa.BigInteger, nullable=True),
        sa.Column("exchange", sa.String(10), nullable=True),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("filing_url", sa.String(500), nullable=True),
        sa.Column("raw_payload", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("symbol", "as_of_date"),
    )
    op.create_index(
        "ix_shareholding_pattern_as_of_date",
        "de_shareholding_pattern",
        ["as_of_date"],
    )
    op.create_index(
        "ix_shareholding_pattern_symbol",
        "de_shareholding_pattern",
        ["symbol"],
    )


def downgrade() -> None:
    op.drop_table("de_shareholding_pattern")
    op.drop_table("de_bulk_block_deals")
    op.drop_table("de_insider_trades")
    op.drop_table("de_rbi_policy_rate")
    op.drop_table("de_rbi_fx_rate")
    op.drop_table("de_gsec_yield")
    op.drop_table("de_participant_oi")
    op.drop_table("de_fo_ban_list")
    op.drop_table("de_fo_bhavcopy")
