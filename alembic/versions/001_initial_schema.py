"""Initial schema — all 40+ tables for JIP Data Engine v2.0.

Revision ID: 001
Revises:
Create Date: 2026-04-05 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB, UUID

# revision identifiers
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -------------------------------------------------------------------------
    # 1. Extensions
    # -------------------------------------------------------------------------
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    try:
        op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    except Exception:
        pass  # pgvector not available in local dev — skip, add manually in production

    # -------------------------------------------------------------------------
    # 2. Core reference tables (no FKs to other app tables)
    # -------------------------------------------------------------------------
    op.create_table(
        "de_contributors",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("role", sa.String(20), nullable=False),
        sa.Column("is_admin", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "role IN ('admin','analyst','pipeline','viewer','external')",
            name="chk_contributor_role",
        ),
        sa.UniqueConstraint("name", name="uq_contributors_name"),
    )

    op.create_table(
        "de_trading_calendar",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("is_trading", sa.Boolean, nullable=False),
        sa.Column("exchange", sa.String(10), nullable=False, server_default="NSE"),
        sa.Column("notes", sa.String(200), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "de_macro_master",
        sa.Column("ticker", sa.String(20), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("source", sa.String(20), nullable=False),
        sa.Column("unit", sa.String(50), nullable=True),
        sa.Column("frequency", sa.String(20), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "source IN ('FRED','RBI','MOSPI','NSO','SEBI','BSE','NSE','manual')",
            name="chk_macro_source",
        ),
        sa.CheckConstraint(
            "frequency IN ('daily','weekly','monthly','quarterly','annual')",
            name="chk_macro_frequency",
        ),
    )

    op.create_table(
        "de_global_instrument_master",
        sa.Column("ticker", sa.String(20), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("instrument_type", sa.String(10), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=True),
        sa.Column("currency", sa.String(5), nullable=True),
        sa.Column("country", sa.String(50), nullable=True),
        sa.Column("category", sa.String(100), nullable=True),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "instrument_type IN ('index','etf')",
            name="chk_global_instrument_type",
        ),
    )

    # -------------------------------------------------------------------------
    # 3. Instrument, Index, MF masters
    # -------------------------------------------------------------------------
    op.create_table(
        "de_instrument",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("current_symbol", sa.String(50), nullable=False),
        sa.Column("isin", sa.String(12), nullable=True),
        sa.Column("company_name", sa.String(500), nullable=True),
        sa.Column("exchange", sa.String(10), nullable=True),
        sa.Column("series", sa.String(10), nullable=True),
        sa.Column("sector", sa.String(200), nullable=True),
        sa.Column("industry", sa.String(200), nullable=True),
        sa.Column("nifty_50", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("nifty_200", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("nifty_500", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("listing_date", sa.Date, nullable=True),
        sa.Column("bse_symbol", sa.String(50), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("is_suspended", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("suspended_from", sa.Date, nullable=True),
        sa.Column("delisted_on", sa.Date, nullable=True),
        sa.Column("is_tradeable", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("current_symbol", name="uq_instrument_current_symbol"),
    )

    op.create_table(
        "de_index_master",
        sa.Column("index_code", sa.String(50), primary_key=True),
        sa.Column("index_name", sa.String(200), nullable=False),
        sa.Column("category", sa.String(20), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "category IN ('broad','sectoral','thematic','strategy')",
            name="chk_index_category",
        ),
    )

    op.create_table(
        "de_mf_master",
        sa.Column("mstar_id", sa.String(20), primary_key=True),
        sa.Column("amfi_code", sa.String(20), nullable=True),
        sa.Column("isin", sa.String(12), nullable=True),
        sa.Column("fund_name", sa.String(500), nullable=False),
        sa.Column("amc_name", sa.String(200), nullable=True),
        sa.Column("category_name", sa.String(200), nullable=True),
        sa.Column("broad_category", sa.String(100), nullable=True),
        sa.Column("is_index_fund", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("is_etf", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("inception_date", sa.Date, nullable=True),
        sa.Column("closure_date", sa.Date, nullable=True),
        sa.Column("merged_into_mstar_id", sa.String(20), sa.ForeignKey("de_mf_master.mstar_id", ondelete="SET NULL"), nullable=True),
        sa.Column("primary_benchmark", sa.String(100), nullable=True),
        sa.Column("expense_ratio", sa.Numeric(6, 4), nullable=True),
        sa.Column("investment_strategy", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "merged_into_mstar_id IS NULL OR merged_into_mstar_id != mstar_id",
            name="chk_mf_no_self_merge",
        ),
    )
    op.create_index("ix_de_mf_master_merged_into", "de_mf_master", ["merged_into_mstar_id"])

    # -------------------------------------------------------------------------
    # 4. Pipeline infra tables (source_files, pipeline_log)
    #    Must exist BEFORE any price tables that FK to them
    # -------------------------------------------------------------------------
    op.create_table(
        "de_source_files",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_name", sa.String(100), nullable=False),
        sa.Column("file_name", sa.String(500), nullable=False),
        sa.Column("file_date", sa.Date, nullable=True),
        sa.Column("checksum", sa.String(64), nullable=True),
        sa.Column("file_size_bytes", sa.BigInteger, nullable=True),
        sa.Column("row_count", sa.BigInteger, nullable=True),
        sa.Column("format_version", sa.String(50), nullable=True),
        sa.Column("ingested_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("source_name", "file_date", "checksum", name="uq_source_files_dedup"),
    )

    op.create_table(
        "de_pipeline_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("pipeline_name", sa.String(100), nullable=False),
        sa.Column("business_date", sa.Date, nullable=True),
        sa.Column("run_number", sa.Integer, nullable=False, server_default="1"),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("rows_processed", sa.BigInteger, nullable=True),
        sa.Column("rows_failed", sa.BigInteger, nullable=True),
        sa.Column("source_date", sa.Date, nullable=True),
        sa.Column("source_rowcount", sa.BigInteger, nullable=True),
        sa.Column("source_checksum", sa.String(64), nullable=True),
        sa.Column("error_detail", sa.Text, nullable=True),
        sa.Column("track_status", JSONB, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("pipeline_name", "business_date", "run_number", name="uq_pipeline_log_run"),
        sa.CheckConstraint(
            "status IN ('pending','running','success','partial','failed','skipped')",
            name="chk_pipeline_log_status",
        ),
    )
    op.create_index(
        "ix_de_pipeline_log_source_checksum",
        "de_pipeline_log",
        ["source_checksum"],
        postgresql_where=sa.text("source_checksum IS NOT NULL"),
    )

    # -------------------------------------------------------------------------
    # 5. System/migration tables
    # -------------------------------------------------------------------------
    op.create_table(
        "de_system_flags",
        sa.Column("key", sa.String(50), primary_key=True),
        sa.Column("value", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("updated_by", sa.String(100), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "de_migration_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("source_db", sa.String(100), nullable=False),
        sa.Column("source_table", sa.String(100), nullable=False),
        sa.Column("target_table", sa.String(100), nullable=False),
        sa.Column("rows_read", sa.BigInteger, nullable=True),
        sa.Column("rows_written", sa.BigInteger, nullable=True),
        sa.Column("rows_errored", sa.BigInteger, nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("checksum_source", sa.BigInteger, nullable=True),
        sa.Column("checksum_dest", sa.BigInteger, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending','running','success','failed','partial')",
            name="chk_migration_log_status",
        ),
    )

    op.create_table(
        "de_migration_errors",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("migration_id", sa.Integer, sa.ForeignKey("de_migration_log.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_row", JSONB, nullable=True),
        sa.Column("error_reason", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_migration_errors_migration_id", "de_migration_errors", ["migration_id"])

    # -------------------------------------------------------------------------
    # 6. Instrument child tables
    # -------------------------------------------------------------------------
    op.create_table(
        "de_market_cap_history",
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("effective_from", sa.Date, primary_key=True),
        sa.Column("cap_category", sa.String(10), nullable=False),
        sa.Column("effective_to", sa.Date, nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "cap_category IN ('large','mid','small','micro')",
            name="chk_market_cap_category",
        ),
    )
    op.create_index("ix_de_market_cap_history_instrument_id", "de_market_cap_history", ["instrument_id"])
    # Partial unique: only one active cap per instrument
    op.create_index(
        "uix_market_cap_active",
        "de_market_cap_history",
        ["instrument_id"],
        unique=True,
        postgresql_where=sa.text("effective_to IS NULL"),
    )

    op.create_table(
        "de_symbol_history",
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("effective_date", sa.Date, primary_key=True),
        sa.Column("old_symbol", sa.String(50), nullable=False),
        sa.Column("new_symbol", sa.String(50), nullable=False),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_symbol_history_instrument_id", "de_symbol_history", ["instrument_id"])

    op.create_table(
        "de_index_constituents",
        sa.Column("index_code", sa.String(50), sa.ForeignKey("de_index_master.index_code", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("effective_from", sa.Date, primary_key=True),
        sa.Column("weight_pct", sa.Numeric(6, 4), nullable=True),
        sa.Column("effective_to", sa.Date, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_index_constituents_index_code", "de_index_constituents", ["index_code"])
    op.create_index("ix_de_index_constituents_instrument_id", "de_index_constituents", ["instrument_id"])
    # Partial unique: only one active constituent per (index, instrument)
    op.create_index(
        "uix_index_constituent_active",
        "de_index_constituents",
        ["index_code", "instrument_id"],
        unique=True,
        postgresql_where=sa.text("effective_to IS NULL"),
    )

    # -------------------------------------------------------------------------
    # 7. Equity OHLCV — partitioned by range on date
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE de_equity_ohlcv (
            date                DATE        NOT NULL,
            instrument_id       UUID        NOT NULL,
            symbol              VARCHAR(50),
            open                NUMERIC(18,4),
            high                NUMERIC(18,4),
            low                 NUMERIC(18,4),
            close               NUMERIC(18,4),
            close_adj           NUMERIC(18,4),
            open_adj            NUMERIC(18,4),
            high_adj            NUMERIC(18,4),
            low_adj             NUMERIC(18,4),
            volume              BIGINT,
            volume_adj          BIGINT,
            delivery_vol        BIGINT,
            delivery_pct        NUMERIC(6,2),
            trades              INTEGER,
            data_status         VARCHAR(20)  NOT NULL DEFAULT 'raw',
            source_file_id      UUID         REFERENCES de_source_files(id) ON DELETE SET NULL,
            pipeline_run_id     INTEGER      REFERENCES de_pipeline_log(id) ON DELETE SET NULL,
            created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (date, instrument_id),
            CONSTRAINT chk_equity_ohlcv_data_status CHECK (data_status IN ('raw','validated','quarantined'))
        ) PARTITION BY RANGE (date)
    """)
    op.execute("CREATE INDEX ix_de_equity_ohlcv_instrument_id ON de_equity_ohlcv (instrument_id)")
    op.execute("CREATE INDEX ix_de_equity_ohlcv_source_file_id ON de_equity_ohlcv (source_file_id)")
    op.execute("CREATE INDEX ix_de_equity_ohlcv_pipeline_run_id ON de_equity_ohlcv (pipeline_run_id)")

    # Create yearly partitions 2000–2034
    for year in range(2000, 2035):
        op.execute(
            f"CREATE TABLE de_equity_ohlcv_y{year} PARTITION OF de_equity_ohlcv "
            f"FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')"
        )
    # Default partition for anything outside range
    op.execute(
        "CREATE TABLE de_equity_ohlcv_default PARTITION OF de_equity_ohlcv DEFAULT"
    )

    # -------------------------------------------------------------------------
    # 8. Corporate actions, adjustment factors, recompute queue
    # -------------------------------------------------------------------------
    op.create_table(
        "de_corporate_actions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False),
        sa.Column("ex_date", sa.Date, nullable=False),
        sa.Column("action_type", sa.String(20), nullable=False),
        sa.Column("dividend_type", sa.String(10), nullable=True),
        sa.Column("ratio_from", sa.Numeric(18, 8), nullable=True),
        sa.Column("ratio_to", sa.Numeric(18, 8), nullable=True),
        sa.Column("cash_value", sa.Numeric(18, 4), nullable=True),
        sa.Column("new_instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="SET NULL"), nullable=True),
        sa.Column("adj_factor", sa.Numeric(18, 8), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "action_type IN ('dividend','split','bonus','rights','merger','demerger','buyback','delisting','suspension','name_change','isin_change','other')",
            name="chk_corp_action_type",
        ),
        sa.CheckConstraint(
            "dividend_type IN ('interim','final','special','none') OR dividend_type IS NULL",
            name="chk_corp_dividend_type",
        ),
        sa.UniqueConstraint(
            "instrument_id", "ex_date", "action_type", "dividend_type",
            name="uq_corporate_actions",
        ),
    )
    op.create_index("ix_de_corporate_actions_instrument_id", "de_corporate_actions", ["instrument_id"])
    op.create_index("ix_de_corporate_actions_new_instrument_id", "de_corporate_actions", ["new_instrument_id"])

    op.create_table(
        "de_adjustment_factors_daily",
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("cumulative_factor", sa.Numeric(18, 8), nullable=False, server_default="1.0"),
        sa.Column("last_action_id", UUID(as_uuid=True), sa.ForeignKey("de_corporate_actions.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_adjustment_factors_daily_instrument_id", "de_adjustment_factors_daily", ["instrument_id"])
    op.create_index("ix_de_adjustment_factors_daily_last_action_id", "de_adjustment_factors_daily", ["last_action_id"])

    op.create_table(
        "de_recompute_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False),
        sa.Column("from_date", sa.Date, nullable=False),
        sa.Column("trigger_action_id", UUID(as_uuid=True), sa.ForeignKey("de_corporate_actions.id", ondelete="SET NULL"), nullable=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="5"),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("heartbeat_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("enqueued_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("error_detail", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("status IN ('pending','processing','complete','failed')", name="chk_recompute_queue_status"),
        sa.CheckConstraint("priority BETWEEN 1 AND 10", name="chk_recompute_queue_priority"),
    )
    op.create_index("ix_de_recompute_queue_instrument_id", "de_recompute_queue", ["instrument_id"])
    op.create_index("ix_de_recompute_queue_trigger_action_id", "de_recompute_queue", ["trigger_action_id"])
    # Partial indexes for queue worker queries
    op.create_index(
        "ix_de_recompute_queue_status_priority",
        "de_recompute_queue",
        ["status", "priority"],
        postgresql_where=sa.text("status IN ('pending','processing')"),
    )
    op.create_index(
        "uix_de_recompute_queue_pending_instrument",
        "de_recompute_queue",
        ["instrument_id"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )

    # -------------------------------------------------------------------------
    # 9. MF NAV — partitioned by range on nav_date
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE de_mf_nav_daily (
            nav_date            DATE         NOT NULL,
            mstar_id            VARCHAR(20)  NOT NULL,
            nav                 NUMERIC(18,4) NOT NULL,
            nav_adj             NUMERIC(18,4),
            nav_change          NUMERIC(18,4),
            nav_change_pct      NUMERIC(10,4),
            return_1d           NUMERIC(10,4),
            return_1w           NUMERIC(10,4),
            return_1m           NUMERIC(10,4),
            return_3m           NUMERIC(10,4),
            return_6m           NUMERIC(10,4),
            return_1y           NUMERIC(10,4),
            return_3y           NUMERIC(10,4),
            return_5y           NUMERIC(10,4),
            return_10y          NUMERIC(10,4),
            nav_52wk_high       NUMERIC(18,4),
            nav_52wk_low        NUMERIC(18,4),
            data_status         VARCHAR(20)  NOT NULL DEFAULT 'raw',
            source_file_id      UUID         REFERENCES de_source_files(id) ON DELETE SET NULL,
            pipeline_run_id     INTEGER      REFERENCES de_pipeline_log(id) ON DELETE SET NULL,
            created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (nav_date, mstar_id),
            CONSTRAINT chk_mf_nav_data_status CHECK (data_status IN ('raw','validated','quarantined')),
            CONSTRAINT chk_mf_nav_positive CHECK (nav > 0)
        ) PARTITION BY RANGE (nav_date)
    """)
    op.execute("CREATE INDEX ix_de_mf_nav_daily_mstar_id ON de_mf_nav_daily (mstar_id)")
    op.execute("CREATE INDEX ix_de_mf_nav_daily_source_file_id ON de_mf_nav_daily (source_file_id)")
    op.execute("CREATE INDEX ix_de_mf_nav_daily_pipeline_run_id ON de_mf_nav_daily (pipeline_run_id)")

    # Create yearly partitions 2006–2034
    for year in range(2006, 2035):
        op.execute(
            f"CREATE TABLE de_mf_nav_daily_y{year} PARTITION OF de_mf_nav_daily "
            f"FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')"
        )
    op.execute(
        "CREATE TABLE de_mf_nav_daily_default PARTITION OF de_mf_nav_daily DEFAULT"
    )

    # -------------------------------------------------------------------------
    # 10. MF dividends and lifecycle
    # -------------------------------------------------------------------------
    op.create_table(
        "de_mf_dividends",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("mstar_id", sa.String(20), sa.ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"), nullable=False),
        sa.Column("record_date", sa.Date, nullable=False),
        sa.Column("dividend_per_unit", sa.Numeric(18, 4), nullable=False),
        sa.Column("nav_before", sa.Numeric(18, 4), nullable=True),
        sa.Column("nav_after", sa.Numeric(18, 4), nullable=True),
        sa.Column("adj_factor", sa.Numeric(18, 8), nullable=True),
        sa.Column("source", sa.String(100), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("dividend_per_unit > 0", name="chk_mf_div_positive"),
        sa.UniqueConstraint("mstar_id", "record_date", name="uq_mf_dividends"),
    )
    op.create_index("ix_de_mf_dividends_mstar_id", "de_mf_dividends", ["mstar_id"])

    op.create_table(
        "de_mf_lifecycle",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("mstar_id", sa.String(20), sa.ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_type", sa.String(30), nullable=False),
        sa.Column("event_date", sa.Date, nullable=False),
        sa.Column("old_value", sa.Text, nullable=True),
        sa.Column("new_value", sa.Text, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "event_type IN ('launch','merge','name_change','category_change','amc_change','closure','benchmark_change','reopen')",
            name="chk_mf_lifecycle_event_type",
        ),
    )
    op.create_index("ix_de_mf_lifecycle_mstar_id", "de_mf_lifecycle", ["mstar_id"])

    # -------------------------------------------------------------------------
    # 11. Index, global and macro prices
    # -------------------------------------------------------------------------
    op.create_table(
        "de_index_prices",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("index_code", sa.String(50), sa.ForeignKey("de_index_master.index_code", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("open", sa.Numeric(18, 4), nullable=True),
        sa.Column("high", sa.Numeric(18, 4), nullable=True),
        sa.Column("low", sa.Numeric(18, 4), nullable=True),
        sa.Column("close", sa.Numeric(18, 4), nullable=True),
        sa.Column("volume", sa.BigInteger, nullable=True),
        sa.Column("pe_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("pb_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("div_yield", sa.Numeric(6, 2), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_index_prices_index_code", "de_index_prices", ["index_code"])

    op.create_table(
        "de_global_prices",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("ticker", sa.String(20), sa.ForeignKey("de_global_instrument_master.ticker", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("open", sa.Numeric(18, 4), nullable=True),
        sa.Column("high", sa.Numeric(18, 4), nullable=True),
        sa.Column("low", sa.Numeric(18, 4), nullable=True),
        sa.Column("close", sa.Numeric(18, 4), nullable=True),
        sa.Column("volume", sa.BigInteger, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_global_prices_ticker", "de_global_prices", ["ticker"])

    op.create_table(
        "de_macro_values",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("ticker", sa.String(20), sa.ForeignKey("de_macro_master.ticker", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("value", sa.Numeric(18, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_macro_values_ticker", "de_macro_values", ["ticker"])

    # -------------------------------------------------------------------------
    # 12. Institutional and MF flows
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE de_institutional_flows (
            date            DATE         NOT NULL,
            category        VARCHAR(20)  NOT NULL,
            market_type     VARCHAR(20)  NOT NULL,
            gross_buy       NUMERIC(18,4),
            gross_sell      NUMERIC(18,4),
            net_flow        NUMERIC(18,4) GENERATED ALWAYS AS (gross_buy - gross_sell) STORED,
            source          VARCHAR(100),
            created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (date, category, market_type),
            CONSTRAINT chk_inst_flow_category CHECK (category IN ('FII','DII','MF','Insurance','Banks','Corporates','Retail','Other')),
            CONSTRAINT chk_inst_flow_market_type CHECK (market_type IN ('equity','debt','hybrid','derivatives')),
            CONSTRAINT chk_inst_flow_gross_buy_positive CHECK (gross_buy >= 0),
            CONSTRAINT chk_inst_flow_gross_sell_positive CHECK (gross_sell >= 0)
        )
    """)

    op.create_table(
        "de_mf_category_flows",
        sa.Column("month_date", sa.Date, primary_key=True),
        sa.Column("category", sa.String(200), primary_key=True),
        sa.Column("net_flow_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("gross_inflow_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("gross_outflow_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("aum_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("sip_flow_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("sip_accounts", sa.Integer, nullable=True),
        sa.Column("folios", sa.Integer, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # -------------------------------------------------------------------------
    # 13. Computed / technical tables
    # -------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE de_equity_technical_daily (
            date            DATE    NOT NULL,
            instrument_id   UUID    NOT NULL REFERENCES de_instrument(id) ON DELETE CASCADE,
            sma_50          NUMERIC(18,4),
            sma_200         NUMERIC(18,4),
            ema_20          NUMERIC(18,4),
            close_adj       NUMERIC(18,4),
            above_50dma     BOOLEAN GENERATED ALWAYS AS (close_adj > sma_50) STORED,
            above_200dma    BOOLEAN GENERATED ALWAYS AS (close_adj > sma_200) STORED,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (date, instrument_id)
        )
    """)
    op.execute("CREATE INDEX ix_de_equity_technical_daily_instrument_id ON de_equity_technical_daily (instrument_id)")

    op.create_table(
        "de_rs_scores",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("entity_type", sa.String(20), primary_key=True),
        sa.Column("entity_id", sa.String(50), primary_key=True),
        sa.Column("vs_benchmark", sa.String(50), primary_key=True),
        sa.Column("rs_1w", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_1m", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_3m", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_6m", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_12m", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_composite", sa.Numeric(10, 4), nullable=True),
        sa.Column("computation_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "de_rs_daily_summary",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("vs_benchmark", sa.String(50), primary_key=True),
        sa.Column("symbol", sa.String(50), nullable=True),
        sa.Column("sector", sa.String(200), nullable=True),
        sa.Column("rs_composite", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_1m", sa.Numeric(10, 4), nullable=True),
        sa.Column("rs_3m", sa.Numeric(10, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_rs_daily_summary_instrument_id", "de_rs_daily_summary", ["instrument_id"])

    op.create_table(
        "de_market_regime",
        sa.Column("computed_at", sa.TIMESTAMP(timezone=True), primary_key=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("regime", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Numeric(6, 2), nullable=True),
        sa.Column("breadth_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("momentum_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("volume_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("global_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("fii_score", sa.Numeric(6, 2), nullable=True),
        sa.Column("indicator_detail", JSONB, nullable=True),
        sa.Column("computation_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("regime IN ('BULL','BEAR','SIDEWAYS','RECOVERY')", name="chk_market_regime_type"),
        sa.CheckConstraint("confidence BETWEEN 0 AND 100", name="chk_market_regime_confidence"),
        sa.CheckConstraint("breadth_score BETWEEN 0 AND 100", name="chk_market_regime_breadth"),
        sa.CheckConstraint("momentum_score BETWEEN 0 AND 100", name="chk_market_regime_momentum"),
        sa.CheckConstraint("volume_score BETWEEN 0 AND 100", name="chk_market_regime_volume"),
        sa.CheckConstraint("global_score BETWEEN 0 AND 100", name="chk_market_regime_global"),
        sa.CheckConstraint("fii_score BETWEEN 0 AND 100", name="chk_market_regime_fii"),
    )

    op.create_table(
        "de_breadth_daily",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("advance", sa.Integer, nullable=True),
        sa.Column("decline", sa.Integer, nullable=True),
        sa.Column("unchanged", sa.Integer, nullable=True),
        sa.Column("total_stocks", sa.Integer, nullable=True),
        sa.Column("ad_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("pct_above_200dma", sa.Numeric(6, 2), nullable=True),
        sa.Column("pct_above_50dma", sa.Numeric(6, 2), nullable=True),
        sa.Column("new_52w_highs", sa.Integer, nullable=True),
        sa.Column("new_52w_lows", sa.Integer, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("pct_above_200dma BETWEEN 0 AND 100", name="chk_breadth_pct_200dma"),
        sa.CheckConstraint("pct_above_50dma BETWEEN 0 AND 100", name="chk_breadth_pct_50dma"),
        sa.CheckConstraint("advance >= 0", name="chk_breadth_advance"),
        sa.CheckConstraint("decline >= 0", name="chk_breadth_decline"),
        sa.CheckConstraint("unchanged >= 0", name="chk_breadth_unchanged"),
        sa.CheckConstraint("total_stocks >= 0", name="chk_breadth_total"),
    )

    op.create_table(
        "de_fo_summary",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("pcr_oi", sa.Numeric(10, 4), nullable=True),
        sa.Column("pcr_volume", sa.Numeric(10, 4), nullable=True),
        sa.Column("total_oi", sa.BigInteger, nullable=True),
        sa.Column("oi_change", sa.BigInteger, nullable=True),
        sa.Column("fii_index_long", sa.Numeric(18, 4), nullable=True),
        sa.Column("fii_index_short", sa.Numeric(18, 4), nullable=True),
        sa.Column("fii_net_futures", sa.Numeric(18, 4), nullable=True),
        sa.Column("fii_net_options", sa.Numeric(18, 4), nullable=True),
        sa.Column("max_pain", sa.Numeric(18, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # -------------------------------------------------------------------------
    # 14. Data anomalies
    # -------------------------------------------------------------------------
    op.create_table(
        "de_data_anomalies",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("pipeline_name", sa.String(100), nullable=False),
        sa.Column("business_date", sa.Date, nullable=True),
        sa.Column("entity_type", sa.String(20), nullable=False),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="SET NULL"), nullable=True),
        sa.Column("mstar_id", sa.String(20), nullable=True),
        sa.Column("ticker", sa.String(20), nullable=True),
        sa.Column("anomaly_type", sa.String(30), nullable=False),
        sa.Column("severity", sa.String(10), nullable=False),
        sa.Column("expected_range", sa.String(200), nullable=True),
        sa.Column("actual_value", sa.String(200), nullable=True),
        sa.Column("is_resolved", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("resolved_by", sa.String(100), nullable=True),
        sa.Column("resolved_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("resolution_note", sa.Text, nullable=True),
        sa.Column("detected_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("entity_type IN ('equity','mf','index','macro','flow')", name="chk_data_anomaly_entity_type"),
        sa.CheckConstraint(
            "anomaly_type IN ('price_spike','price_gap','zero_volume','missing_data','duplicate','negative_value','stale_data','nav_deviation','dividend_anomaly','split_mismatch','invalid_ratio','other')",
            name="chk_data_anomaly_type",
        ),
        sa.CheckConstraint("severity IN ('low','medium','high','critical')", name="chk_data_anomaly_severity"),
        sa.CheckConstraint(
            """
            (entity_type = 'equity' AND instrument_id IS NOT NULL)
            OR (entity_type = 'mf' AND mstar_id IS NOT NULL)
            OR (entity_type IN ('macro','flow') AND ticker IS NOT NULL)
            OR (entity_type = 'index' AND (ticker IS NOT NULL OR instrument_id IS NOT NULL))
            """,
            name="chk_data_anomaly_entity_ref_consistency",
        ),
    )
    op.create_index("ix_de_data_anomalies_instrument_id", "de_data_anomalies", ["instrument_id"])

    # -------------------------------------------------------------------------
    # 15. Qualitative layer
    # -------------------------------------------------------------------------
    op.create_table(
        "de_qual_sources",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("source_name", sa.String(200), nullable=False),
        sa.Column("source_type", sa.String(20), nullable=False),
        sa.Column("contributor_id", sa.Integer, sa.ForeignKey("de_contributors.id", ondelete="SET NULL"), nullable=True),
        sa.Column("feed_url", sa.Text, nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "source_type IN ('podcast','report','interview','webinar','article','social','internal')",
            name="chk_qual_source_type",
        ),
        sa.UniqueConstraint("source_name", name="uq_qual_sources_name"),
    )
    op.create_index("ix_de_qual_sources_contributor_id", "de_qual_sources", ["contributor_id"])

    op.create_table(
        "de_qual_documents",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("source_id", sa.Integer, sa.ForeignKey("de_qual_sources.id", ondelete="CASCADE"), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("source_url", sa.Text, nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("ingested_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("title", sa.String(500), nullable=True),
        sa.Column("original_format", sa.String(10), nullable=True),
        sa.Column("raw_text", sa.Text, nullable=True),
        sa.Column("audio_url", sa.Text, nullable=True),
        sa.Column("audio_duration_s", sa.Integer, nullable=True),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("tags", ARRAY(sa.Text), nullable=True),
        sa.Column("processing_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("processing_error", sa.Text, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "original_format IN ('pdf','audio','video','html','text','docx','xlsx')",
            name="chk_qual_doc_format",
        ),
        sa.CheckConstraint(
            "processing_status IN ('pending','processing','done','failed','skipped')",
            name="chk_qual_doc_status",
        ),
        sa.UniqueConstraint("source_id", "content_hash", name="uq_qual_doc_source_hash"),
    )
    op.create_index("ix_de_qual_documents_source_id", "de_qual_documents", ["source_id"])

    # Add vector embedding columns if pgvector is available
    try:
        op.execute("ALTER TABLE de_qual_documents ADD COLUMN embedding vector(1536)")
    except Exception:
        # Fallback: add as bytea so the column exists even without pgvector
        op.execute("ALTER TABLE de_qual_documents ADD COLUMN IF NOT EXISTS embedding bytea")

    op.create_table(
        "de_qual_extracts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("document_id", UUID(as_uuid=True), sa.ForeignKey("de_qual_documents.id", ondelete="CASCADE"), nullable=False),
        sa.Column("asset_class", sa.String(20), nullable=True),
        sa.Column("entity_ref", sa.String(100), nullable=True),
        sa.Column("direction", sa.String(20), nullable=True),
        sa.Column("timeframe", sa.String(50), nullable=True),
        sa.Column("conviction", sa.String(20), nullable=True),
        sa.Column("view_text", sa.Text, nullable=True),
        sa.Column("source_quote", sa.Text, nullable=True),
        sa.Column("quality_score", sa.Numeric(3, 2), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "asset_class IN ('equity','mf','bond','commodity','currency','macro','real_estate','other')",
            name="chk_qual_extract_asset_class",
        ),
        sa.CheckConstraint("direction IN ('bullish','bearish','neutral','cautious')", name="chk_qual_extract_direction"),
        sa.CheckConstraint("conviction IN ('low','medium','high','very_high')", name="chk_qual_extract_conviction"),
        sa.CheckConstraint("quality_score BETWEEN 0 AND 1", name="chk_qual_extract_quality"),
    )
    op.create_index("ix_de_qual_extracts_document_id", "de_qual_extracts", ["document_id"])

    try:
        op.execute("ALTER TABLE de_qual_extracts ADD COLUMN embedding vector(1536)")
    except Exception:
        op.execute("ALTER TABLE de_qual_extracts ADD COLUMN IF NOT EXISTS embedding bytea")

    op.create_table(
        "de_qual_outcomes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("extract_id", UUID(as_uuid=True), sa.ForeignKey("de_qual_extracts.id", ondelete="CASCADE"), nullable=False),
        sa.Column("outcome_date", sa.Date, nullable=True),
        sa.Column("was_correct", sa.Boolean, nullable=True),
        sa.Column("actual_move_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("entity_ref", sa.String(100), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("recorded_by", sa.Integer, sa.ForeignKey("de_contributors.id", ondelete="SET NULL"), nullable=True),
        sa.Column("recorded_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_qual_outcomes_extract_id", "de_qual_outcomes", ["extract_id"])
    op.create_index("ix_de_qual_outcomes_recorded_by", "de_qual_outcomes", ["recorded_by"])

    # -------------------------------------------------------------------------
    # 16. Client and portfolio tables
    # -------------------------------------------------------------------------
    op.create_table(
        "de_clients",
        sa.Column("client_id", sa.String(50), primary_key=True),
        sa.Column("name", sa.String(500), nullable=True),
        sa.Column("email_enc", sa.Text, nullable=True),
        sa.Column("phone_enc", sa.Text, nullable=True),
        sa.Column("pan_enc", sa.Text, nullable=True),
        sa.Column("pan_hash", sa.String(8), nullable=True),
        sa.Column("email_hash", sa.String(8), nullable=True),
        sa.Column("phone_hash", sa.String(8), nullable=True),
        sa.Column("hmac_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    # Partial indexes on hash columns for existence checks
    op.create_index(
        "ix_de_clients_pan_hash",
        "de_clients",
        ["pan_hash"],
        postgresql_where=sa.text("pan_hash IS NOT NULL"),
    )
    op.create_index(
        "ix_de_clients_email_hash",
        "de_clients",
        ["email_hash"],
        postgresql_where=sa.text("email_hash IS NOT NULL"),
    )
    op.create_index(
        "ix_de_clients_phone_hash",
        "de_clients",
        ["phone_hash"],
        postgresql_where=sa.text("phone_hash IS NOT NULL"),
    )

    op.create_table(
        "de_client_keys",
        sa.Column("client_id", sa.String(50), sa.ForeignKey("de_clients.client_id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("key_version", sa.Integer, primary_key=True),
        sa.Column("encrypted_dek", sa.Text, nullable=False),
        sa.Column("kms_key_id", sa.String(200), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_client_keys_client_id", "de_client_keys", ["client_id"])
    op.create_index(
        "uix_de_client_keys_active",
        "de_client_keys",
        ["client_id"],
        unique=True,
        postgresql_where=sa.text("is_active = TRUE"),
    )

    op.create_table(
        "de_pii_access_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("accessed_by", sa.String(100), nullable=True),
        sa.Column("client_id", sa.String(50), nullable=True),
        sa.Column("fields_accessed", ARRAY(sa.Text), nullable=True),
        sa.Column("purpose", sa.String(200), nullable=True),
        sa.Column("source_ip", INET, nullable=True),
        sa.Column("accessed_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_table(
        "de_portfolios",
        sa.Column("portfolio_id", sa.String(50), primary_key=True),
        sa.Column("client_id", sa.String(50), sa.ForeignKey("de_clients.client_id", ondelete="CASCADE"), nullable=False),
        sa.Column("portfolio_name", sa.String(200), nullable=False),
        sa.Column("inception_date", sa.Date, nullable=True),
        sa.Column("strategy", sa.String(200), nullable=True),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_portfolios_client_id", "de_portfolios", ["client_id"])

    op.create_table(
        "de_portfolio_nav",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("portfolio_id", sa.String(50), sa.ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("nav", sa.Numeric(18, 4), nullable=False),
        sa.Column("aum_cr", sa.Numeric(18, 4), nullable=True),
        sa.Column("units", sa.Numeric(18, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("nav > 0", name="chk_portfolio_nav_positive"),
    )
    op.create_index("ix_de_portfolio_nav_portfolio_id", "de_portfolio_nav", ["portfolio_id"])

    op.create_table(
        "de_portfolio_transactions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("portfolio_id", sa.String(50), sa.ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="SET NULL"), nullable=True),
        sa.Column("symbol", sa.String(50), nullable=True),
        sa.Column("transaction_type", sa.String(20), nullable=False),
        sa.Column("quantity", sa.Numeric(18, 4), nullable=True),
        sa.Column("price", sa.Numeric(18, 4), nullable=True),
        sa.Column("amount", sa.Numeric(18, 4), nullable=True),
        sa.Column("source_ref", sa.String(200), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "transaction_type IN ('buy','sell','dividend','interest','fee','transfer_in','transfer_out','split','bonus')",
            name="chk_portfolio_txn_type",
        ),
        sa.UniqueConstraint(
            "portfolio_id", "trade_date", "instrument_id", "transaction_type", "source_ref",
            name="uq_portfolio_transactions",
        ),
    )
    op.create_index("ix_de_portfolio_transactions_portfolio_id", "de_portfolio_transactions", ["portfolio_id"])
    op.create_index("ix_de_portfolio_transactions_instrument_id", "de_portfolio_transactions", ["instrument_id"])

    op.create_table(
        "de_portfolio_holdings",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("portfolio_id", sa.String(50), sa.ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("symbol", sa.String(50), nullable=True),
        sa.Column("quantity", sa.Numeric(18, 4), nullable=True),
        sa.Column("avg_cost", sa.Numeric(18, 4), nullable=True),
        sa.Column("current_value", sa.Numeric(18, 4), nullable=True),
        sa.Column("weight_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("weight_pct BETWEEN 0 AND 100", name="chk_portfolio_holdings_weight"),
    )
    op.create_index("ix_de_portfolio_holdings_portfolio_id", "de_portfolio_holdings", ["portfolio_id"])
    op.create_index("ix_de_portfolio_holdings_instrument_id", "de_portfolio_holdings", ["instrument_id"])

    op.create_table(
        "de_portfolio_risk_metrics",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("portfolio_id", sa.String(50), sa.ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"), nullable=False, primary_key=True),
        sa.Column("cagr", sa.Numeric(10, 4), nullable=True),
        sa.Column("volatility", sa.Numeric(10, 4), nullable=True),
        sa.Column("sharpe_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("max_drawdown", sa.Numeric(10, 4), nullable=True),
        sa.Column("alpha", sa.Numeric(10, 4), nullable=True),
        sa.Column("beta", sa.Numeric(10, 4), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_de_portfolio_risk_metrics_portfolio_id", "de_portfolio_risk_metrics", ["portfolio_id"])

    # -------------------------------------------------------------------------
    # 17. Champion trades
    # -------------------------------------------------------------------------
    op.create_table(
        "de_champion_trades",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="SET NULL"), nullable=True),
        sa.Column("symbol", sa.String(50), nullable=True),
        sa.Column("direction", sa.String(10), nullable=False),
        sa.Column("entry_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("exit_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("quantity", sa.Numeric(18, 4), nullable=True),
        sa.Column("pnl", sa.Numeric(18, 4), nullable=True),
        sa.Column("stop_loss", sa.Numeric(18, 4), nullable=True),
        sa.Column("target_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("stage", sa.String(20), nullable=False, server_default="idea"),
        sa.Column("signal_type", sa.String(100), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("source_ref", sa.String(200), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("direction IN ('long','short','neutral')", name="chk_champion_trade_direction"),
        sa.CheckConstraint(
            "stage IN ('idea','active','partial_exit','closed','cancelled')",
            name="chk_champion_trade_stage",
        ),
        sa.UniqueConstraint("source_ref", name="uq_champion_trades_source_ref"),
    )
    op.create_index("ix_de_champion_trades_instrument_id", "de_champion_trades", ["instrument_id"])

    # -------------------------------------------------------------------------
    # 18. MF holdings
    # -------------------------------------------------------------------------
    op.create_table(
        "de_mf_holdings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("mstar_id", sa.String(20), sa.ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("holding_name", sa.String(500), nullable=True),
        sa.Column("isin", sa.String(12), nullable=True),
        sa.Column("instrument_id", UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="SET NULL"), nullable=True),
        sa.Column("weight_pct", sa.Numeric(6, 4), nullable=True),
        sa.Column("shares_held", sa.BigInteger, nullable=True),
        sa.Column("market_value", sa.Numeric(18, 4), nullable=True),
        sa.Column("sector_code", sa.String(50), nullable=True),
        sa.Column("is_mapped", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("mstar_id", "as_of_date", "isin", name="uq_mf_holdings"),
    )
    op.create_index("ix_de_mf_holdings_mstar_id", "de_mf_holdings", ["mstar_id"])
    op.create_index("ix_de_mf_holdings_instrument_id", "de_mf_holdings", ["instrument_id"])

    # -------------------------------------------------------------------------
    # 19. Request log
    # -------------------------------------------------------------------------
    op.create_table(
        "de_request_log",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("request_id", sa.String(100), nullable=True),
        sa.Column("actor", sa.String(100), nullable=True),
        sa.Column("source_ip", INET, nullable=True),
        sa.Column("method", sa.String(10), nullable=True),
        sa.Column("endpoint", sa.String(500), nullable=True),
        sa.Column("status_code", sa.Integer, nullable=True),
        sa.Column("duration_ms", sa.Integer, nullable=True),
        sa.Column("requested_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # -------------------------------------------------------------------------
    # 20. Seed data
    # -------------------------------------------------------------------------
    op.execute("""
        INSERT INTO de_contributors (name, role, is_admin, is_active) VALUES
        ('system', 'admin', true, true),
        ('pipeline_bot', 'pipeline', false, true)
        ON CONFLICT (name) DO NOTHING
    """)

    op.execute("""
        INSERT INTO de_system_flags (key, value, updated_by, updated_at, reason) VALUES
        ('price_pipeline_enabled', true, 'system', NOW(), 'Default enabled at init'),
        ('nav_pipeline_enabled', true, 'system', NOW(), 'Default enabled at init'),
        ('flow_pipeline_enabled', true, 'system', NOW(), 'Default enabled at init'),
        ('qual_pipeline_enabled', true, 'system', NOW(), 'Default enabled at init'),
        ('recompute_worker_enabled', true, 'system', NOW(), 'Default enabled at init'),
        ('regime_computation_enabled', true, 'system', NOW(), 'Default enabled at init')
        ON CONFLICT (key) DO NOTHING
    """)


def downgrade() -> None:
    # Drop in reverse dependency order
    op.execute("DROP TABLE IF EXISTS de_request_log CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_holdings CASCADE")
    op.execute("DROP TABLE IF EXISTS de_champion_trades CASCADE")
    op.execute("DROP TABLE IF EXISTS de_portfolio_risk_metrics CASCADE")
    op.execute("DROP TABLE IF EXISTS de_portfolio_holdings CASCADE")
    op.execute("DROP TABLE IF EXISTS de_portfolio_transactions CASCADE")
    op.execute("DROP TABLE IF EXISTS de_portfolio_nav CASCADE")
    op.execute("DROP TABLE IF EXISTS de_portfolios CASCADE")
    op.execute("DROP TABLE IF EXISTS de_pii_access_log CASCADE")
    op.execute("DROP TABLE IF EXISTS de_client_keys CASCADE")
    op.execute("DROP TABLE IF EXISTS de_clients CASCADE")
    op.execute("DROP TABLE IF EXISTS de_qual_outcomes CASCADE")
    op.execute("DROP TABLE IF EXISTS de_qual_extracts CASCADE")
    op.execute("DROP TABLE IF EXISTS de_qual_documents CASCADE")
    op.execute("DROP TABLE IF EXISTS de_qual_sources CASCADE")
    op.execute("DROP TABLE IF EXISTS de_data_anomalies CASCADE")
    op.execute("DROP TABLE IF EXISTS de_recompute_queue CASCADE")
    op.execute("DROP TABLE IF EXISTS de_fo_summary CASCADE")
    op.execute("DROP TABLE IF EXISTS de_breadth_daily CASCADE")
    op.execute("DROP TABLE IF EXISTS de_market_regime CASCADE")
    op.execute("DROP TABLE IF EXISTS de_rs_daily_summary CASCADE")
    op.execute("DROP TABLE IF EXISTS de_rs_scores CASCADE")
    op.execute("DROP TABLE IF EXISTS de_equity_technical_daily CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_category_flows CASCADE")
    op.execute("DROP TABLE IF EXISTS de_institutional_flows CASCADE")
    op.execute("DROP TABLE IF EXISTS de_macro_values CASCADE")
    op.execute("DROP TABLE IF EXISTS de_global_prices CASCADE")
    op.execute("DROP TABLE IF EXISTS de_index_prices CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_lifecycle CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_dividends CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_nav_daily CASCADE")
    op.execute("DROP TABLE IF EXISTS de_adjustment_factors_daily CASCADE")
    op.execute("DROP TABLE IF EXISTS de_corporate_actions CASCADE")
    op.execute("DROP TABLE IF EXISTS de_equity_ohlcv CASCADE")
    op.execute("DROP TABLE IF EXISTS de_index_constituents CASCADE")
    op.execute("DROP TABLE IF EXISTS de_symbol_history CASCADE")
    op.execute("DROP TABLE IF EXISTS de_market_cap_history CASCADE")
    op.execute("DROP TABLE IF EXISTS de_system_flags CASCADE")
    op.execute("DROP TABLE IF EXISTS de_migration_errors CASCADE")
    op.execute("DROP TABLE IF EXISTS de_migration_log CASCADE")
    op.execute("DROP TABLE IF EXISTS de_pipeline_log CASCADE")
    op.execute("DROP TABLE IF EXISTS de_source_files CASCADE")
    op.execute("DROP TABLE IF EXISTS de_mf_master CASCADE")
    op.execute("DROP TABLE IF EXISTS de_index_master CASCADE")
    op.execute("DROP TABLE IF EXISTS de_instrument CASCADE")
    op.execute("DROP TABLE IF EXISTS de_global_instrument_master CASCADE")
    op.execute("DROP TABLE IF EXISTS de_macro_master CASCADE")
    op.execute("DROP TABLE IF EXISTS de_trading_calendar CASCADE")
    op.execute("DROP TABLE IF EXISTS de_contributors CASCADE")
