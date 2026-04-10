"""Goldilocks Intelligence Engine — schema additions.

Drop raw-SQL goldilocks tables, recreate Alembic-managed.
Add new computation tables, alter existing tables.

Revision ID: 003_goldilocks
Revises: 002_expand_global_instrument_type
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "003_goldilocks"
down_revision = "002_expand_global_instrument_type"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── Drop existing raw-SQL goldilocks tables (empty, safe) ──
    op.execute("DROP TABLE IF EXISTS de_goldilocks_stock_ideas CASCADE")
    op.execute("DROP TABLE IF EXISTS de_goldilocks_sector_view CASCADE")
    op.execute("DROP TABLE IF EXISTS de_goldilocks_market_view CASCADE")

    # ── 1. de_goldilocks_market_view ──
    op.create_table(
        "de_goldilocks_market_view",
        sa.Column("report_date", sa.Date, primary_key=True),
        sa.Column("nifty_close", sa.Numeric(18, 4)),
        sa.Column("nifty_support_1", sa.Numeric(18, 4)),
        sa.Column("nifty_support_2", sa.Numeric(18, 4)),
        sa.Column("nifty_resistance_1", sa.Numeric(18, 4)),
        sa.Column("nifty_resistance_2", sa.Numeric(18, 4)),
        sa.Column("bank_nifty_close", sa.Numeric(18, 4)),
        sa.Column("bank_nifty_support_1", sa.Numeric(18, 4)),
        sa.Column("bank_nifty_support_2", sa.Numeric(18, 4)),
        sa.Column("bank_nifty_resistance_1", sa.Numeric(18, 4)),
        sa.Column("bank_nifty_resistance_2", sa.Numeric(18, 4)),
        sa.Column("trend_direction", sa.String(20)),
        sa.Column("trend_strength", sa.Integer),
        sa.Column("headline", sa.Text),
        sa.Column("overall_view", sa.Text),
        sa.Column("global_impact", sa.String(20)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("trend_direction IN ('upward','downward','sideways')", name="chk_gl_mv_trend_dir"),
        sa.CheckConstraint("trend_strength BETWEEN 1 AND 5", name="chk_gl_mv_trend_str"),
        sa.CheckConstraint("global_impact IN ('positive','negative','neutral')", name="chk_gl_mv_global"),
    )

    # ── 2. de_goldilocks_sector_view ──
    op.create_table(
        "de_goldilocks_sector_view",
        sa.Column("report_date", sa.Date, primary_key=True),
        sa.Column("sector", sa.String(100), primary_key=True),
        sa.Column("trend", sa.String(20)),
        sa.Column("outlook", sa.Text),
        sa.Column("rank", sa.Integer),
        sa.Column("top_picks", JSONB),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # ── 3. de_goldilocks_stock_ideas ──
    op.create_table(
        "de_goldilocks_stock_ideas",
        sa.Column("id", sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("document_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_qual_documents.id", ondelete="SET NULL"), index=True),
        sa.Column("published_date", sa.Date),
        sa.Column("symbol", sa.String(20)),
        sa.Column("company_name", sa.String(200)),
        sa.Column("idea_type", sa.String(20)),
        sa.Column("entry_price", sa.Numeric(18, 4)),
        sa.Column("entry_zone_low", sa.Numeric(18, 4)),
        sa.Column("entry_zone_high", sa.Numeric(18, 4)),
        sa.Column("target_1", sa.Numeric(18, 4)),
        sa.Column("target_2", sa.Numeric(18, 4)),
        sa.Column("lt_target", sa.Numeric(18, 4)),
        sa.Column("stop_loss", sa.Numeric(18, 4)),
        sa.Column("timeframe", sa.String(50)),
        sa.Column("rationale", sa.Text),
        sa.Column("technical_params", JSONB),
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
        sa.Column("status_updated_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("idea_type IN ('stock_bullet','big_catch')", name="chk_gl_idea_type"),
        sa.CheckConstraint("status IN ('active','target_1_hit','target_2_hit','sl_hit','expired','closed')", name="chk_gl_idea_status"),
    )

    # ── 4. de_oscillator_weekly ──
    op.create_table(
        "de_oscillator_weekly",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("instrument_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), primary_key=True, index=True),
        sa.Column("stochastic_k", sa.Numeric(8, 4)),
        sa.Column("stochastic_d", sa.Numeric(8, 4)),
        sa.Column("rsi_14", sa.Numeric(8, 4)),
        sa.Column("disparity_20", sa.Numeric(8, 4)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # ── 5. de_oscillator_monthly ──
    op.create_table(
        "de_oscillator_monthly",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("instrument_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), primary_key=True, index=True),
        sa.Column("stochastic_k", sa.Numeric(8, 4)),
        sa.Column("stochastic_d", sa.Numeric(8, 4)),
        sa.Column("rsi_14", sa.Numeric(8, 4)),
        sa.Column("disparity_20", sa.Numeric(8, 4)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # ── 6. de_divergence_signals ──
    op.create_table(
        "de_divergence_signals",
        sa.Column("id", sa.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("instrument_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("timeframe", sa.String(10), nullable=False),
        sa.Column("divergence_type", sa.String(20), nullable=False),
        sa.Column("indicator", sa.String(20), nullable=False),
        sa.Column("price_direction", sa.String(20)),
        sa.Column("indicator_direction", sa.String(20)),
        sa.Column("strength", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("timeframe IN ('daily','weekly','monthly')", name="chk_div_timeframe"),
        sa.CheckConstraint("divergence_type IN ('bullish','bearish','triple_bullish','triple_bearish')", name="chk_div_type"),
        sa.CheckConstraint("indicator IN ('rsi','stochastic','macd')", name="chk_div_indicator"),
    )

    # ── 7. de_fib_levels ──
    op.create_table(
        "de_fib_levels",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("instrument_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), primary_key=True, index=True),
        sa.Column("swing_high", sa.Numeric(18, 4)),
        sa.Column("swing_low", sa.Numeric(18, 4)),
        sa.Column("fib_236", sa.Numeric(18, 4)),
        sa.Column("fib_382", sa.Numeric(18, 4)),
        sa.Column("fib_500", sa.Numeric(18, 4)),
        sa.Column("fib_618", sa.Numeric(18, 4)),
        sa.Column("fib_786", sa.Numeric(18, 4)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # ── 8. de_index_pivots ──
    op.create_table(
        "de_index_pivots",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("index_code", sa.String(30), primary_key=True),
        sa.Column("pivot", sa.Numeric(18, 4)),
        sa.Column("s1", sa.Numeric(18, 4)),
        sa.Column("s2", sa.Numeric(18, 4)),
        sa.Column("s3", sa.Numeric(18, 4)),
        sa.Column("r1", sa.Numeric(18, 4)),
        sa.Column("r2", sa.Numeric(18, 4)),
        sa.Column("r3", sa.Numeric(18, 4)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # ── 9. de_intermarket_ratios ──
    op.create_table(
        "de_intermarket_ratios",
        sa.Column("date", sa.Date, primary_key=True),
        sa.Column("ratio_name", sa.String(50), primary_key=True),
        sa.Column("value", sa.Numeric(18, 6)),
        sa.Column("sma_20", sa.Numeric(18, 6)),
        sa.Column("direction", sa.String(10)),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("direction IN ('rising','falling','flat')", name="chk_imr_direction"),
    )

    # ── 10. ALTER de_qual_documents — add report_type ──
    op.add_column("de_qual_documents", sa.Column("report_type", sa.String(30)))

    # ── 11. ALTER de_equity_technical_daily — add new columns ──
    op.add_column("de_equity_technical_daily", sa.Column("stochastic_k", sa.Numeric(8, 4)))
    op.add_column("de_equity_technical_daily", sa.Column("stochastic_d", sa.Numeric(8, 4)))
    op.add_column("de_equity_technical_daily", sa.Column("disparity_20", sa.Numeric(8, 4)))
    op.add_column("de_equity_technical_daily", sa.Column("disparity_50", sa.Numeric(8, 4)))
    op.add_column("de_equity_technical_daily", sa.Column("bollinger_width", sa.Numeric(8, 4)))


def downgrade() -> None:
    # Remove added columns
    op.drop_column("de_equity_technical_daily", "bollinger_width")
    op.drop_column("de_equity_technical_daily", "disparity_50")
    op.drop_column("de_equity_technical_daily", "disparity_20")
    op.drop_column("de_equity_technical_daily", "stochastic_d")
    op.drop_column("de_equity_technical_daily", "stochastic_k")
    op.drop_column("de_qual_documents", "report_type")

    # Drop new tables
    op.drop_table("de_intermarket_ratios")
    op.drop_table("de_index_pivots")
    op.drop_table("de_fib_levels")
    op.drop_table("de_divergence_signals")
    op.drop_table("de_oscillator_monthly")
    op.drop_table("de_oscillator_weekly")
    op.drop_table("de_goldilocks_stock_ideas")
    op.drop_table("de_goldilocks_sector_view")
    op.drop_table("de_goldilocks_market_view")
