"""Create de_equity_fundamentals table for Screener.in scraped data.

Revision ID: 012_equity_fundamentals
Revises: 011_multi_year_risk
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "012_equity_fundamentals"
down_revision = "011_multi_year_risk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_equity_fundamentals",
        sa.Column("instrument_id", sa.UUID(as_uuid=True), sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        # Valuation
        sa.Column("market_cap_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("pe_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("pb_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("peg_ratio", sa.Numeric(10, 4), nullable=True),
        sa.Column("ev_ebitda", sa.Numeric(10, 4), nullable=True),
        # Profitability
        sa.Column("roe_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("roce_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("operating_margin_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("net_margin_pct", sa.Numeric(8, 4), nullable=True),
        # Balance sheet
        sa.Column("debt_to_equity", sa.Numeric(10, 4), nullable=True),
        sa.Column("interest_coverage", sa.Numeric(10, 4), nullable=True),
        sa.Column("current_ratio", sa.Numeric(10, 4), nullable=True),
        # Per-share
        sa.Column("eps_ttm", sa.Numeric(18, 4), nullable=True),
        sa.Column("book_value", sa.Numeric(18, 4), nullable=True),
        sa.Column("face_value", sa.Numeric(10, 2), nullable=True),
        sa.Column("dividend_per_share", sa.Numeric(18, 4), nullable=True),
        sa.Column("dividend_yield_pct", sa.Numeric(8, 4), nullable=True),
        # Ownership
        sa.Column("promoter_holding_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("pledged_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("fii_holding_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("dii_holding_pct", sa.Numeric(6, 2), nullable=True),
        # Growth
        sa.Column("revenue_growth_yoy_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("profit_growth_yoy_pct", sa.Numeric(10, 4), nullable=True),
        # 52-week
        sa.Column("high_52w", sa.Numeric(18, 4), nullable=True),
        sa.Column("low_52w", sa.Numeric(18, 4), nullable=True),
        # Audit
        sa.Column("source", sa.String(50), server_default="screener", nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("instrument_id", "as_of_date"),
    )
    op.create_index("ix_de_equity_fundamentals_date", "de_equity_fundamentals", ["as_of_date"])


def downgrade() -> None:
    op.drop_index("ix_de_equity_fundamentals_date")
    op.drop_table("de_equity_fundamentals")
