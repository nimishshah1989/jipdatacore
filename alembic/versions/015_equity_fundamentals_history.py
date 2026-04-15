"""Create de_equity_fundamentals_history for 10-year annual/quarterly financials.

Revision ID: 015_equity_fundamentals_history
Revises: 014_sector_breadth_daily
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "015_equity_fundamentals_history"
down_revision = "014_sector_breadth_daily"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_equity_fundamentals_history",
        sa.Column(
            "instrument_id", sa.UUID(as_uuid=True),
            sa.ForeignKey("de_instrument.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("fiscal_period_end", sa.Date, nullable=False),
        sa.Column("period_type", sa.String(10), nullable=False),
        # P&L
        sa.Column("revenue_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("expenses_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("operating_profit_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("opm_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("other_income_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("interest_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("depreciation_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("profit_before_tax_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("tax_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("net_profit_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("eps", sa.Numeric(18, 4), nullable=True),
        # Balance sheet
        sa.Column("equity_capital_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("reserves_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("borrowings_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("other_liabilities_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("fixed_assets_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("cwip_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("investments_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("other_assets_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("total_assets_cr", sa.Numeric(18, 2), nullable=True),
        # Cash flow
        sa.Column("cfo_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("cfi_cr", sa.Numeric(18, 2), nullable=True),
        sa.Column("cff_cr", sa.Numeric(18, 2), nullable=True),
        # Audit
        sa.Column("source", sa.String(50), server_default="screener", nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("instrument_id", "fiscal_period_end", "period_type"),
    )
    op.create_index(
        "ix_de_eq_fh_inst_type_period",
        "de_equity_fundamentals_history",
        ["instrument_id", "period_type", sa.text("fiscal_period_end DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_de_eq_fh_inst_type_period", table_name="de_equity_fundamentals_history")
    op.drop_table("de_equity_fundamentals_history")
