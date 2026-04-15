"""Create de_sector_breadth_daily table for per-sector breadth rollups.

Revision ID: 014_sector_breadth_daily
Revises: 013_sector_mapping
Create Date: 2026-04-15
"""

from alembic import op
import sqlalchemy as sa

revision = "014_sector_breadth_daily"
down_revision = "013_sector_mapping"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "de_sector_breadth_daily",
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("sector", sa.String(100), nullable=False),
        # Counts
        sa.Column("stocks_total", sa.Integer, nullable=False),
        sa.Column("stocks_above_50dma", sa.Integer, nullable=False),
        sa.Column("stocks_above_200dma", sa.Integer, nullable=False),
        sa.Column("stocks_above_20ema", sa.Integer, nullable=False),
        # Percentages
        sa.Column("pct_above_50dma", sa.Numeric(6, 2), nullable=False),
        sa.Column("pct_above_200dma", sa.Numeric(6, 2), nullable=False),
        sa.Column("pct_above_20ema", sa.Numeric(6, 2), nullable=False),
        # Momentum split
        sa.Column("stocks_rsi_overbought", sa.Integer, nullable=False),
        sa.Column("stocks_rsi_oversold", sa.Integer, nullable=False),
        sa.Column("stocks_macd_bullish", sa.Integer, nullable=False),
        # Aggregate signals
        sa.Column("breadth_regime", sa.String(20), nullable=True),
        # Audit
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("date", "sector"),
    )

    op.create_index(
        "ix_sector_breadth_sector_date",
        "de_sector_breadth_daily",
        ["sector", sa.text("date DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_sector_breadth_sector_date", table_name="de_sector_breadth_daily")
    op.drop_table("de_sector_breadth_daily")
