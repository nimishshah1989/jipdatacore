"""Add 3-year and 5-year risk metric columns to all v2 technical tables.

Revision ID: 011_multi_year_risk
Revises: 010_rename_v2_cols
Create Date: 2026-04-15

Atlas needs 1/3/5y windows for long-term risk screening. Current state has only
1-year variants. This migration adds sharpe/sortino/calmar/max_drawdown/volatility/
beta/information_ratio at 3y and 5y, plus treynor and downside_risk at 1y/3y/5y.
"""

from alembic import op

revision = "011_multi_year_risk"
down_revision = "010_rename_v2_cols"
branch_labels = None
depends_on = None

_TABLES = [
    "de_equity_technical_daily_v2",
    "de_etf_technical_daily_v2",
    "de_global_technical_daily_v2",
    "de_index_technical_daily",
    "de_mf_technical_daily",
]

_NEW_COLUMNS = [
    "sharpe_3y",
    "sharpe_5y",
    "sortino_3y",
    "sortino_5y",
    "calmar_3y",
    "calmar_5y",
    "max_drawdown_3y",
    "max_drawdown_5y",
    "volatility_3y",
    "volatility_5y",
    "beta_3y",
    "beta_5y",
    "information_ratio_3y",
    "information_ratio_5y",
    "treynor_1y",
    "treynor_3y",
    "treynor_5y",
    "downside_risk_1y",
    "downside_risk_3y",
    "downside_risk_5y",
]


def upgrade() -> None:
    for tbl in _TABLES:
        for col in _NEW_COLUMNS:
            op.execute(
                f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col} NUMERIC(10,4)"
            )


def downgrade() -> None:
    for tbl in _TABLES:
        for col in _NEW_COLUMNS:
            op.execute(
                f"ALTER TABLE {tbl} DROP COLUMN IF EXISTS {col}"
            )
