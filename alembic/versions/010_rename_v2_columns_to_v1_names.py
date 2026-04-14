"""Rename v2 technical-table columns to match existing v1 names.

Revision ID: 010_rename_v2_cols
Revises: 009_mf_holdings_composite_idx
Create Date: 2026-04-14

Why:
- IND-C5 diff against the live ``de_equity_technical_daily`` (v1) table
  showed that downstream consumers (``regime.py``, ``sectors.py``,
  ``post_qa.py``, ``market.py``, and the external MarketPulse / MFPulse
  / Champion platforms) read column names like ``bollinger_upper``,
  ``sharpe_1y``, ``beta_nifty`` that don't exist on the v2 tables.
- The v2 migration (008) followed pandas-ta-classic conventions (``bb_upper``,
  ``risk_sharpe_1y``, etc.). A transparent-rename cutover requires the
  v2 tables to expose the same column names as v1 so that downstream SQL
  keeps working byte-for-byte.
- Per Option A in the indicators-v2 status report (2026-04-14): rename
  v2 columns to v1 names is the lowest-risk path — zero downstream code
  changes, a single migration, and the internal ``strategy.yaml``
  ``output_columns`` map absorbs the rename as its new destination
  (``BBU_20_2.0: bollinger_upper`` instead of ``BBU_20_2.0: bb_upper``).

Columns renamed (where present — the ``DO`` block skips missing ones):
    bb_upper              -> bollinger_upper
    bb_lower              -> bollinger_lower
    bb_width              -> bollinger_width
    hv_20                 -> volatility_20d
    hv_60                 -> volatility_60d
    risk_sharpe_1y        -> sharpe_1y
    risk_sortino_1y       -> sortino_1y
    risk_calmar_1y        -> calmar_ratio
    risk_max_drawdown_1y  -> max_drawdown_1y
    risk_beta_nifty       -> beta_nifty

Columns retained under v2 names (no v1 counterpart):
    bb_middle, bb_pct_b, hv_252, risk_alpha_nifty, risk_omega,
    risk_information_ratio — plus all the brand-new pandas-ta indicators
    (aroon, supertrend, donchian, keltner, cci, tsi, zlma, kama, etc.).

Affects tables:
    de_equity_technical_daily_v2
    de_etf_technical_daily_v2
    de_global_technical_daily_v2
    de_index_technical_daily
    de_mf_technical_daily
"""

from alembic import op

revision = "010_rename_v2_cols"
down_revision = "009_mf_holdings_composite_idx"
branch_labels = None
depends_on = None


_TABLES = [
    "de_equity_technical_daily_v2",
    "de_etf_technical_daily_v2",
    "de_global_technical_daily_v2",
    "de_index_technical_daily",
    "de_mf_technical_daily",
]

# (from, to) pairs — applied in order where the source column exists.
_RENAMES = [
    ("bb_upper", "bollinger_upper"),
    ("bb_lower", "bollinger_lower"),
    ("bb_width", "bollinger_width"),
    ("hv_20", "volatility_20d"),
    ("hv_60", "volatility_60d"),
    ("risk_sharpe_1y", "sharpe_1y"),
    ("risk_sortino_1y", "sortino_1y"),
    ("risk_calmar_1y", "calmar_ratio"),
    ("risk_max_drawdown_1y", "max_drawdown_1y"),
    ("risk_beta_nifty", "beta_nifty"),
]


def upgrade() -> None:
    for tbl in _TABLES:
        for old, new in _RENAMES:
            op.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = '{tbl}'
                          AND column_name = '{old}'
                    ) THEN
                        ALTER TABLE {tbl} RENAME COLUMN {old} TO {new};
                    END IF;
                END $$;
                """
            )


def downgrade() -> None:
    for tbl in _TABLES:
        for old, new in _RENAMES:
            op.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = '{tbl}'
                          AND column_name = '{new}'
                    ) THEN
                        ALTER TABLE {tbl} RENAME COLUMN {new} TO {old};
                    END IF;
                END $$;
                """
            )
