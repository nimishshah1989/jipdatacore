"""Expand de_global_instrument_master instrument_type check constraint.

Adds 'bond', 'commodity', 'forex', 'crypto' to the allowed values so that
yfinance can ingest treasury yield indices, commodity futures, additional FX
pairs, and crypto tickers.

Revision ID: 002
Revises: 001
Create Date: 2026-04-08 00:00:00.000000
"""

from __future__ import annotations

from alembic import op

# revision identifiers
revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the old constraint and recreate with expanded value list.
    op.drop_constraint(
        "chk_global_instrument_type",
        "de_global_instrument_master",
        type_="check",
    )
    op.create_check_constraint(
        "chk_global_instrument_type",
        "de_global_instrument_master",
        "instrument_type IN ('index','etf','bond','commodity','forex','crypto')",
    )


def downgrade() -> None:
    # Revert to original narrow constraint (removes bond/commodity/forex/crypto rows first).
    op.drop_constraint(
        "chk_global_instrument_type",
        "de_global_instrument_master",
        type_="check",
    )
    op.create_check_constraint(
        "chk_global_instrument_type",
        "de_global_instrument_master",
        "instrument_type IN ('index','etf')",
    )
