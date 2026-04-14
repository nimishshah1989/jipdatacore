"""Add composite index on de_mf_holdings (as_of_date, instrument_id).

Revision ID: 009_mf_holdings_composite_idx
Revises: 008_indicators_v2
Create Date: 2026-04-14

Why:
- de_mf_holdings today has single-column indexes on instrument_id and on
  as_of_date, but no composite on (as_of_date, instrument_id). Queries
  that ask "which funds held stock X on snapshot date D" — common for
  sector breadth rollups, cross-fund exposure analysis, and the
  upcoming indicators-v2 fund breadth work — currently do a bitmap-OR
  of two single-column indexes or a seq-scan on the larger disclosure
  partitions. A composite cuts that to a single b-tree lookup.
- Uses CREATE INDEX CONCURRENTLY so it won't lock the table during
  creation. CONCURRENTLY cannot run inside a transaction, so this
  migration opts out via ``disable_ddl_transaction = True``.
- The upcoming v2 fund-level breadth rollup (post IND-C11) will query
  this path heavily; creating the index now avoids a slow first run.
- Note: a duplicate single-column index already exists on
  de_mf_holdings (``ix_de_mf_holdings_instrument_id`` and
  ``ix_mf_holdings_instrument`` — same column). Filed separately as
  hygiene debt, not addressed here.
"""

from alembic import op

revision = "009_mf_holdings_composite_idx"
down_revision = "008_indicators_v2"
branch_labels = None
depends_on = None

# CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
# This tells Alembic to execute the migration outside its usual wrapper.
disable_ddl_transaction = True


def upgrade() -> None:
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_de_mf_holdings_asof_instr "
        "ON de_mf_holdings (as_of_date, instrument_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_de_mf_holdings_asof_instr")
