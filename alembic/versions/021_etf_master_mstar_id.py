"""Atlas-M0 -- add mstar_id link to de_etf_master.

Per architect direction (2026-05-04): de_etf_holdings should remain the
canonical store for ETF portfolio disclosures (clean separation from
de_mf_holdings). The blocker is that the Morningstar master service
(x6d9w6xxu0hmhrr4) returns mstar_id but NO Ticker datapoint, so the
universe ingest can't FK-resolve to de_etf_master.ticker.

Adding a nullable mstar_id column gives a stable cross-system handle.
The universe ingest will populate it via name-match for existing rows
and create new de_etf_master rows for ETFs we don't currently have
(synthetic ticker = mstar_id).

Revision ID: 021_etf_master_mstar_id
Revises: 020_atlas_m0_cleanup
Create Date: 2026-05-04
"""
from alembic import op
import sqlalchemy as sa

revision = "021_etf_master_mstar_id"
down_revision = "020_atlas_m0_cleanup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "de_etf_master",
        sa.Column("mstar_id", sa.String(20), nullable=True),
    )
    op.create_index(
        "ix_de_etf_master_mstar_id",
        "de_etf_master",
        ["mstar_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_de_etf_master_mstar_id", table_name="de_etf_master")
    op.drop_column("de_etf_master", "mstar_id")
