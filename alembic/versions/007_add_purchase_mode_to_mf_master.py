"""Add purchase_mode column to de_mf_master.

Revision ID: 007_purchase_mode
Revises: 006_embedding_384
Create Date: 2026-04-14

Why:
- Morningstar and mfpulse both expose a purchase_mode field indicating whether
  a fund scheme is available for Direct / Regular / Both purchase channels.
- Stored as Integer (Morningstar API numeric code) rather than Enum to avoid
  migration churn if codes expand. Application layer maps to human-readable label.
- No index added: sparse, low-cardinality column not used in WHERE clauses alone.
"""

from alembic import op
import sqlalchemy as sa

revision = "007_purchase_mode"
down_revision = "006_embedding_384"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "de_mf_master",
        sa.Column("purchase_mode", sa.Integer, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("de_mf_master", "purchase_mode")
