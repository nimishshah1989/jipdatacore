"""Atlas-M0 Job 3 -- drop derived tables not consumed by Atlas.

Per ATLAS_M0_DATA_CORE_PREP section 4: JIP Data Core has derived tables
built for the prior JIP Intelligence methodology. Atlas computes its own
derivations, so these are candidates for removal.

Candidate tables (from spec section 4.1):
  Confirmed unused by Atlas:
    - de_rs_scores                  (~34k rows)
    - de_sector_breadth_daily       (~313k rows)
    - de_equity_technical_daily     (~11k rows)
    - de_mf_derived_daily
    - de_mf_sector_exposure         (~13k rows)
  Likely unused (Atlas doesn't consume):
    - de_fo_bhavcopy                (~368k rows)
    - de_bse_announcements          (~5k rows)
    - de_market_cap_history         (~12k rows)

Per spec section 4.2 default ("keep all if architect unanswered"), this
migration is GATED by an env var. Running `alembic upgrade head` is a no-op
unless the architect explicitly opts in:

    export ATLAS_M0_CLEANUP_CONFIRM=drop_unused_jip_intel_tables
    alembic upgrade head

The ATLAS_M0_CLEANUP_CONFIRM value must be exactly the magic string above so
running e.g. `=true` or `=1` does not accidentally drop tables.

Downgrade is intentionally NOT implemented -- re-deriving these tables means
re-running the prior JIP Intelligence compute against historical data. If
this migration is run by mistake, restore from backup.

Revision ID: 020_atlas_m0_cleanup
Revises: 019_atlas_m0_etf_holdings
Create Date: 2026-05-04
"""
from __future__ import annotations

import os

from alembic import op

revision = "020_atlas_m0_cleanup"
down_revision = "019_atlas_m0_etf_holdings"
branch_labels = None
depends_on = None

CONFIRM_ENV = "ATLAS_M0_CLEANUP_CONFIRM"
CONFIRM_VALUE = "drop_unused_jip_intel_tables"

# The dependency-respecting drop order. Children before parents.
# Each entry: (table_name, requires_cascade)
DROP_ORDER: list[tuple[str, bool]] = [
    # Derived tables built on prior JIP Intelligence methodology
    ("de_rs_scores", False),
    ("de_rs_daily_summary", False),
    ("de_sector_breadth_daily", False),
    ("de_breadth_daily", False),
    ("de_equity_technical_daily", False),
    ("de_mf_derived_daily", False),
    ("de_mf_sector_exposure", False),
    # Atlas-unused inventory
    ("de_fo_bhavcopy", False),
    ("de_fo_summary", False),
    ("de_bse_announcements", False),
    ("de_market_cap_history", True),  # FK from de_instrument cascade
]


def upgrade() -> None:
    confirm = os.environ.get(CONFIRM_ENV)
    if confirm != CONFIRM_VALUE:
        # Default: keep all tables. Print guidance and return.
        print(
            f"[020_atlas_m0_cleanup] No-op: set "
            f"{CONFIRM_ENV}={CONFIRM_VALUE} to actually drop. "
            "Per spec section 4.2 default, the cleanup requires explicit "
            "architect confirmation. Skipping.",
            flush=True,
        )
        return

    print(
        f"[020_atlas_m0_cleanup] {CONFIRM_ENV} matches -- dropping "
        f"{len(DROP_ORDER)} unused derived tables.",
        flush=True,
    )
    for table, cascade in DROP_ORDER:
        suffix = " CASCADE" if cascade else ""
        op.execute(f'DROP TABLE IF EXISTS "{table}"{suffix}')
        print(f"[020_atlas_m0_cleanup] dropped {table}{suffix}", flush=True)


def downgrade() -> None:
    raise RuntimeError(
        "Migration 020_atlas_m0_cleanup is intentionally one-way. "
        "Restore from RDS snapshot to recover the dropped tables, then "
        "re-run the prior JIP Intelligence derivation pipelines if needed."
    )
