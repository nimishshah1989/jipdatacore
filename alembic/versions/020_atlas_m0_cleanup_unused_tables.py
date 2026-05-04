"""Atlas-M0 Job 3 -- drop tables not consumed by Atlas.

Per ATLAS_M0_DATA_CORE_PREP section 4 plus architect sign-off (Nimish,
2026-05-04): Atlas requires only OHLCV history for stocks / MFs / ETFs /
indices / global, instrument masters, and instrument-to-sector / sizing
maps. Everything else is derived (and will be recomputed by Atlas) or
unused, so we drop it now to remove ambiguity and reclaim ~17.5 GB.

The migration is GATED by an env var so accidental `alembic upgrade head`
does NOT destroy data:

    export ATLAS_M0_CLEANUP_CONFIRM=drop_unused_jip_intel_tables
    alembic upgrade head

If the env var is missing or wrong, upgrade is a no-op and alembic_version
advances anyway -- subsequent re-runs are then no-ops too. To execute the
drops you MUST set the env var on the run that crosses 019 -> 020.

Downgrade is intentionally NOT implemented: re-deriving these tables
means re-running the prior JIP Intelligence compute. Restore from RDS
snapshot if this is run by mistake.

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

# Architect-approved drop list, 2026-05-04. Order: dependents before parents.
# Each entry is (table_name, use_cascade). CASCADE is on for any table that
# might still have FK dependents -- safer to nuke the FK than fail the drop
# halfway through and leave alembic_version in a half-state.
DROP_ORDER: list[tuple[str, bool]] = [
    # ------------------------------------------------------------------
    # Derived technicals / breadth / RS  (~15.3 GB total)
    # ------------------------------------------------------------------
    ("de_rs_scores", True),
    ("de_rs_daily_summary", True),
    ("de_equity_technical_daily", True),
    ("de_etf_technical_daily", True),
    ("de_global_technical_daily", True),
    ("de_index_technical_daily", True),
    ("de_mf_technical_daily", True),
    ("de_mf_derived_daily", True),
    ("de_mf_sector_exposure", True),
    ("de_breadth_daily", True),
    ("de_sector_breadth_daily", True),
    ("de_market_regime", True),
    # ------------------------------------------------------------------
    # F&O  (~103 MB)
    # ------------------------------------------------------------------
    ("de_fo_bhavcopy", True),
    ("de_fo_ban_list", True),
    ("de_fo_summary", True),
    ("de_participant_oi", True),
    # ------------------------------------------------------------------
    # Filings / disclosures  (~24 MB)
    # ------------------------------------------------------------------
    ("de_bse_announcements", True),
    ("de_bse_corp_actions", True),
    ("de_bse_insider_trades", True),
    ("de_bse_pledge_history", True),
    ("de_bse_result_calendar", True),
    ("de_bse_sast_disclosures", True),
    ("de_bse_shareholding", True),
    ("de_shareholding_pattern", True),
    ("de_insider_trades", True),
    ("de_bulk_block_deals", True),
    # ------------------------------------------------------------------
    # Macro / rates / flows  (~14 MB)
    # ------------------------------------------------------------------
    ("de_macro_values", True),
    ("de_macro_master", True),
    ("de_gsec_yield", True),
    ("de_rbi_fx_rate", True),
    ("de_rbi_policy_rate", True),
    ("de_institutional_flows", True),
    ("de_mf_category_flows", True),
    ("de_intermarket_ratios", True),
    # ------------------------------------------------------------------
    # Goldilocks (prior intelligence engine)  (~3 MB)
    # ------------------------------------------------------------------
    ("de_goldilocks_market_view", True),
    ("de_goldilocks_sector_view", True),
    ("de_goldilocks_stock_ideas", True),
    ("de_oscillator_weekly", True),
    ("de_oscillator_monthly", True),
    ("de_divergence_signals", True),
    ("de_fib_levels", True),
    ("de_index_pivots", True),
    # ------------------------------------------------------------------
    # Qualitative / news  (~9 MB)
    # ------------------------------------------------------------------
    ("de_qual_outcomes", True),
    ("de_qual_extracts", True),
    ("de_qual_documents", True),
    ("de_qual_sources", True),
    # ------------------------------------------------------------------
    # Champion-specific
    # ------------------------------------------------------------------
    ("de_champion_trades", True),
    # ------------------------------------------------------------------
    # Fundamentals (architect: drop and recreate fresh later)
    # ------------------------------------------------------------------
    ("de_equity_fundamentals_history", True),
    ("de_equity_fundamentals", True),
    # ------------------------------------------------------------------
    # Client / portfolio (architect: drop, recreate from scratch later)
    # ------------------------------------------------------------------
    ("de_portfolio_risk_metrics", True),
    ("de_portfolio_holdings", True),
    ("de_portfolio_transactions", True),
    ("de_portfolio_nav", True),
    ("de_portfolios", True),
    ("de_pii_access_log", True),
    ("de_client_keys", True),
    ("de_clients", True),
    # ------------------------------------------------------------------
    # Empty / orphaned 0-byte stubs (legacy schema names)
    # ------------------------------------------------------------------
    ("de_flow_daily", True),
    ("de_index_price_daily", True),
    ("de_equity_price_daily", True),
    ("de_global_price_daily", True),
]


def upgrade() -> None:
    confirm = os.environ.get(CONFIRM_ENV)
    if confirm != CONFIRM_VALUE:
        print(
            f"[020_atlas_m0_cleanup] No-op: set "
            f"{CONFIRM_ENV}={CONFIRM_VALUE} to actually drop. Per Atlas-M0 "
            "spec, cleanup requires explicit architect confirmation. "
            "Skipping (alembic_version still advances to 020).",
            flush=True,
        )
        return

    print(
        f"[020_atlas_m0_cleanup] {CONFIRM_ENV} matches -- dropping "
        f"{len(DROP_ORDER)} tables (architect-approved 2026-05-04).",
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
