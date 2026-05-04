"""Single source of truth: pipeline name → asset metadata.

Each TableSpec describes one de_* table:
  - which pipeline produces it
  - which schedule group fires it
  - cron expression (IST)
  - expected max lag (freshness SLA)
  - date column (for freshness check)
  - category + criticality (for dashboard grouping)

Adding a new table = add one row here. Dagster picks up everything else:
  asset, freshness policy, row-count check, schedule, alerts.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    """SLA + identity for one de_* table."""

    table: str
    pipeline: str
    schedule_group: str
    cron_expr: str  # IST timezone (Asia/Kolkata)
    date_col: str
    category: str  # equity / mf / etf / global / macro / qualitative / computed
    criticality: str  # P0 / P1 / P2
    max_lag_hours: int  # red threshold
    fresh_lag_hours: int  # green threshold (amber between fresh and max)
    rowcount_check: bool = True  # apply ±5% rowcount delta check


# ---------------------------------------------------------------------------
# Master table registry — every de_* table that has freshness SLA.
# Tables not listed are either: (a) static reference data, (b) join helpers,
# (c) audit trail (de_pipeline_log, de_cron_run themselves).
# Drift detector flags any de_* table missing from here.
# ---------------------------------------------------------------------------

TABLE_SPECS: list[TableSpec] = [
    # ── Equity (daily, weekday EOD 19:15 IST) ──
    TableSpec("de_equity_ohlcv_y2026", "equity_bhav", "eod", "15 19 * * 1-5", "date", "equity", "P0", 24, 4),
    TableSpec("de_corporate_actions", "equity_corporate_actions", "eod", "15 19 * * 1-5", "ex_date", "equity", "P1", 168, 24),
    # ── Indices ──
    TableSpec("de_index_prices", "nse_indices", "eod", "15 19 * * 1-5", "date", "equity", "P0", 24, 4),
    TableSpec("de_index_constituents", "index_constituents", "weekly_indices", "0 19 * * 6", "effective_from", "equity", "P2", 168, 48),
    # ── MF (NAV daily 23:30 IST after AMFI publishes; holdings monthly) ──
    TableSpec("de_mf_nav_daily_y2026", "amfi_nav", "amfi_late", "30 23 * * 1-5", "nav_date", "mf", "P0", 36, 12),
    TableSpec("de_mf_holdings", "morningstar_portfolio", "holdings_monthly", "0 21 1 * *", "as_of_date", "mf", "P1", 30 * 24, 7 * 24),
    # ── ETF ──
    TableSpec("de_etf_ohlcv", "etf_prices", "eod", "15 19 * * 1-5", "date", "etf", "P1", 24, 4),
    TableSpec("de_etf_holdings", "morningstar_portfolio", "holdings_monthly", "0 21 1 * *", "as_of_date", "etf", "P1", 30 * 24, 7 * 24),
    # ── Global (yfinance / Stooq) ──
    TableSpec("de_global_prices", "yfinance_global", "eod", "33 18 * * *", "date", "global", "P1", 24, 4),
]
# NOTE -- the following TableSpecs were removed in the Atlas-M0 cleanup
# (migration 020_atlas_m0_cleanup_unused_tables, 2026-05-04). Their target
# tables have been dropped from the database and Atlas computes its own
# derivations from raw OHLCV + holdings:
#   de_equity_technical_daily, de_etf_technical_daily,
#   de_global_technical_daily, de_index_technical_daily, de_mf_technical_daily,
#   de_rs_scores, de_rs_daily_summary, de_breadth_daily, de_sector_breadth_daily,
#   de_mf_derived_daily, de_mf_sector_exposure, de_market_regime,
#   de_fo_bhavcopy, de_fo_ban_list, de_fo_summary, de_participant_oi,
#   de_macro_values, de_macro_master, de_gsec_yield, de_rbi_fx_rate,
#   de_rbi_policy_rate, de_institutional_flows, de_mf_category_flows,
#   de_intermarket_ratios, de_bse_announcements, de_bse_corp_actions,
#   de_bse_result_calendar, de_insider_trades, de_bulk_block_deals,
#   de_shareholding_pattern, de_equity_fundamentals_history,
#   de_qual_documents, de_goldilocks_market_view.
# Re-add an entry here only if the corresponding table is recreated.


def all_specs() -> list[TableSpec]:
    return list(TABLE_SPECS)


def by_table(table: str) -> TableSpec | None:
    for s in TABLE_SPECS:
        if s.table == table:
            return s
    return None


def by_pipeline(pipeline: str) -> list[TableSpec]:
    return [s for s in TABLE_SPECS if s.pipeline == pipeline]


def categories() -> list[str]:
    return sorted({s.category for s in TABLE_SPECS})
