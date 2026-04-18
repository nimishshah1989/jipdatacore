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
    # ── Equity (daily, weekday EOD 18:33 IST) ──
    TableSpec("de_equity_ohlcv_y2026", "equity_bhav", "eod", "15 19 * * 1-5", "date", "equity", "P0", 24, 4),
    TableSpec("de_corporate_actions", "equity_corporate_actions", "eod", "15 19 * * 1-5", "ex_date", "equity", "P1", 168, 24),
    TableSpec("de_equity_technical_daily", "equity_technicals_sql", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P0", 24, 4),
    TableSpec("de_rs_scores", "relative_strength", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P0", 24, 4),
    TableSpec("de_rs_daily_summary", "relative_strength", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P1", 24, 4),
    TableSpec("de_breadth_daily", "market_breadth", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P0", 24, 4),
    TableSpec("de_sector_breadth_daily", "sector_breadth", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P1", 24, 4),
    # ── Indices ──
    TableSpec("de_index_prices", "nse_indices", "eod", "15 19 * * 1-5", "date", "equity", "P0", 24, 4),
    TableSpec("de_index_constituents", "index_constituents", "weekly_indices", "0 19 * * 6", "effective_from", "equity", "P2", 168, 48),
    TableSpec("de_index_technical_daily", "equity_technicals_sql", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P1", 24, 4),
    # ── F&O ──
    TableSpec("de_fo_bhavcopy", "fo_bhavcopy", "eod", "15 19 * * 1-5", "trade_date", "equity", "P1", 24, 4),
    TableSpec("de_fo_ban_list", "fo_ban_list", "eod", "15 19 * * 1-5", "business_date", "equity", "P1", 24, 4),
    TableSpec("de_participant_oi", "participant_oi", "eod", "15 19 * * 1-5", "trade_date", "equity", "P1", 24, 4),
    TableSpec("de_fo_summary", "fo_summary", "fo_summary", "30 20 * * 1-5", "date", "equity", "P1", 24, 4),
    # ── MF (NAV daily, late 23:00 IST after AMFI publishes) ──
    TableSpec("de_mf_nav_daily_y2026", "amfi_nav", "amfi_late", "30 23 * * 1-5", "nav_date", "mf", "P0", 36, 12),
    TableSpec("de_mf_derived_daily", "mf_derived", "nightly_compute", "30 0 * * 2-6", "nav_date", "computed", "P0", 36, 12),
    TableSpec("de_mf_technical_daily", "mf_derived", "nightly_compute", "30 0 * * 2-6", "nav_date", "computed", "P1", 36, 12),
    TableSpec("de_mf_holdings", "morningstar_portfolio", "holdings_monthly", "0 21 1 * *", "as_of_date", "mf", "P1", 30 * 24, 7 * 24),
    TableSpec("de_mf_category_flows", "mf_category_flows", "holdings_monthly", "0 21 1 * *", "month_date", "mf", "P1", 45 * 24, 15 * 24),
    # ── ETF ──
    TableSpec("de_etf_ohlcv", "etf_prices", "eod", "15 19 * * 1-5", "date", "etf", "P1", 24, 4),
    TableSpec("de_etf_technical_daily", "etf_technicals", "nightly_compute", "30 0 * * 2-6", "date", "computed", "P1", 24, 4),
    # ── Global (yfinance + FRED) ──
    TableSpec("de_global_prices", "yfinance_global", "eod", "33 18 * * *", "date", "global", "P1", 24, 4),
    TableSpec("de_global_technical_daily", "global_technicals", "nightly_compute", "30 0 * * *", "date", "computed", "P1", 24, 4),
    TableSpec("de_macro_values", "fred_macro", "eod", "33 18 * * *", "date", "macro", "P2", 168, 48),
    # ── Macro (RBI / FBIL) ──
    TableSpec("de_gsec_yield", "gsec_yields", "eod", "15 19 * * 1-5", "yield_date", "macro", "P2", 48, 12),
    TableSpec("de_rbi_fx_rate", "rbi_fx_rates", "eod", "15 19 * * 1-5", "rate_date", "macro", "P2", 48, 12),
    TableSpec("de_rbi_policy_rate", "rbi_policy_rates", "macro_daily", "15 9 * * *", "effective_date", "macro", "P2", 7 * 24, 48),
    # ── Flows ──
    TableSpec("de_institutional_flows", "fii_dii_flows", "eod", "15 19 * * 1-5", "date", "flows", "P0", 24, 4),
    # ── Fundamentals ──
    TableSpec("de_shareholding_pattern", "shareholding_pattern", "filings_daily", "0 19 * * *", "as_of_date", "fundamentals", "P1", 30 * 24, 7 * 24),
    TableSpec("de_equity_fundamentals_history", "equity_fundamentals", "fundamentals_weekly", "30 23 * * 6", "fiscal_period_end", "fundamentals", "P1", 8 * 24, 24),
    # ── BSE filings ──
    TableSpec("de_bse_announcements", "bse_filings", "bse_filings_daily", "0 18 * * 1-5", "announcement_dt", "qualitative", "P1", 24, 4),
    TableSpec("de_bse_corp_actions", "bse_filings", "bse_filings_daily", "0 18 * * 1-5", "ex_date", "qualitative", "P1", 168, 48),
    TableSpec("de_bse_result_calendar", "bse_filings", "bse_filings_daily", "0 18 * * 1-5", "result_date", "qualitative", "P1", 168, 48),
    # ── Insider trades + bulk/block deals ──
    TableSpec("de_insider_trades", "insider_trades", "eod", "15 19 * * 1-5", "disclosure_date", "qualitative", "P1", 24, 4),
    TableSpec("de_bulk_block_deals", "bulk_block_deals", "eod", "15 19 * * 1-5", "deal_date", "qualitative", "P1", 24, 4),
    # ── Qualitative (RSS + Goldilocks) ──
    TableSpec("de_qual_documents", "qualitative_rss", "eod", "15 19 * * 1-5", "created_at", "qualitative", "P2", 24, 6),
    TableSpec("de_goldilocks_market_view", "__goldilocks_compute__", "nightly_compute", "30 0 * * 2-6", "report_date", "qualitative", "P2", 168, 24),
]


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
