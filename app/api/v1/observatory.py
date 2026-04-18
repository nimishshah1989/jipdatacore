"""
JIP Data Observatory API — public monitoring endpoints.

NOTE: These endpoints intentionally do NOT require JWT authentication.
They are designed for the internal monitoring dashboard, ops tooling,
and health checks that run outside the JWT context (e.g., cron checks,
SRE dashboards). All data returned is operational/structural metadata —
no financial data, no PII, no client data.

GET  /api/v1/observatory/pulse          — Data freshness per stream
GET  /api/v1/observatory/quality        — Quality check results
GET  /api/v1/observatory/coverage       — Table coverage matrix
GET  /api/v1/observatory/pipelines      — 7-day pipeline health
GET  /api/v1/observatory/dictionary     — Data dictionary
GET  /api/v1/observatory/health-action  — Self-healing: what's broken + how to fix
POST /api/v1/observatory/healing-result — Log a self-healing fix attempt
GET  /api/v1/observatory/agents         — Managed agent status
GET  /api/v1/observatory/daily-report   — Daily pipeline summary + 7-day uptime
GET  /api/v1/observatory/audit          — Live data audit: metrics × instruments × schedule × discrepancies
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.ext.asyncio import create_async_engine

from app.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/observatory", tags=["observatory"])

# Observatory uses its own connection to avoid session poisoning from shared pool.
_obs_engine = None


async def get_observatory_db():
    """Yield a fresh, isolated async connection for observatory queries."""
    global _obs_engine
    if _obs_engine is None:
        from app.config import get_settings
        _obs_engine = create_async_engine(
            get_settings().database_url, pool_size=2, max_overflow=0,
        )
    async with _obs_engine.connect() as conn:
        yield conn

# ---------------------------------------------------------------------------
# Static column descriptions for the data dictionary
# ---------------------------------------------------------------------------

COLUMN_DESCRIPTIONS: dict[str, dict[str, str]] = {
    "de_equity_ohlcv": {
        "date": "Trading date (NSE business day)",
        "instrument_id": "UUID reference to de_instrument",
        "symbol": "NSE ticker symbol (e.g. RELIANCE)",
        "open": "Opening price (adjusted)",
        "high": "Intraday high",
        "low": "Intraday low",
        "close": "Closing price",
        "close_adj": "Split/bonus-adjusted closing price",
        "volume": "Total traded volume (shares)",
        "delivery_vol": "Delivery volume (compulsory delivery)",
        "delivery_pct": "Delivery as % of total volume",
        "data_status": "raw | validated | quarantined",
    },
    "de_equity_technical_daily": {
        "date": "Calculation date",
        "instrument_id": "UUID reference to de_instrument",
        "sma_50": "50-day simple moving average",
        "sma_200": "200-day simple moving average",
        "ema_20": "20-day exponential moving average",
        "close_adj": "Adjusted close used for calculations",
        "above_50dma": "True if price > SMA-50 (computed column)",
        "above_200dma": "True if price > SMA-200 (computed column)",
    },
    "de_rs_score_daily": {
        "date": "RS calculation date",
        "instrument_id": "UUID reference to de_instrument",
        "rs_score": "Relative strength vs universe (0-100)",
        "rs_rank": "Rank within universe (1 = strongest)",
        "rs_1m": "1-month RS momentum",
        "rs_3m": "3-month RS momentum",
    },
    "de_market_breadth_daily": {
        "date": "Market breadth date",
        "advances": "Number of advancing stocks",
        "declines": "Number of declining stocks",
        "pct_above_50dma": "% of universe stocks above 50-DMA",
        "pct_above_200dma": "% of universe stocks above 200-DMA",
        "breadth_regime": "bull | neutral | bear",
    },
    "de_mf_nav": {
        "nav_date": "NAV date (business day)",
        "mstar_id": "Morningstar fund identifier",
        "scheme_code": "AMFI scheme code",
        "nav": "Net asset value (INR per unit)",
        "aum_cr": "AUM in crores INR",
    },
    "de_mf_derived_daily": {
        "nav_date": "Derived metrics date",
        "mstar_id": "Morningstar fund identifier",
        "rs_score_hw": "Holdings-weighted RS score",
        "manager_alpha_1y": "1-year manager alpha vs benchmark",
        "holdings_coverage_pct": "% of holdings with RS scores",
    },
    "de_mf_holdings": {
        "report_date": "Holdings report date (monthly)",
        "mstar_id": "Fund identifier",
        "isin": "ISIN of held security",
        "holding_pct": "Weight of holding in portfolio (%)",
        "market_value_cr": "Market value in crores INR",
    },
    "de_mf_category_flows": {
        "flow_date": "Flow date",
        "category": "AMFI fund category",
        "gross_purchase_cr": "Gross purchases in crores INR",
        "gross_redemption_cr": "Gross redemptions in crores INR",
        "net_flow_cr": "Net flows = purchases - redemptions",
    },
    "de_etf_ohlcv": {
        "date": "Trading date",
        "ticker": "ETF ticker symbol (FK to de_etf_master)",
        "open": "Opening price",
        "high": "Intraday high",
        "low": "Intraday low",
        "close": "Closing price",
        "volume": "Total traded volume",
    },
    "de_global_prices": {
        "price_date": "Price date",
        "ticker": "Yahoo Finance / FRED ticker",
        "close": "Closing/last price",
        "currency": "Price currency (USD, INR, etc.)",
        "source": "yfinance | fred | manual",
    },
    "de_macro_values": {
        "report_date": "Report/release date",
        "indicator": "Macro indicator code (e.g. CPI_YOY)",
        "value": "Indicator value",
        "frequency": "monthly | quarterly | annual",
        "source": "Data source (FRED, RBI, MOSPI, etc.)",
    },
    "de_qualitative_items": {
        "published_at": "Publication date/time",
        "source": "Data source (RSS feed, upload, etc.)",
        "title": "Item headline or title",
        "sentiment_score": "Claude API sentiment: -1 (negative) to +1 (positive)",
        "entities": "JSON array of extracted entities",
    },
    "de_instrument": {
        "id": "UUID primary key",
        "isin": "International Securities Identification Number",
        "symbol": "NSE trading symbol",
        "company_name": "Full registered company name",
        "industry": "NSE industry classification",
        "market_cap_cr": "Latest market cap in crores INR",
        "is_active": "True if currently listed",
    },
    "de_pipeline_log": {
        "id": "Auto-increment run ID",
        "pipeline_name": "Pipeline identifier",
        "business_date": "Business date processed",
        "run_number": "Retry count (1 = first run)",
        "status": "pending | running | success | partial | failed | skipped",
        "rows_processed": "Rows successfully written",
        "rows_failed": "Rows that failed validation",
        "error_detail": "Error message if status = failed",
    },
}

# ---------------------------------------------------------------------------
# Data stream definitions for pulse endpoint
# ---------------------------------------------------------------------------

STREAM_DEFINITIONS: list[dict[str, Any]] = [
    {
        "stream_id": "equity_ohlcv",
        "label": "Equity OHLCV",
        "table": "de_equity_ohlcv_y2026",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "equity_technicals",
        "label": "Equity Technicals",
        "table": "de_equity_technical_daily",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "rs_scores",
        "label": "RS Scores",
        "table": "de_rs_scores",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "market_breadth",
        "label": "Breadth / Regime",
        "table": "de_breadth_daily",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "mf_nav",
        "label": "MF NAV",
        "table": "de_mf_nav_daily_y2026",
        "date_col": "nav_date",
        "category": "mf",
    },
    {
        "stream_id": "mf_derived",
        "label": "MF Derived",
        "table": "de_mf_derived_daily",
        "date_col": "nav_date",
        "category": "mf",
    },
    {
        "stream_id": "mf_holdings",
        "label": "MF Holdings",
        "table": "de_mf_holdings",
        "date_col": "as_of_date",
        "category": "mf",
        # Morningstar publishes holdings monthly (~45 days lag from quarter end).
        "fresh_hours": 45 * 24,
        "stale_hours": 90 * 24,
    },
    {
        "stream_id": "mf_flows",
        "label": "MF Category Flows",
        "table": "de_mf_category_flows",
        "date_col": "month_date",
        "category": "mf",
        # AMFI publishes monthly category flows ~10–15 days after month-end.
        "fresh_hours": 45 * 24,
        "stale_hours": 75 * 24,
    },
    {
        "stream_id": "etf_ohlcv",
        "label": "ETF OHLCV",
        "table": "de_etf_ohlcv",
        "date_col": "date",
        "category": "etf",
    },
    {
        "stream_id": "global_prices",
        "label": "Global Prices",
        "table": "de_global_prices",
        "date_col": "date",
        "category": "global",
    },
    {
        "stream_id": "macro_values",
        "label": "Macro Values",
        "table": "de_macro_values",
        "date_col": "date",
        "category": "macro",
    },
    {
        "stream_id": "qualitative",
        "label": "Qualitative",
        "table": "de_qual_documents",
        "date_col": "created_at",
        "category": "qualitative",
    },
    # Goldilocks / computation-derived tables
    {
        "stream_id": "goldilocks_market_view",
        "label": "Goldilocks Market View",
        "table": "de_goldilocks_market_view",
        "date_col": "report_date",
        "category": "qualitative",
        # Goldilocks publishes irregularly (daily-ish for trend friend,
        # but gaps around weekends/holidays). Weekly SLA is appropriate.
        "fresh_hours": 7 * 24,
        "stale_hours": 14 * 24,
    },
    {
        "stream_id": "oscillator_weekly",
        "label": "Oscillators (Weekly)",
        "table": "de_oscillator_weekly",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "index_pivots",
        "label": "Index Pivots",
        "table": "de_index_pivots",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "intermarket_ratios",
        "label": "Intermarket Ratios",
        "table": "de_intermarket_ratios",
        "date_col": "date",
        "category": "global",
    },
    {
        "stream_id": "fib_levels",
        "label": "Fibonacci Levels",
        "table": "de_fib_levels",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "divergence_signals",
        "label": "Divergence Signals",
        "table": "de_divergence_signals",
        "date_col": "date",
        "category": "equity",
    },
    # Additional missing streams
    {
        "stream_id": "corporate_actions",
        "label": "Corporate Actions",
        "table": "de_corporate_actions",
        "date_col": "ex_date",
        "category": "equity",
    },
    {
        "stream_id": "market_regime",
        "label": "Market Regime",
        "table": "de_market_regime",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "index_prices",
        "label": "Index Prices",
        "table": "de_index_prices",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "institutional_flows",
        "label": "FII/DII Flows",
        "table": "de_institutional_flows",
        "date_col": "date",
        "category": "flows",
    },
    {
        "stream_id": "global_technicals",
        "label": "Global Technicals",
        "table": "de_global_technical_daily",
        "date_col": "date",
        "category": "global",
    },
    {
        "stream_id": "goldilocks_sector_view",
        "label": "Goldilocks Sector View",
        "table": "de_goldilocks_sector_view",
        "date_col": "report_date",
        "category": "qualitative",
        "fresh_hours": 14 * 24,  # sector views come in fortnightly/monthly
        "stale_hours": 30 * 24,
    },
    {
        "stream_id": "goldilocks_stock_ideas",
        "label": "Goldilocks Stock Ideas",
        "table": "de_goldilocks_stock_ideas",
        "date_col": "published_date",
        "category": "qualitative",
        "fresh_hours": 14 * 24,  # stock ideas are published ad-hoc
        "stale_hours": 30 * 24,
    },
    {
        "stream_id": "oscillator_monthly",
        "label": "Oscillators (Monthly)",
        "table": "de_oscillator_monthly",
        "date_col": "date",
        "category": "equity",
    },
    # ── Previously untracked tables ──
    {
        "stream_id": "adjustment_factors",
        "label": "Adjustment Factors",
        "table": "de_adjustment_factors_daily",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "mf_dividends",
        "label": "MF Dividends",
        "table": "de_mf_dividends",
        "date_col": "ex_date",
        "category": "mf",
    },
    {
        "stream_id": "fo_summary",
        "label": "F&O Summary",
        "table": "de_fo_summary",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "rs_daily_summary",
        "label": "RS Daily Summary",
        "table": "de_rs_daily_summary",
        "date_col": "date",
        "category": "equity",
    },
    {
        "stream_id": "champion_trades",
        "label": "Champion Trades",
        "table": "de_champion_trades",
        "date_col": "trade_date",
        "category": "equity",
    },
    # ── Atlas additions (derivatives, macro, filings) ──
    {
        "stream_id": "fo_bhavcopy",
        "label": "F&O Bhavcopy",
        "table": "de_fo_bhavcopy",
        "date_col": "trade_date",
        "category": "derivatives",
    },
    {
        "stream_id": "fo_ban_list",
        "label": "F&O Ban List",
        "table": "de_fo_ban_list",
        "date_col": "business_date",
        "category": "derivatives",
    },
    {
        "stream_id": "participant_oi",
        "label": "Participant OI",
        "table": "de_participant_oi",
        "date_col": "trade_date",
        "category": "derivatives",
    },
    {
        "stream_id": "gsec_yields",
        "label": "G-Sec Yields",
        "table": "de_gsec_yield",
        "date_col": "yield_date",
        "category": "macro",
    },
    {
        "stream_id": "rbi_fx_rates",
        "label": "RBI FX Reference Rates",
        "table": "de_rbi_fx_rate",
        "date_col": "rate_date",
        "category": "macro",
    },
    {
        "stream_id": "rbi_policy_rates",
        "label": "RBI Policy Rates",
        "table": "de_rbi_policy_rate",
        "date_col": "effective_date",
        "category": "macro",
    },
    {
        "stream_id": "insider_trades",
        "label": "Insider Trades (PIT)",
        "table": "de_insider_trades",
        "date_col": "disclosure_date",
        "category": "flows",
    },
    {
        "stream_id": "bulk_block_deals",
        "label": "Bulk & Block Deals",
        "table": "de_bulk_block_deals",
        "date_col": "deal_date",
        "category": "flows",
    },
    {
        "stream_id": "shareholding_pattern",
        "label": "Shareholding Pattern",
        "table": "de_shareholding_pattern",
        "date_col": "as_of_date",
        "category": "fundamentals",
    },
]


def _freshness_status(
    hours_old: Optional[float],
    stream_def: Optional[dict[str, Any]] = None,
) -> str:
    """Classify freshness based on hours since last update.

    Default (daily streams): fresh <36h, stale 36–96h, critical >96h.
    A stream definition may override via `fresh_hours` / `stale_hours`,
    which is how monthly (AMFI category flows) and weekly (Morningstar)
    streams avoid perpetually showing as critical.
    """
    if hours_old is None:
        return "unknown"
    if hours_old < 0:
        return "fresh"  # future-dated data (e.g. macro forecasts)

    fresh_h = 36.0
    stale_h = 96.0
    if stream_def:
        fresh_h = float(stream_def.get("fresh_hours", fresh_h))
        stale_h = float(stream_def.get("stale_hours", stale_h))

    if hours_old < fresh_h:
        return "fresh"
    if hours_old <= stale_h:
        return "stale"
    return "critical"


async def _table_exists(db: AsyncSession, table_name: str) -> bool:
    """Check if a table exists in the public schema."""
    result = await db.execute(
        sa.text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = 'public' AND table_name = :tname"
            ")"
        ),
        {"tname": table_name},
    )
    return result.scalar_one()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/pulse",
    status_code=status.HTTP_200_OK,
    summary="Data stream freshness — one card per stream",
)
async def get_pulse(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Query MAX(date) for each critical data stream and return freshness status.
    No auth required — this is the primary health check for the monitoring dashboard.
    """
    now_utc = datetime.now(tz=timezone.utc)
    streams: list[dict[str, Any]] = []

    # Pull row counts from pg_stat_user_tables in one shot (server-side aggregation)
    stat_result = await db.execute(
        sa.text(
            "SELECT relname, n_live_tup "
            "FROM pg_stat_user_tables "
            "WHERE schemaname = 'public'"
        )
    )
    row_counts: dict[str, int] = {row.relname: row.n_live_tup for row in stat_result.fetchall()}

    for stream in STREAM_DEFINITIONS:
        table = stream["table"]
        date_col = stream["date_col"]
        exists = await _table_exists(db, table)

        if not exists:
            streams.append(
                {
                    "stream_id": stream["stream_id"],
                    "label": stream["label"],
                    "category": stream["category"],
                    "table": table,
                    "row_count": 0,
                    "last_date": None,
                    "hours_old": None,
                    "status": "unknown",
                    "exists": False,
                }
            )
            continue

        max_result = await db.execute(
            sa.text(f"SELECT MAX({date_col}) AS max_date FROM {table}")  # noqa: S608
        )
        max_date = max_result.scalar_one_or_none()

        hours_old: Optional[float] = None
        if max_date is not None:
            if hasattr(max_date, "tzinfo") and max_date.tzinfo is not None:
                delta = now_utc - max_date
            else:
                # DATE column — compare as date
                from datetime import date as date_type

                if isinstance(max_date, date_type):
                    delta = now_utc.date() - max_date
                    hours_old = delta.days * 24.0
                else:
                    delta = now_utc - max_date.replace(tzinfo=timezone.utc)
                    hours_old = delta.total_seconds() / 3600.0

            if hours_old is None:
                hours_old = delta.total_seconds() / 3600.0

        streams.append(
            {
                "stream_id": stream["stream_id"],
                "label": stream["label"],
                "category": stream["category"],
                "table": table,
                "row_count": row_counts.get(table, 0),
                "last_date": str(max_date) if max_date is not None else None,
                "hours_old": round(hours_old, 1) if hours_old is not None else None,
                "status": _freshness_status(hours_old, stream),
                "exists": True,
            }
        )

    fresh = sum(1 for s in streams if s["status"] == "fresh")
    stale = sum(1 for s in streams if s["status"] == "stale")
    critical = sum(1 for s in streams if s["status"] in ("critical", "unknown"))

    overall = "healthy"
    if critical > 0:
        overall = "critical"
    elif stale > 0:
        overall = "degraded"

    return {
        "as_of": now_utc.isoformat(),
        "overall_status": overall,
        "summary": {"fresh": fresh, "stale": stale, "critical": critical},
        "streams": streams,
    }


@router.get(
    "/quality",
    status_code=status.HTTP_200_OK,
    summary="Quality check results grouped by category",
)
async def get_quality(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Query de_data_quality_checks for latest check results.
    Returns pass/warn/fail counts and individual check details.
    No auth required.
    """
    now_utc = datetime.now(tz=timezone.utc)

    table_exists = await _table_exists(db, "de_data_quality_checks")
    if not table_exists:
        logger.info("observatory_quality_table_missing")
        return {
            "as_of": now_utc.isoformat(),
            "table_present": False,
            "summary": {"pass": 0, "warn": 0, "fail": 0},
            "by_category": {},
            "checks": [],
        }

    # Latest result per check_name (in case same check runs multiple times)
    rows_result = await db.execute(
        sa.text(
            """
            SELECT DISTINCT ON (check_name)
                check_name,
                check_category,
                check_status,
                actual_value,
                threshold_value,
                detail,
                checked_at
            FROM de_data_quality_checks
            ORDER BY check_name, checked_at DESC
            """
        )
    )
    rows = rows_result.fetchall()

    checks: list[dict[str, Any]] = []
    by_category: dict[str, dict[str, int]] = {}

    for row in rows:
        cat = row.check_category or "uncategorized"
        if cat not in by_category:
            by_category[cat] = {"pass": 0, "warn": 0, "fail": 0}

        chk_status = (row.check_status or "unknown").lower()
        if chk_status in by_category[cat]:
            by_category[cat][chk_status] += 1

        checks.append(
            {
                "check_name": row.check_name,
                "category": cat,
                "status": chk_status,
                "actual_value": str(row.actual_value) if row.actual_value is not None else None,
                "threshold_value": (
                    str(row.threshold_value) if row.threshold_value is not None else None
                ),
                "detail": row.detail,
                "checked_at": row.checked_at.isoformat() if row.checked_at else None,
            }
        )

    total_pass = sum(v["pass"] for v in by_category.values())
    total_warn = sum(v["warn"] for v in by_category.values())
    total_fail = sum(v["fail"] for v in by_category.values())

    return {
        "as_of": now_utc.isoformat(),
        "table_present": True,
        "summary": {"pass": total_pass, "warn": total_warn, "fail": total_fail},
        "by_category": by_category,
        "checks": checks,
    }


@router.get(
    "/coverage",
    status_code=status.HTTP_200_OK,
    summary="Coverage matrix — row counts and date ranges per table",
)
async def get_coverage(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Query pg_stat_user_tables for all de_* tables and return coverage info.
    For key tables also queries MIN/MAX date and exact COUNT.
    No auth required.
    """
    now_utc = datetime.now(tz=timezone.utc)

    # All de_* tables from pg_stat
    stat_result = await db.execute(
        sa.text(
            "SELECT relname, n_live_tup, last_analyze, last_autoanalyze "
            "FROM pg_stat_user_tables "
            "WHERE schemaname = 'public' AND relname LIKE 'de_%' "
            "ORDER BY n_live_tup DESC"
        )
    )
    stat_rows = stat_result.fetchall()

    # Key tables with date columns — get exact date ranges
    date_range_queries: list[dict[str, str]] = [
        s for s in STREAM_DEFINITIONS if s.get("date_col")
    ]

    date_ranges: dict[str, dict[str, Any]] = {}
    for q in date_range_queries:
        table = q["table"]
        date_col = q["date_col"]
        exists = await _table_exists(db, table)
        if not exists:
            continue
        try:
            dr = await db.execute(
                sa.text(
                    f"SELECT MIN({date_col}) AS min_d, MAX({date_col}) AS max_d, "  # noqa: S608
                    f"COUNT(*) AS cnt FROM {table}"
                )
            )
            row = dr.fetchone()
            if row:
                date_ranges[table] = {
                    "min_date": str(row.min_d) if row.min_d else None,
                    "max_date": str(row.max_d) if row.max_d else None,
                    "exact_count": row.cnt,
                }
        except Exception as exc:
            logger.warning("observatory_coverage_date_range_error", table=table, error=str(exc))

    tables: list[dict[str, Any]] = []
    for row in stat_rows:
        entry: dict[str, Any] = {
            "table": row.relname,
            "row_count_approx": row.n_live_tup,
            "last_analyzed": (
                row.last_analyze.isoformat()
                if row.last_analyze
                else (row.last_autoanalyze.isoformat() if row.last_autoanalyze else None)
            ),
        }
        if row.relname in date_ranges:
            entry.update(date_ranges[row.relname])
        tables.append(entry)

    return {
        "as_of": now_utc.isoformat(),
        "table_count": len(tables),
        "tables": tables,
    }


@router.get(
    "/pipelines",
    status_code=status.HTTP_200_OK,
    summary="7-day pipeline health heatmap data",
)
async def get_pipelines(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Query de_pipeline_log for the last 7 days.
    Groups by pipeline_name and business_date, returns success/fail/duration matrix.
    No auth required.
    """
    now_utc = datetime.now(tz=timezone.utc)

    rows_result = await db.execute(
        sa.text(
            """
            SELECT
                pipeline_name,
                business_date,
                status,
                rows_processed,
                EXTRACT(EPOCH FROM (completed_at - started_at)) AS duration_secs,
                completed_at
            FROM de_pipeline_log
            WHERE business_date >= CURRENT_DATE - INTERVAL '7 days'
               OR (business_date IS NULL AND created_at >= NOW() - INTERVAL '7 days')
            ORDER BY pipeline_name, business_date DESC, created_at DESC
            """
        )
    )
    rows = rows_result.fetchall()

    # Aggregate per pipeline + date
    from collections import defaultdict

    pipeline_map: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for row in rows:
        p_name = row.pipeline_name
        b_date = str(row.business_date) if row.business_date else "no-date"

        if b_date not in pipeline_map[p_name]:
            pipeline_map[p_name][b_date] = {
                "date": b_date,
                "statuses": [],
                "total_rows": 0,
                "max_duration_secs": None,
                "last_run_at": None,
            }

        cell = pipeline_map[p_name][b_date]
        cell["statuses"].append(row.status)
        if row.rows_processed:
            cell["total_rows"] += row.rows_processed
        if row.duration_secs is not None:
            dur = float(row.duration_secs)
            if cell["max_duration_secs"] is None or dur > cell["max_duration_secs"]:
                cell["max_duration_secs"] = round(dur, 1)
        if row.completed_at and (
            cell["last_run_at"] is None
            or row.completed_at.isoformat() > cell["last_run_at"]
        ):
            cell["last_run_at"] = row.completed_at.isoformat()

    # Compute rolled-up status per cell
    def _cell_status(statuses: list[str]) -> str:
        if not statuses:
            return "not_run"
        if "success" in statuses:
            return "success"
        if "partial" in statuses:
            return "partial"
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        return statuses[-1]

    pipelines: list[dict[str, Any]] = []
    for p_name, date_map in sorted(pipeline_map.items()):
        cells = []
        for b_date, cell in sorted(date_map.items(), reverse=True):
            cells.append(
                {
                    "date": cell["date"],
                    "status": _cell_status(cell["statuses"]),
                    "total_rows": cell["total_rows"],
                    "duration_secs": cell["max_duration_secs"],
                    "last_run_at": cell["last_run_at"],
                }
            )
        pipelines.append({"pipeline_name": p_name, "days": cells})

    return {
        "as_of": now_utc.isoformat(),
        "window_days": 7,
        "pipeline_count": len(pipelines),
        "pipelines": pipelines,
    }


@router.get(
    "/dictionary",
    status_code=status.HTTP_200_OK,
    summary="Data dictionary — all de_* table columns with descriptions",
)
async def get_dictionary(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Query information_schema.columns for all de_* tables.
    Enriches with human-readable descriptions from the static dict.
    No auth required.
    """
    now_utc = datetime.now(tz=timezone.utc)

    cols_result = await db.execute(
        sa.text(
            """
            SELECT
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name LIKE 'de_%'
            ORDER BY table_name, ordinal_position
            """
        )
    )
    col_rows = cols_result.fetchall()

    columns: list[dict[str, Any]] = []
    for row in col_rows:
        tbl = row.table_name
        col = row.column_name
        description = COLUMN_DESCRIPTIONS.get(tbl, {}).get(col, "")
        columns.append(
            {
                "table": tbl,
                "column": col,
                "data_type": row.data_type,
                "is_nullable": row.is_nullable == "YES",
                "has_default": row.column_default is not None,
                "description": description,
            }
        )

    # Group by table
    from collections import defaultdict

    by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for col in columns:
        by_table[col["table"]].append(col)

    tables_list = [
        {
            "table": tbl,
            "column_count": len(cols),
            "columns": cols,
        }
        for tbl, cols in sorted(by_table.items())
    ]

    return {
        "as_of": now_utc.isoformat(),
        "table_count": len(tables_list),
        "column_count": len(columns),
        "tables": tables_list,
    }


# ---------------------------------------------------------------------------
# Stream → pipeline mapping for self-healing
# ---------------------------------------------------------------------------

STREAM_PIPELINE_MAP: dict[str, str] = {
    "equity_ohlcv": "equity_bhav",
    "equity_technicals": "equity_technicals_sql",
    "rs_scores": "relative_strength",
    "market_breadth": "market_breadth",
    "market_regime": "market_breadth",
    "mf_nav": "amfi_nav",
    "mf_derived": "mf_derived",
    "mf_holdings": "morningstar_portfolio",
    "mf_flows": "mf_category_flows",
    "etf_ohlcv": "etf_prices",
    "global_prices": "yfinance_global",
    "global_technicals": "global_technicals",
    "macro_values": "fred_macro",
    "index_prices": "nse_indices",
    "institutional_flows": "fii_dii_flows",
    "corporate_actions": "nse_corporate_actions",
    "qualitative": "qualitative_rss",
    "goldilocks_market_view": "__goldilocks_compute__",
    "goldilocks_sector_view": "__goldilocks_compute__",
    "goldilocks_stock_ideas": "__goldilocks_compute__",
    "oscillator_weekly": "full_runner",
    "oscillator_monthly": "full_runner",
    "index_pivots": "full_runner",
    "intermarket_ratios": "full_runner",
    "fib_levels": "full_runner",
    "divergence_signals": "full_runner",
    "adjustment_factors": "equity_corporate_actions",
    "mf_dividends": "mf_eod",
    "fo_summary": "fo_summary",
    "rs_daily_summary": "relative_strength",
    "champion_trades": "__goldilocks_compute__",
    # Atlas additions
    "fo_bhavcopy": "fo_bhavcopy",
    "fo_ban_list": "fo_ban_list",
    "participant_oi": "participant_oi",
    "gsec_yields": "gsec_yields",
    "rbi_fx_rates": "rbi_fx_rates",
    "rbi_policy_rates": "rbi_policy_rates",
    "insider_trades": "insider_trades",
    "bulk_block_deals": "bulk_block_deals",
    "shareholding_pattern": "shareholding_pattern",
}


# ---------------------------------------------------------------------------
# Self-healing endpoints
# ---------------------------------------------------------------------------

@router.get("/health-action", status_code=status.HTTP_200_OK)
async def health_action(
    db: AsyncSession = Depends(get_observatory_db),
):
    """Return actionable fix list for stale/critical streams.

    Used by the self-healing agent (Agent 3) to know what to fix and how.
    """
    from datetime import date as date_type

    now_utc = datetime.now(tz=timezone.utc)
    today = date_type.today()
    actions: list[dict[str, Any]] = []

    # Get current pulse data
    for stream_def in STREAM_DEFINITIONS:
        sid = stream_def["stream_id"]
        table = stream_def["table"]
        date_col = stream_def["date_col"]

        # Check if table exists
        try:
            exists_result = await db.execute(
                sa.text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_name = :tbl)"
                ),
                {"tbl": table},
            )
            if not exists_result.scalar_one():
                continue
        except Exception:
            continue

        # Get latest date
        try:
            max_result = await db.execute(
                sa.text(f"SELECT MAX({date_col}) FROM {table}")  # noqa: S608
            )
            max_date = max_result.scalar_one_or_none()
        except Exception:
            max_date = None

        if max_date is None:
            stream_status = "unknown"
            hours_old = None
        else:
            if hasattr(max_date, "timestamp"):
                hours_old = (now_utc - max_date.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            else:
                from datetime import datetime as dt
                max_dt = dt.combine(max_date, dt.min.time()).replace(tzinfo=timezone.utc)
                hours_old = (now_utc - max_dt).total_seconds() / 3600
            stream_status = _freshness_status(hours_old, stream_def)

        if stream_status in ("stale", "critical", "unknown"):
            pipeline = STREAM_PIPELINE_MAP.get(sid)
            if pipeline is None:
                continue

            # Check today's healing attempts
            try:
                heal_result = await db.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM de_healing_log "
                        "WHERE date = :today AND stream_id = :sid"
                    ),
                    {"today": today, "sid": sid},
                )
                retries_today = heal_result.scalar_one()
            except Exception:
                retries_today = 0

            actions.append({
                "stream_id": sid,
                "status": stream_status,
                "hours_old": round(hours_old, 1) if hours_old else None,
                "last_date": str(max_date) if max_date else None,
                "pipeline_to_fix": pipeline,
                "retries_today": retries_today,
                "max_retries": 2,
                "should_fix": retries_today < 2,
            })

    return {
        "as_of": now_utc.isoformat(),
        "actions_needed": len(actions),
        "actions": actions,
    }


class HealingResultRequest(BaseModel):
    stream_id: str
    pipeline_triggered: str
    action: str  # trigger, retry, escalate
    result: str  # success, failed, timeout
    retries: int = 0
    error_detail: Optional[str] = None


@router.post("/healing-result", status_code=status.HTTP_201_CREATED)
async def record_healing_result(
    body: HealingResultRequest,
    db: AsyncSession = Depends(get_observatory_db),
):
    """Record a self-healing fix attempt by Agent 3."""
    from datetime import date as date_type

    await db.execute(
        sa.text(
            "INSERT INTO de_healing_log "
            "(date, stream_id, pipeline_triggered, action, result, retries, error_detail) "
            "VALUES (:date, :sid, :pipeline, :action, :result, :retries, :error)"
        ),
        {
            "date": date_type.today(),
            "sid": body.stream_id,
            "pipeline": body.pipeline_triggered,
            "action": body.action,
            "result": body.result,
            "retries": body.retries,
            "error": body.error_detail,
        },
    )
    await db.commit()

    return {"status": "recorded"}


# ---------------------------------------------------------------------------
# Semantic search over qualitative content (RAG retrieval)
# ---------------------------------------------------------------------------


@router.get("/search", status_code=status.HTTP_200_OK)
async def semantic_search(
    db: AsyncSession = Depends(get_observatory_db),
    q: str = "",
    k: int = 10,
    table: str = "extracts",  # "extracts" | "documents" | "both"
):
    """Semantic search over goldilocks + other qualitative content.

    Uses the local bge-small-en-v1.5 embedder (384 dim) and pgvector HNSW
    indexes for sub-100ms retrieval. Returns top-k most similar rows by
    cosine distance.

    Query params:
      q       — the search query text (required, non-empty)
      k       — number of results to return (default 10, max 50)
      table   — which table to search: 'extracts' (de_qual_extracts,
                default), 'documents' (de_qual_documents), or 'both'.
    """
    if not q or not q.strip():
        return {"error": "query 'q' is required", "results": []}
    k = max(1, min(50, int(k)))

    try:
        from app.pipelines.qualitative.local_embedder import (
            embed_texts,
            to_pgvector_literal,
        )
        qvec = embed_texts([q])[0]
        qlit = to_pgvector_literal(qvec)
    except Exception as exc:
        logger.error("search_embedder_unavailable", error=str(exc))
        return {"error": f"embedder unavailable: {exc}", "results": []}

    results: dict[str, Any] = {"query": q, "k": k}

    if table in ("extracts", "both"):
        try:
            rows = await db.execute(
                sa.text(
                    """
                    SELECT id::text, document_id::text, entity_ref, direction,
                           conviction, view_text, source_quote, quality_score,
                           1 - (embedding <=> (:qv)::vector) AS similarity
                    FROM de_qual_extracts
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> (:qv)::vector
                    LIMIT :k
                    """
                ),
                {"qv": qlit, "k": k},
            )
            results["extracts"] = [
                {
                    "id": r.id,
                    "document_id": r.document_id,
                    "entity_ref": r.entity_ref,
                    "direction": r.direction,
                    "conviction": r.conviction,
                    "view_text": r.view_text,
                    "source_quote": r.source_quote,
                    "quality_score": float(r.quality_score) if r.quality_score is not None else None,
                    "similarity": round(float(r.similarity), 4),
                }
                for r in rows.fetchall()
            ]
        except Exception as exc:
            logger.warning("search_extracts_failed", error=str(exc))
            results["extracts"] = []

    if table in ("documents", "both"):
        try:
            rows = await db.execute(
                sa.text(
                    """
                    SELECT id::text, title, report_type, published_at,
                           LEFT(raw_text, 400) AS snippet,
                           1 - (embedding <=> (:qv)::vector) AS similarity
                    FROM de_qual_documents
                    WHERE embedding IS NOT NULL
                    ORDER BY embedding <=> (:qv)::vector
                    LIMIT :k
                    """
                ),
                {"qv": qlit, "k": k},
            )
            results["documents"] = [
                {
                    "id": r.id,
                    "title": r.title,
                    "report_type": r.report_type,
                    "published_at": r.published_at.isoformat() if r.published_at else None,
                    "snippet": r.snippet,
                    "similarity": round(float(r.similarity), 4),
                }
                for r in rows.fetchall()
            ]
        except Exception as exc:
            logger.warning("search_documents_failed", error=str(exc))
            results["documents"] = []

    return results


# ---------------------------------------------------------------------------
# Cron runs endpoint — visibility into scheduled-job success/failure
# ---------------------------------------------------------------------------


@router.get("/cron-runs", status_code=status.HTTP_200_OK)
async def cron_runs(
    db: AsyncSession = Depends(get_observatory_db),
    hours: int = 48,
):
    """Return recent cron-job runs from de_cron_run.

    Backs the dashboard's Jobs panel: one row per scheduled run with
    started/finished timestamps, HTTP code, duration, and status. If the
    de_cron_run table does not yet exist (pre-005 migration), returns an
    empty list with a hint so the dashboard can render gracefully.
    """
    try:
        rows = await db.execute(
            sa.text(
                """
                SELECT id, schedule_name, business_date, started_at, finished_at,
                       duration_seconds, http_code, curl_exit_code, status,
                       error_body, host
                FROM de_cron_run
                WHERE started_at > now() - make_interval(hours => :hours)
                ORDER BY started_at DESC
                LIMIT 200
                """
            ),
            {"hours": hours},
        )
        runs = [
            {
                "id": r.id,
                "schedule_name": r.schedule_name,
                "business_date": r.business_date.isoformat() if r.business_date else None,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "duration_seconds": float(r.duration_seconds) if r.duration_seconds is not None else None,
                "http_code": r.http_code,
                "curl_exit_code": r.curl_exit_code,
                "status": r.status,
                "error_body": (r.error_body[:500] if r.error_body else None),
                "host": r.host,
            }
            for r in rows.fetchall()
        ]
        return {"hours": hours, "count": len(runs), "runs": runs, "table_exists": True}
    except Exception as exc:
        logger.warning("cron_runs_unavailable", error=str(exc))
        return {"hours": hours, "count": 0, "runs": [], "table_exists": False,
                "hint": "run alembic migration 005_cron_run"}


# ---------------------------------------------------------------------------
# Agent status endpoint
# ---------------------------------------------------------------------------

AGENT_DEFINITIONS = [
    {
        "agent_id": "jip-eod-ingestion",
        "name": "EOD Ingestion",
        "schedule": "18:33 IST, Mon-Fri",
        "pipelines": ["equity_bhav", "nse_indices", "equity_corporate_actions",
                       "fii_dii_flows", "mf_eod", "yfinance_global", "fred_macro",
                       "india_vix", "nse_etf_sync", "etf_prices"],
        "match_patterns": ["eod", "bhav", "amfi", "yfinance", "fred", "fii_dii", "etf_price", "nse_etf"],
    },
    {
        "agent_id": "jip-nightly-compute",
        "name": "Nightly Compute",
        "schedule": "19:33 IST, Mon-Fri",
        "pipelines": ["equity_technicals_sql", "relative_strength", "market_breadth",
                       "mf_derived", "etf_technicals", "global_technicals"],
        "match_patterns": ["nightly", "technicals", "rs_scores", "breadth", "regime",
                           "fund_metrics", "etf_", "global_"],
    },
    {
        "agent_id": "jip-health-check",
        "name": "Health + Self-Heal",
        "schedule": "23:33 IST, Daily",
        "pipelines": [],
        "match_patterns": ["health", "healing", "pulse"],
    },
]


@router.get("/agents", status_code=status.HTTP_200_OK)
async def agent_status(
    db: AsyncSession = Depends(get_observatory_db),
):
    """Return managed agent status with last-run info."""
    now_utc = datetime.now(tz=timezone.utc)
    agents: list[dict[str, Any]] = []

    for agent_def in AGENT_DEFINITIONS:
        # Find most recent pipeline log matching this agent's patterns
        last_run = None
        last_status = "unknown"

        for pattern in agent_def["match_patterns"]:
            try:
                result = await db.execute(
                    sa.text(
                        "SELECT pipeline_name, status, completed_at "
                        "FROM de_pipeline_log "
                        "WHERE pipeline_name ILIKE :pattern "
                        "ORDER BY completed_at DESC NULLS LAST LIMIT 1"
                    ),
                    {"pattern": f"%{pattern}%"},
                )
                row = result.fetchone()
                if row and row[2]:
                    if last_run is None or row[2] > last_run:
                        last_run = row[2]
                        last_status = row[1]
            except Exception:
                continue

        hours_since = None
        if last_run:
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=timezone.utc)
            hours_since = round((now_utc - last_run).total_seconds() / 3600, 1)

        agents.append({
            "agent_id": agent_def["agent_id"],
            "name": agent_def["name"],
            "schedule": agent_def["schedule"],
            "pipeline_count": len(agent_def["pipelines"]),
            "last_run": last_run.isoformat() if last_run else None,
            "last_status": last_status,
            "hours_since_last_run": hours_since,
            "health": "healthy" if hours_since and hours_since < 36 else
                      "stale" if hours_since and hours_since < 96 else "unknown",
        })

    return {
        "as_of": now_utc.isoformat(),
        "agents": agents,
    }


# ---------------------------------------------------------------------------
# Daily report endpoint
# ---------------------------------------------------------------------------

@router.get("/daily-report", status_code=status.HTTP_200_OK)
async def daily_report(
    db: AsyncSession = Depends(get_observatory_db),
):
    """Daily pipeline summary with 7-day rolling uptime per stream."""
    from datetime import date as date_type, timedelta

    now_utc = datetime.now(tz=timezone.utc)
    today = date_type.today()
    week_ago = today - timedelta(days=7)

    # Today's pipeline runs
    try:
        runs_result = await db.execute(
            sa.text(
                "SELECT pipeline_name, status, rows_processed, duration_seconds, error_detail "
                "FROM de_pipeline_log "
                "WHERE business_date = :today "
                "ORDER BY completed_at DESC NULLS LAST"
            ),
            {"today": today},
        )
        today_runs = [
            {
                "pipeline": r[0],
                "status": r[1],
                "rows": r[2],
                "duration_s": float(r[3]) if r[3] else 0,
                "error": str(r[4])[:200] if r[4] else None,
            }
            for r in runs_result.fetchall()
        ]
    except Exception:
        today_runs = []

    # Today's healing events
    try:
        heal_result = await db.execute(
            sa.text(
                "SELECT stream_id, action, result, error_detail "
                "FROM de_healing_log WHERE date = :today ORDER BY created_at DESC"
            ),
            {"today": today},
        )
        today_heals = [
            {"stream": r[0], "action": r[1], "result": r[2], "error": r[3]}
            for r in heal_result.fetchall()
        ]
    except Exception:
        today_heals = []

    # 7-day uptime per stream
    uptime_by_stream: dict[str, dict[str, Any]] = {}
    for stream_def in STREAM_DEFINITIONS:
        sid = stream_def["stream_id"]
        table = stream_def["table"]
        date_col = stream_def["date_col"]

        # Count distinct dates with data in last 7 days
        try:
            result = await db.execute(
                sa.text(
                    f"SELECT COUNT(DISTINCT {date_col}::date) "  # noqa: S608
                    f"FROM {table} "  # noqa: S608
                    f"WHERE {date_col} >= :week_ago"  # noqa: S608
                ),
                {"week_ago": week_ago},
            )
            days_with_data = result.scalar_one() or 0
        except Exception:
            days_with_data = 0

        # 5 weekdays in 7 days for market data, 7 for global
        expected_days = 5 if stream_def.get("category") in ("equity", "mf") else 7
        uptime_pct = round(min(100, days_with_data / max(expected_days, 1) * 100), 1)

        uptime_by_stream[sid] = {
            "days_with_data": days_with_data,
            "expected_days": expected_days,
            "uptime_pct": uptime_pct,
        }

    # Overall uptime
    uptimes = [v["uptime_pct"] for v in uptime_by_stream.values()]
    overall_uptime = round(sum(uptimes) / len(uptimes), 1) if uptimes else 0

    return {
        "as_of": now_utc.isoformat(),
        "date": today.isoformat(),
        "overall_uptime_pct": overall_uptime,
        "pipeline_runs_today": len(today_runs),
        "failures_today": sum(1 for r in today_runs if r["status"] == "failed"),
        "healing_events_today": len(today_heals),
        "runs": today_runs,
        "heals": today_heals,
        "uptime_by_stream": uptime_by_stream,
    }


# ---------------------------------------------------------------------------
# Data audit endpoint — live inventory + cron schedule + discrepancies
# ---------------------------------------------------------------------------

# Candidate columns that identify a distinct "instrument" in each table.
# First match found in the table is used to COUNT(DISTINCT ...).
_ENTITY_CANDIDATES: list[str] = [
    "instrument_id",
    "mstar_id",
    "index_id",
    "entity_id",
    "ticker",
    "indicator_id",
    "pair_code",
    "isin",
    "scheme_code",
    "symbol",
    "source_id",
    "category",
    "sector",
    "fund_id",
]

# Columns excluded from the "metrics" listing — these are keys, timestamps,
# or housekeeping columns, not measured values.
_METRIC_EXCLUDE_COLS: set[str] = {
    "id", "created_at", "updated_at", "meta_id", "computation_version",
    "data_status", "run_number", "business_date", "completed_at", "started_at",
    "date", "nav_date", "ex_date", "report_date", "as_of_date",
    "published_date", "month_date", "price_date", "flow_date", "trade_date",
    "computed_at", "checked_at", "processed_at",
    "instrument_id", "mstar_id", "index_id", "entity_id", "entity_type",
    "vs_benchmark", "ticker", "indicator_id", "pair_code", "source_id",
    "fund_id", "isin", "scheme_code", "symbol",
    "source", "category", "sector",
}

# Cron expression → human-readable label
def _describe_cron(cron_expr: str) -> str:
    """Best-effort human description of a cron expression."""
    if not cron_expr:
        return "triggered"
    parts = cron_expr.split()
    if len(parts) != 5:
        return cron_expr
    minute, hour, dom, month, dow = parts
    time_part = f"{hour.zfill(2)}:{minute.zfill(2)} IST"
    if minute.startswith("*/"):
        return f"every {minute[2:]} minutes"
    if dow == "1-5":
        when = "Mon-Fri"
    elif dow == "0":
        when = "Sun"
    elif dow == "*" and dom == "*":
        when = "daily"
    elif dom != "*" and month == "*":
        when = f"on day {dom} of month"
    else:
        when = f"dow={dow} dom={dom}"
    return f"{time_part}, {when}"


@router.get(
    "/audit",
    status_code=status.HTTP_200_OK,
    summary="Live data audit — metrics, instruments, schedules, discrepancies",
)
async def get_audit(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    Live data audit that powers the /data-audit dashboard.

    Returns three sections:
      metric_inventory  — per-table metric list, instrument counts by entity
                          type, date range, row counts
      table_schedule    — per-table pipeline, schedule group, cron expression,
                          last successful run, next scheduled run
      discrepancies     — tables not scheduled, stale/critical tables, empty
                          tables, quarantined data, and any de_* tables not
                          registered in STREAM_DEFINITIONS
    """
    from collections import defaultdict
    from datetime import date as date_type
    from datetime import timedelta

    from app.orchestrator.scheduler import IST, CronSchedule

    now_utc = datetime.now(tz=timezone.utc)
    now_ist = now_utc.astimezone(IST)

    schedule = CronSchedule.default()

    # 1. Pull row counts from pg_stat_user_tables (single query)
    stat_result = await db.execute(
        sa.text(
            "SELECT relname, n_live_tup "
            "FROM pg_stat_user_tables "
            "WHERE schemaname = 'public' AND relname LIKE 'de_%'"
        )
    )
    row_counts_approx: dict[str, int] = {
        row.relname: row.n_live_tup for row in stat_result.fetchall()
    }

    # 2. Pull column lists for all de_* tables in one shot
    cols_result = await db.execute(
        sa.text(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name LIKE 'de_%'
            ORDER BY table_name, ordinal_position
            """
        )
    )
    cols_by_table: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for row in cols_result.fetchall():
        cols_by_table[row.table_name].append((row.column_name, row.data_type))

    # 3. Build schedule_group → cron_expr lookup and pipeline → schedule mapping
    pipeline_to_schedule: dict[str, dict[str, Any]] = {}
    for entry in schedule.entries:
        for pipeline_name in entry.pipelines:
            # Map both the alias form and the raw form — caller may use either
            pipeline_to_schedule[pipeline_name] = {
                "schedule_group": entry.name,
                "cron_expression": entry.cron_expr or None,
                "cron_label": _describe_cron(entry.cron_expr),
                "description": entry.description,
                "trigger_after": entry.trigger_after,
            }

    # Also include SCHEDULE_REGISTRY groups that are not in CronSchedule
    # (e.g. nightly_compute, technicals) — triggered, no cron_expr
    try:
        from app.pipelines.registry import SCHEDULE_REGISTRY
        for group_name, pipelines in SCHEDULE_REGISTRY.items():
            for pipeline_name in pipelines:
                if pipeline_name in pipeline_to_schedule:
                    continue
                pipeline_to_schedule[pipeline_name] = {
                    "schedule_group": group_name,
                    "cron_expression": None,
                    "cron_label": "triggered (" + group_name + ")",
                    "description": "",
                    "trigger_after": None,
                }
    except Exception:
        pass

    # 4. Pull last pipeline runs per pipeline_name (latest successful + latest any)
    last_run_rows = await db.execute(
        sa.text(
            """
            SELECT DISTINCT ON (pipeline_name, status)
                pipeline_name, status, completed_at, rows_processed
            FROM de_pipeline_log
            WHERE completed_at IS NOT NULL
            ORDER BY pipeline_name, status, completed_at DESC
            """
        )
    )
    last_run_by_pipeline: dict[str, dict[str, Any]] = defaultdict(dict)
    for r in last_run_rows.fetchall():
        last_run_by_pipeline[r.pipeline_name][r.status] = {
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "rows_processed": r.rows_processed,
        }

    # 5. For each stream: build metric_inventory + table_schedule entries
    metric_inventory: list[dict[str, Any]] = []
    table_schedule: list[dict[str, Any]] = []
    stale_streams: list[dict[str, Any]] = []
    empty_streams: list[dict[str, Any]] = []
    unscheduled_streams: list[dict[str, Any]] = []
    quarantined_tables: list[dict[str, Any]] = []

    known_tables: set[str] = set()

    for stream in STREAM_DEFINITIONS:
        table = stream["table"]
        date_col = stream["date_col"]
        stream_id = stream["stream_id"]
        category = stream["category"]
        label = stream["label"]
        known_tables.add(table)

        cols = cols_by_table.get(table, [])
        if not cols:
            # Table missing altogether
            metric_inventory.append({
                "stream_id": stream_id,
                "label": label,
                "table": table,
                "category": category,
                "exists": False,
                "metrics": [],
                "metric_count": 0,
                "instrument_counts": {},
                "total_instruments": 0,
                "min_date": None,
                "max_date": None,
                "row_count_exact": 0,
                "row_count_approx": 0,
            })
            table_schedule.append({
                "stream_id": stream_id,
                "table": table,
                "label": label,
                "exists": False,
                "pipeline": STREAM_PIPELINE_MAP.get(stream_id),
                "schedule_group": None,
                "cron_expression": None,
                "cron_label": None,
                "last_success_at": None,
                "last_run_at": None,
                "last_run_status": None,
                "next_run": None,
            })
            continue

        col_names = [c[0] for c in cols]
        has_data_status = "data_status" in col_names

        # Pick entity column
        entity_col: Optional[str] = None
        for cand in _ENTITY_CANDIDATES:
            if cand in col_names:
                entity_col = cand
                break

        # Metric columns = all cols minus housekeeping
        metrics = [c for c in col_names if c not in _METRIC_EXCLUDE_COLS]

        # Query: MIN/MAX date + exact COUNT
        min_date, max_date, exact_count = None, None, 0
        try:
            dr = await db.execute(
                sa.text(
                    f"SELECT MIN({date_col}) AS min_d, MAX({date_col}) AS max_d, "  # noqa: S608
                    f"COUNT(*) AS cnt FROM {table}"  # noqa: S608
                )
            )
            dr_row = dr.fetchone()
            if dr_row:
                min_date = str(dr_row.min_d) if dr_row.min_d else None
                max_date = str(dr_row.max_d) if dr_row.max_d else None
                exact_count = int(dr_row.cnt or 0)
        except Exception as exc:
            logger.warning(
                "audit_date_range_error", table=table, error=str(exc)
            )

        # Instrument counts
        instrument_counts: dict[str, int] = {}
        total_instruments = 0
        if entity_col and exact_count > 0:
            try:
                if stream_id == "rs_scores" and "entity_type" in col_names:
                    ic = await db.execute(
                        sa.text(
                            f"SELECT entity_type, COUNT(DISTINCT {entity_col}) AS n "  # noqa: S608
                            f"FROM {table} GROUP BY entity_type"  # noqa: S608
                        )
                    )
                    for ic_row in ic.fetchall():
                        instrument_counts[str(ic_row.entity_type)] = int(ic_row.n or 0)
                else:
                    ic = await db.execute(
                        sa.text(
                            f"SELECT COUNT(DISTINCT {entity_col}) AS n FROM {table}"  # noqa: S608
                        )
                    )
                    instrument_counts[category] = int(ic.scalar_one() or 0)
                total_instruments = sum(instrument_counts.values())
            except Exception as exc:
                logger.warning(
                    "audit_instrument_count_error", table=table, error=str(exc)
                )

        # Quarantined rows
        quarantined_count = 0
        if has_data_status:
            try:
                q = await db.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM {table} "  # noqa: S608
                        f"WHERE data_status = 'quarantined'"  # noqa: S608
                    )
                )
                quarantined_count = int(q.scalar_one() or 0)
                if quarantined_count > 0:
                    quarantined_tables.append({
                        "stream_id": stream_id,
                        "table": table,
                        "label": label,
                        "quarantined_rows": quarantined_count,
                    })
            except Exception:
                pass

        # Freshness
        hours_old: Optional[float] = None
        if max_date:
            try:
                from datetime import date as dt_date
                md = datetime.strptime(max_date[:10], "%Y-%m-%d").date()
                hours_old = (now_utc.date() - md).days * 24.0
            except Exception:
                hours_old = None

        freshness = _freshness_status(hours_old)

        metric_inventory.append({
            "stream_id": stream_id,
            "label": label,
            "table": table,
            "category": category,
            "exists": True,
            "metrics": metrics,
            "metric_count": len(metrics),
            "entity_column": entity_col,
            "instrument_counts": instrument_counts,
            "total_instruments": total_instruments,
            "min_date": min_date,
            "max_date": max_date,
            "row_count_exact": exact_count,
            "row_count_approx": row_counts_approx.get(table, 0),
            "hours_old": round(hours_old, 1) if hours_old is not None else None,
            "freshness": freshness,
            "quarantined_rows": quarantined_count,
        })

        # Schedule mapping
        pipeline_name = STREAM_PIPELINE_MAP.get(stream_id)
        sched_info = pipeline_to_schedule.get(pipeline_name) if pipeline_name else None

        # Also try matching via DAG alias (e.g. amfi_nav ↔ mf_eod)
        if sched_info is None and pipeline_name:
            try:
                from app.pipelines.registry import DAG_ALIAS
                for alias, real in DAG_ALIAS.items():
                    if real == pipeline_name and alias in pipeline_to_schedule:
                        sched_info = pipeline_to_schedule[alias]
                        break
                    if alias == pipeline_name and real in pipeline_to_schedule:
                        sched_info = pipeline_to_schedule[real]
                        break
            except Exception:
                pass

        last_success = None
        last_run = None
        last_status = None
        if pipeline_name:
            runs = last_run_by_pipeline.get(pipeline_name, {})
            if "success" in runs:
                last_success = runs["success"]["completed_at"]
            # Latest of any status
            latest_any = None
            latest_status = None
            for stt, info in runs.items():
                ts = info.get("completed_at")
                if ts and (latest_any is None or ts > latest_any):
                    latest_any = ts
                    latest_status = stt
            last_run = latest_any
            last_status = latest_status

        # Next run (only for cron-scheduled entries)
        next_run = None
        if sched_info and sched_info.get("cron_expression"):
            try:
                entry_obj = None
                for e in schedule.entries:
                    if e.name == sched_info["schedule_group"]:
                        entry_obj = e
                        break
                if entry_obj:
                    nxt = schedule.next_run_after(entry_obj, now_ist)
                    if nxt:
                        next_run = nxt.isoformat()
            except Exception:
                pass

        table_schedule.append({
            "stream_id": stream_id,
            "table": table,
            "label": label,
            "exists": True,
            "pipeline": pipeline_name,
            "schedule_group": sched_info["schedule_group"] if sched_info else None,
            "cron_expression": sched_info["cron_expression"] if sched_info else None,
            "cron_label": sched_info["cron_label"] if sched_info else None,
            "last_success_at": last_success,
            "last_run_at": last_run,
            "last_run_status": last_status,
            "next_run": next_run,
            "is_scheduled": bool(sched_info and sched_info.get("cron_expression")),
            "is_triggered": bool(sched_info and not sched_info.get("cron_expression")),
        })

        # Discrepancy classification
        if pipeline_name is None:
            unscheduled_streams.append({
                "stream_id": stream_id,
                "table": table,
                "label": label,
                "reason": "no pipeline mapped for stream",
            })
        elif sched_info is None:
            unscheduled_streams.append({
                "stream_id": stream_id,
                "table": table,
                "label": label,
                "pipeline": pipeline_name,
                "reason": "pipeline has no schedule or trigger",
            })

        if freshness == "critical":
            stale_streams.append({
                "stream_id": stream_id,
                "table": table,
                "label": label,
                "max_date": max_date,
                "hours_old": round(hours_old, 1) if hours_old is not None else None,
                "pipeline": pipeline_name,
            })

        if exact_count == 0:
            empty_streams.append({
                "stream_id": stream_id,
                "table": table,
                "label": label,
                "pipeline": pipeline_name,
            })

    # 6. de_* tables in DB that are NOT in STREAM_DEFINITIONS
    all_db_tables = set(cols_by_table.keys())
    unmapped_tables = sorted(
        t for t in all_db_tables
        if t not in known_tables
        and not t.startswith("de_pipeline_log")
        and not t.startswith("de_healing_log")
        and not t.startswith("de_data_quality")
        and not t.startswith("de_migration")
        and not t.startswith("de_request_log")
        and not t.startswith("de_source_files")
        and not t.startswith("de_system_flags")
        and not t.startswith("de_client")
        and not t.startswith("de_portfolio")
        and not t.startswith("de_pii")
        and not t.startswith("alembic_")
    )

    # 7. Summary counters
    summary = {
        "stream_count": len(metric_inventory),
        "existing_tables": sum(1 for s in metric_inventory if s["exists"]),
        "total_metrics": sum(s["metric_count"] for s in metric_inventory),
        "total_instruments": sum(s["total_instruments"] for s in metric_inventory),
        "scheduled": sum(1 for s in table_schedule if s["is_scheduled"]),
        "triggered": sum(1 for s in table_schedule if s["is_triggered"]),
        "unscheduled": len(unscheduled_streams),
        "stale_critical": len(stale_streams),
        "empty": len(empty_streams),
        "quarantined": len(quarantined_tables),
        "unmapped_db_tables": len(unmapped_tables),
    }

    return {
        "as_of": now_utc.isoformat(),
        "as_of_ist": now_ist.isoformat(),
        "summary": summary,
        "metric_inventory": metric_inventory,
        "table_schedule": table_schedule,
        "discrepancies": {
            "unscheduled_streams": unscheduled_streams,
            "stale_streams": stale_streams,
            "empty_streams": empty_streams,
            "quarantined_tables": quarantined_tables,
            "unmapped_db_tables": [{"table": t} for t in unmapped_tables],
        },
    }


# ---------------------------------------------------------------------------
# Per-table health check — 4-pointer diagnostic per stream
# ---------------------------------------------------------------------------

# Expected update frequency per category (hours between updates)
_FRESHNESS_THRESHOLDS: dict[str, dict[str, float]] = {
    "equity":       {"fresh": 36, "stale": 96},
    "mf":           {"fresh": 36, "stale": 96},
    "etf":          {"fresh": 36, "stale": 96},
    "flows":        {"fresh": 36, "stale": 96},
    "global":       {"fresh": 48, "stale": 120},
    "macro":        {"fresh": 168, "stale": 336},     # weekly / biweekly
    "qualitative":  {"fresh": 2, "stale": 12},        # every 30 min
    "monthly":      {"fresh": 720, "stale": 1440},    # 30d / 60d
}

# Minimum expected row count per stream (order-of-magnitude floor)
_MIN_ROW_EXPECTATIONS: dict[str, int] = {
    "equity_ohlcv": 100_000,
    "equity_technicals": 50_000,
    "rs_scores": 10_000,
    "mf_nav": 50_000,
    "mf_holdings": 5_000,
    "index_prices": 5_000,
    "global_prices": 1_000,
    "institutional_flows": 500,
    "fo_summary": 200,
}


def _freshness_grade(hours_old: float | None, category: str) -> dict[str, Any]:
    """Grade freshness for a specific category."""
    thresholds = _FRESHNESS_THRESHOLDS.get(category, {"fresh": 36, "stale": 96})
    if hours_old is None:
        return {"check": "freshness", "status": "unknown", "detail": "no data"}
    if hours_old < thresholds["fresh"]:
        return {"check": "freshness", "status": "pass", "detail": f"{hours_old:.0f}h old"}
    if hours_old < thresholds["stale"]:
        return {"check": "freshness", "status": "warn", "detail": f"{hours_old:.0f}h old (stale)"}
    return {"check": "freshness", "status": "fail", "detail": f"{hours_old:.0f}h old (critical)"}


def _completeness_grade(
    row_count: int, stream_id: str, has_today: bool,
) -> dict[str, Any]:
    """Grade row count and whether today's data exists."""
    if row_count == 0:
        return {"check": "completeness", "status": "fail", "detail": "empty table"}
    min_expected = _MIN_ROW_EXPECTATIONS.get(stream_id, 0)
    if min_expected and row_count < min_expected:
        return {
            "check": "completeness", "status": "warn",
            "detail": f"{row_count:,} rows (expected >={min_expected:,})",
        }
    return {
        "check": "completeness", "status": "pass",
        "detail": f"{row_count:,} rows",
    }


def _quality_grade(
    quarantined: int, total: int,
) -> dict[str, Any]:
    """Grade data quality based on quarantined rows."""
    if total == 0:
        return {"check": "quality", "status": "unknown", "detail": "no data"}
    if quarantined == 0:
        return {"check": "quality", "status": "pass", "detail": "0 quarantined"}
    pct = quarantined / total * 100
    if pct < 1:
        return {
            "check": "quality", "status": "pass",
            "detail": f"{quarantined:,} quarantined ({pct:.2f}%)",
        }
    if pct < 5:
        return {
            "check": "quality", "status": "warn",
            "detail": f"{quarantined:,} quarantined ({pct:.1f}%)",
        }
    return {
        "check": "quality", "status": "fail",
        "detail": f"{quarantined:,} quarantined ({pct:.1f}%)",
    }


def _pipeline_grade(
    last_success_at: str | None, last_status: str | None,
    is_scheduled: bool, hours_old: float | None,
) -> dict[str, Any]:
    """Grade pipeline health — is it running and succeeding?"""
    if not is_scheduled and last_success_at is None:
        return {"check": "pipeline", "status": "fail", "detail": "no pipeline / not scheduled"}
    if last_status == "failed":
        return {
            "check": "pipeline", "status": "fail",
            "detail": f"last run failed (at {last_success_at or 'never'})",
        }
    if last_success_at is None:
        return {"check": "pipeline", "status": "warn", "detail": "never succeeded"}
    if last_status in ("success", "partial"):
        return {"check": "pipeline", "status": "pass", "detail": f"last success: {last_success_at}"}
    return {"check": "pipeline", "status": "warn", "detail": f"status={last_status}"}


@router.get(
    "/health-detail",
    status_code=status.HTTP_200_OK,
    summary="Per-table 4-point health check — freshness, completeness, quality, pipeline",
)
async def get_health_detail(
    db=Depends(get_observatory_db),
) -> dict[str, Any]:
    """
    For every tracked stream, run 4 health checks:
      1. Freshness — is data recent enough for its expected frequency?
      2. Completeness — does the table have enough rows? Is today present?
      3. Quality — what fraction of rows are quarantined?
      4. Pipeline — is the feeding pipeline scheduled and succeeding?

    Returns per-table health card + overall score.
    """
    from collections import defaultdict
    from datetime import date as date_type

    now_utc = datetime.now(tz=timezone.utc)
    today = date_type.today()

    from app.orchestrator.scheduler import IST, CronSchedule
    schedule = CronSchedule.default()

    # Pipeline schedule lookup (same logic as /audit)
    pipeline_to_schedule: dict[str, dict[str, Any]] = {}
    for entry in schedule.entries:
        for pname in entry.pipelines:
            pipeline_to_schedule[pname] = {
                "schedule_group": entry.name,
                "cron_expression": entry.cron_expr or None,
            }
    try:
        from app.pipelines.registry import SCHEDULE_REGISTRY
        for group_name, pipelines in SCHEDULE_REGISTRY.items():
            for pname in pipelines:
                if pname not in pipeline_to_schedule:
                    pipeline_to_schedule[pname] = {
                        "schedule_group": group_name,
                        "cron_expression": None,
                    }
    except Exception:
        pass

    # Last pipeline success per pipeline_name
    try:
        run_result = await db.execute(
            sa.text(
                """
                SELECT DISTINCT ON (pipeline_name)
                    pipeline_name, status, completed_at
                FROM de_pipeline_log
                WHERE completed_at IS NOT NULL
                ORDER BY pipeline_name, completed_at DESC
                """
            )
        )
        last_runs: dict[str, dict[str, Any]] = {
            r.pipeline_name: {"status": r.status, "at": r.completed_at.isoformat() if r.completed_at else None}
            for r in run_result.fetchall()
        }
    except Exception:
        last_runs = {}

    # Column existence cache
    cols_result = await db.execute(
        sa.text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name LIKE 'de_%'"
        )
    )
    cols_by_table: dict[str, set[str]] = defaultdict(set)
    for r in cols_result.fetchall():
        cols_by_table[r.table_name].add(r.column_name)

    # Build health cards
    health_cards: list[dict[str, Any]] = []
    totals = {"pass": 0, "warn": 0, "fail": 0, "unknown": 0}

    for stream in STREAM_DEFINITIONS:
        table = stream["table"]
        date_col = stream["date_col"]
        sid = stream["stream_id"]
        cat = stream["category"]

        cols = cols_by_table.get(table, set())
        if not cols:
            checks = [
                {"check": "freshness", "status": "fail", "detail": "table missing"},
                {"check": "completeness", "status": "fail", "detail": "table missing"},
                {"check": "quality", "status": "unknown", "detail": "table missing"},
                {"check": "pipeline", "status": "fail", "detail": "table missing"},
            ]
            overall = "fail"
            health_cards.append({
                "stream_id": sid, "label": stream["label"], "table": table,
                "category": cat, "exists": False, "overall": overall,
                "checks": checks, "pass_count": 0, "fail_count": 4,
            })
            totals["fail"] += 1
            continue

        has_data_status = "data_status" in cols

        # 1. Freshness + row count + quarantined — single compound query
        query_parts = [
            f"COUNT(*) AS total",
            f"MIN({date_col}) AS min_d",
            f"MAX({date_col}) AS max_d",
        ]
        if has_data_status:
            query_parts.append(
                "SUM(CASE WHEN data_status='quarantined' THEN 1 ELSE 0 END) AS q_count"
            )

        try:
            q = await db.execute(
                sa.text(f"SELECT {', '.join(query_parts)} FROM {table}")  # noqa: S608
            )
            row = q.fetchone()
            total_rows = int(row.total or 0)
            max_date = row.max_d
            quarantined = int(row.q_count) if has_data_status and hasattr(row, "q_count") and row.q_count else 0
        except Exception:
            total_rows = 0
            max_date = None
            quarantined = 0

        hours_old: float | None = None
        has_today = False
        if max_date is not None:
            try:
                from datetime import date as dt_date
                if isinstance(max_date, dt_date):
                    md = max_date
                else:
                    md = datetime.strptime(str(max_date)[:10], "%Y-%m-%d").date()
                hours_old = (today - md).days * 24.0
                has_today = md >= today
            except Exception:
                pass

        # Pipeline info
        pipeline_name = STREAM_PIPELINE_MAP.get(sid)
        sched = pipeline_to_schedule.get(pipeline_name) if pipeline_name else None
        if sched is None and pipeline_name:
            try:
                from app.pipelines.registry import DAG_ALIAS
                for alias, real in DAG_ALIAS.items():
                    if real == pipeline_name and alias in pipeline_to_schedule:
                        sched = pipeline_to_schedule[alias]
                        break
            except Exception:
                pass

        is_scheduled = bool(sched)
        run_info = last_runs.get(pipeline_name, {}) if pipeline_name else {}

        # Build 4 checks
        c1 = _freshness_grade(hours_old, cat)
        c2 = _completeness_grade(total_rows, sid, has_today)
        c3 = _quality_grade(quarantined, total_rows) if has_data_status else {
            "check": "quality", "status": "pass", "detail": "no data_status column (validated elsewhere)",
        }
        c4 = _pipeline_grade(
            run_info.get("at"), run_info.get("status"), is_scheduled, hours_old,
        )

        checks = [c1, c2, c3, c4]
        pass_count = sum(1 for c in checks if c["status"] == "pass")
        fail_count = sum(1 for c in checks if c["status"] == "fail")
        warn_count = sum(1 for c in checks if c["status"] == "warn")

        if fail_count > 0:
            overall = "unhealthy"
        elif warn_count > 0:
            overall = "degraded"
        elif pass_count == 4:
            overall = "healthy"
        else:
            overall = "unknown"

        totals["pass" if overall == "healthy" else "warn" if overall == "degraded" else "fail"] += 1

        health_cards.append({
            "stream_id": sid,
            "label": stream["label"],
            "table": table,
            "category": cat,
            "exists": True,
            "overall": overall,
            "checks": checks,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "row_count": total_rows,
            "max_date": str(max_date) if max_date else None,
            "hours_old": round(hours_old, 1) if hours_old is not None else None,
            "pipeline": pipeline_name,
            "schedule_group": sched["schedule_group"] if sched else None,
        })

    healthy = sum(1 for h in health_cards if h["overall"] == "healthy")
    degraded = sum(1 for h in health_cards if h["overall"] == "degraded")
    unhealthy = sum(1 for h in health_cards if h["overall"] == "unhealthy")

    if unhealthy > 0:
        engine_status = "unhealthy"
    elif degraded > 0:
        engine_status = "degraded"
    else:
        engine_status = "healthy"

    return {
        "as_of": now_utc.isoformat(),
        "engine_status": engine_status,
        "summary": {
            "total_streams": len(health_cards),
            "healthy": healthy,
            "degraded": degraded,
            "unhealthy": unhealthy,
        },
        "streams": health_cards,
    }
