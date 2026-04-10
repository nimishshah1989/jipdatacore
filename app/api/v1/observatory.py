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
        "symbol": "ETF ticker symbol",
        "close": "Closing price",
        "aum_cr": "ETF AUM in crores INR",
        "tracking_error": "Annualised tracking error vs index",
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
    },
    {
        "stream_id": "mf_flows",
        "label": "MF Category Flows",
        "table": "de_mf_category_flows",
        "date_col": "month_date",
        "category": "mf",
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
    },
    {
        "stream_id": "goldilocks_stock_ideas",
        "label": "Goldilocks Stock Ideas",
        "table": "de_goldilocks_stock_ideas",
        "date_col": "published_date",
        "category": "qualitative",
    },
    {
        "stream_id": "oscillator_monthly",
        "label": "Oscillators (Monthly)",
        "table": "de_oscillator_monthly",
        "date_col": "date",
        "category": "equity",
    },
]


def _freshness_status(hours_old: Optional[float]) -> str:
    """Classify freshness based on hours since last update.

    Thresholds tuned for market data: yesterday's EOD close is "fresh"
    until today's EOD runs (~19:00 IST, i.e. ~36h window).
    """
    if hours_old is None:
        return "unknown"
    if hours_old < 0:
        return "fresh"  # future-dated data (e.g. macro forecasts)
    if hours_old < 36:
        return "fresh"
    if hours_old <= 96:
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
                "status": _freshness_status(hours_old),
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
            stream_status = _freshness_status(hours_old)

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
# Agent status endpoint
# ---------------------------------------------------------------------------

AGENT_DEFINITIONS = [
    {
        "agent_id": "jip-eod-ingestion",
        "name": "EOD Ingestion",
        "schedule": "18:33 IST, Mon-Fri",
        "pipelines": ["equity_bhav", "nse_indices", "equity_corporate_actions",
                       "fii_dii_flows", "mf_eod", "yfinance_global", "fred_macro", "india_vix"],
        "match_patterns": ["eod", "bhav", "amfi", "yfinance", "fred", "fii_dii"],
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
