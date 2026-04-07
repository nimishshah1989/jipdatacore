"""Master ingestion orchestrator — seeds and backfills all data streams.

Runs once to bootstrap the full historical dataset:
  Stream 0 (foundation)  — trading calendar + instrument master
  Streams 1-5 (parallel) — equity, MF, indices, global/macro, flows
  Stream 6 (cross-cutting)— deferred; requires Stream 1 data

Usage:
    python -m app.pipelines.orchestrate_ingestion                  # all streams
    python -m app.pipelines.orchestrate_ingestion --streams 0,4    # selective
    python -m app.pipelines.orchestrate_ingestion --dry-run        # plan only
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import structlog
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.models.instruments import (
    DeGlobalInstrumentMaster,
    DeInstrument,
    DeMacroMaster,
)
from app.models.pipeline import DePipelineLog
from app.models.prices import DeGlobalPrices, DeMacroValues
from app.pipelines.trading_calendar import populate_trading_calendar

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

def _gi(ticker: str, name: str, itype: str, exch: str, curr: str, ctry: str, cat: str) -> dict[str, str]:
    """Build a global instrument dict (reduces line length in the constant below)."""
    return {"ticker": ticker, "name": name, "instrument_type": itype,
            "exchange": exch, "currency": curr, "country": ctry, "category": cat}


GLOBAL_INSTRUMENTS: list[dict[str, str]] = [
    # Global equity indices
    _gi("^GSPC",     "S&P 500",                       "index", "NYSE",     "USD", "US", "broad"),
    _gi("^IXIC",     "NASDAQ Composite",               "index", "NASDAQ",   "USD", "US", "broad"),
    _gi("^DJI",      "Dow Jones Industrial Average",   "index", "NYSE",     "USD", "US", "broad"),
    _gi("^FTSE",     "FTSE 100",                       "index", "LSE",      "GBP", "UK", "broad"),
    _gi("^GDAXI",    "DAX",                            "index", "XETRA",    "EUR", "DE", "broad"),
    _gi("^FCHI",     "CAC 40",                         "index", "EURONEXT", "EUR", "FR", "broad"),
    _gi("^N225",     "Nikkei 225",                     "index", "TSE",      "JPY", "JP", "broad"),
    _gi("^HSI",      "Hang Seng",                      "index", "HKEX",     "HKD", "HK", "broad"),
    _gi("000001.SS", "Shanghai Composite",             "index", "SSE",      "CNY", "CN", "broad"),
    _gi("^AXJO",     "ASX 200",                        "index", "ASX",      "AUD", "AU", "broad"),
    # Emerging / world ETFs
    _gi("EEM",       "MSCI Emerging Markets ETF",      "etf",   "NYSE",     "USD", "US", "emerging"),
    _gi("URTH",      "MSCI World ETF",                 "etf",   "NYSE",     "USD", "US", "world"),
    # Commodities + FX — check constraint only allows 'index'/'etf'; using 'index'
    _gi("DX-Y.NYB",  "US Dollar Index",                "index", "ICE",      "USD", "US", "fx"),
    _gi("CL=F",      "Crude Oil WTI",                  "index", "NYMEX",    "USD", "US", "commodity"),
    _gi("BZ=F",      "Crude Oil Brent",                "index", "ICE",      "USD", "US", "commodity"),
    _gi("GC=F",      "Gold",                           "index", "COMEX",    "USD", "US", "commodity"),
    _gi("SI=F",      "Silver",                         "index", "COMEX",    "USD", "US", "commodity"),
    _gi("USDINR=X",  "USD/INR",                        "index", "FX",       "INR", "IN", "fx"),
    _gi("USDJPY=X",  "USD/JPY",                        "index", "FX",       "JPY", "JP", "fx"),
    _gi("EURUSD=X",  "EUR/USD",                        "index", "FX",       "USD", "US", "fx"),
    _gi("USDCNH=X",  "USD/CNH",                        "index", "FX",       "CNH", "CN", "fx"),
]

MACRO_SERIES: list[dict[str, str]] = [
    {"ticker": "DGS10", "name": "10-Year Treasury Yield", "source": "FRED", "unit": "percent", "frequency": "daily"},
    {"ticker": "DGS2", "name": "2-Year Treasury Yield", "source": "FRED", "unit": "percent", "frequency": "daily"},
    {"ticker": "FEDFUNDS", "name": "Federal Funds Rate", "source": "FRED", "unit": "percent", "frequency": "monthly"},
    {"ticker": "T10Y2Y", "name": "10Y-2Y Treasury Spread", "source": "FRED", "unit": "percent", "frequency": "daily"},
    {"ticker": "CPIAUCSL", "name": "Consumer Price Index", "source": "FRED", "unit": "index", "frequency": "monthly"},
    {"ticker": "UNRATE", "name": "Unemployment Rate", "source": "FRED", "unit": "percent", "frequency": "monthly"},
    {"ticker": "INDIAVIX", "name": "India VIX", "source": "NSE", "unit": "index", "frequency": "daily"},
]

NSE_EQUITY_CSV_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a float/string value to Decimal via string to avoid float drift."""
    if value is None:
        return None
    try:
        return Decimal(str(round(float(value), 8)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _make_run_log(pipeline_name: str) -> DePipelineLog:
    """Create a minimal in-memory run_log for pipeline execution."""
    return DePipelineLog(
        pipeline_name=pipeline_name,
        business_date=date.today(),
        run_number=1,
        status="running",
        started_at=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Stream 0 — Foundation
# ---------------------------------------------------------------------------

async def stream_0_foundation(sf: async_sessionmaker) -> None:
    """Seed trading calendar (2007-2026) and instrument master."""
    logger.info("stream_0_start", stream="foundation")

    # 0.1 Trading Calendar
    total_cal_rows = 0
    for year in range(2007, 2027):
        async with sf() as session:
            async with session.begin():
                count = await populate_trading_calendar(session, year)
                total_cal_rows += count

    logger.info("stream_0_calendar_done", rows=total_cal_rows)

    # 0.2 Instrument Enrichment
    instruments_upserted = 0
    try:
        async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(NSE_EQUITY_CSV_URL)
            resp.raise_for_status()
            content = resp.text

        from app.pipelines.equity.master_refresh import parse_equity_listing_csv
        instruments = parse_equity_listing_csv(content)

        if instruments:
            rows = [
                {
                    "current_symbol": inst["symbol"],
                    "isin": inst.get("isin"),
                    "company_name": inst.get("company_name"),
                    "series": inst.get("series"),
                    "listing_date": inst.get("listing_date"),
                }
                for inst in instruments
                if inst.get("symbol")
            ]

            async with sf() as session:
                async with session.begin():
                    stmt = pg_insert(DeInstrument).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["current_symbol"],
                        set_={
                            "isin": stmt.excluded.isin,
                            "company_name": stmt.excluded.company_name,
                            "series": stmt.excluded.series,
                            "listing_date": stmt.excluded.listing_date,
                        },
                    )
                    await session.execute(stmt)
                    instruments_upserted = len(rows)

    except Exception as exc:
        logger.error("stream_0_instrument_enrichment_failed", error=str(exc))

    logger.info("stream_0_done", calendar_rows=total_cal_rows, instruments=instruments_upserted)


# ---------------------------------------------------------------------------
# Stream 1 — Equity
# ---------------------------------------------------------------------------

async def stream_1_equity(sf: async_sessionmaker) -> None:
    """Corporate actions backfill (2020-01 → 2026-04). Delivery skipped."""
    logger.info("stream_1_start", stream="equity")

    # 1.1 Corporate Actions Backfill
    from app.pipelines.equity.corporate_actions import CorporateActionsPipeline

    pipeline = CorporateActionsPipeline()
    months_processed = 0
    months_failed = 0

    # Iterate monthly from 2020-01 to 2026-04
    year, month = 2020, 1
    end_year, end_month = 2026, 4

    while (year, month) <= (end_year, end_month):
        first_of_month = date(year, month, 1)
        run_log = _make_run_log("corporate_actions_backfill")
        try:
            async with sf() as session:
                async with session.begin():
                    await pipeline.execute(
                        business_date=first_of_month,
                        session=session,
                        run_log=run_log,
                    )
            months_processed += 1
        except Exception as exc:
            months_failed += 1
            logger.error(
                "stream_1_corp_actions_month_failed",
                month=f"{year}-{month:02d}",
                error=str(exc),
            )

        # Advance month
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

    # 1.2 Delivery Backfill — skipped
    logger.info(
        "stream_1_delivery_skipped",
        reason="Delivery backfill skipped — run separately with dedicated workers",
    )

    logger.info(
        "stream_1_done",
        months_processed=months_processed,
        months_failed=months_failed,
    )


# ---------------------------------------------------------------------------
# Stream 2 — Mutual Funds
# ---------------------------------------------------------------------------

async def stream_2_mf(sf: async_sessionmaker) -> None:
    """Compute MF returns for existing 851 equity funds with NAV data."""
    logger.info("stream_2_start", stream="mf")

    # 2.1 Returns computation
    try:
        from app.pipelines.mf.returns import compute_returns_for_date

        async with sf() as session:
            async with session.begin():
                count = await compute_returns_for_date(session, date.today())
        logger.info("stream_2_returns_done", funds_updated=count)
    except Exception as exc:
        logger.warning("stream_2_returns_skipped", error=str(exc))

    logger.info("stream_2_done")


# ---------------------------------------------------------------------------
# Stream 3 — Indices
# ---------------------------------------------------------------------------

async def stream_3_indices(sf: async_sessionmaker) -> None:
    """Index missing-name mapping (deferred) and constituents snapshot."""
    logger.info("stream_3_start", stream="indices")

    # 3.1 Missing indices
    logger.info(
        "stream_3_missing_indices",
        note="40 missing indices need name mapping — manual review required",
    )

    # 3.2 Index Constituents
    try:
        from app.pipelines.indices.index_constituents import IndexConstituentsPipeline

        pipeline = IndexConstituentsPipeline()
        run_log = _make_run_log("index_constituents_backfill")
        async with sf() as session:
            async with session.begin():
                await pipeline.execute(
                    business_date=date.today(),
                    session=session,
                    run_log=run_log,
                )
        logger.info("stream_3_constituents_done")
    except Exception as exc:
        logger.error("stream_3_constituents_failed", error=str(exc))

    logger.info("stream_3_done")


# ---------------------------------------------------------------------------
# Stream 4 — Global + Macro
# ---------------------------------------------------------------------------

async def _truncate_global_tables(sf: async_sessionmaker) -> None:
    """Truncate stale global price and instrument master tables."""
    import sqlalchemy as sa

    async with sf() as session:
        async with session.begin():
            await session.execute(
                sa.text("TRUNCATE TABLE de_global_prices, de_global_instrument_master CASCADE")
            )
    logger.info("stream_4_truncated_global_tables")


async def _seed_global_instruments(sf: async_sessionmaker) -> int:
    """Upsert 21 global instrument master rows."""
    rows = [
        {
            "ticker": inst["ticker"],
            "name": inst["name"],
            "instrument_type": inst["instrument_type"],
            "exchange": inst["exchange"],
            "currency": inst["currency"],
            "country": inst["country"],
            "category": inst["category"],
            "source": "yfinance",
        }
        for inst in GLOBAL_INSTRUMENTS
    ]

    async with sf() as session:
        async with session.begin():
            stmt = pg_insert(DeGlobalInstrumentMaster).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["ticker"],
                set_={
                    "name": stmt.excluded.name,
                    "instrument_type": stmt.excluded.instrument_type,
                    "exchange": stmt.excluded.exchange,
                    "currency": stmt.excluded.currency,
                    "country": stmt.excluded.country,
                    "category": stmt.excluded.category,
                    "source": stmt.excluded.source,
                },
            )
            await session.execute(stmt)

    return len(rows)


async def _backfill_global_prices(sf: async_sessionmaker) -> int:
    """Download OHLCV history from yfinance and upsert into de_global_prices."""
    import yfinance as yf
    import pandas as pd

    tickers = [inst["ticker"] for inst in GLOBAL_INSTRUMENTS]
    logger.info("stream_4_yfinance_download_start", ticker_count=len(tickers))

    df = yf.download(tickers, start="2016-01-01", progress=False, auto_adjust=False)
    if df.empty:
        logger.warning("stream_4_yfinance_empty_result")
        return 0

    rows: list[dict[str, Any]] = []

    # yfinance returns MultiIndex columns: (Price, Ticker)
    for ticker in tickers:
        try:
            ticker_df = df.xs(ticker, axis=1, level=1) if isinstance(df.columns, pd.MultiIndex) else df
        except KeyError:
            logger.warning("stream_4_ticker_missing_from_download", ticker=ticker)
            continue

        for idx_date, row in ticker_df.iterrows():
            close_val = _safe_decimal(row.get("Close") or row.get("close"))
            if close_val is None:
                continue  # skip rows with no close price

            rows.append({
                "date": idx_date.date() if hasattr(idx_date, "date") else idx_date,
                "ticker": ticker,
                "open": _safe_decimal(row.get("Open") or row.get("open")),
                "high": _safe_decimal(row.get("High") or row.get("high")),
                "low": _safe_decimal(row.get("Low") or row.get("low")),
                "close": close_val,
                "volume": int(row.get("Volume") or row.get("volume") or 0) or None,
            })

    if not rows:
        logger.warning("stream_4_no_global_price_rows_parsed")
        return 0

    # Batch upsert in chunks of 5000
    BATCH = 5_000
    total = 0
    for start in range(0, len(rows), BATCH):
        batch = rows[start : start + BATCH]
        async with sf() as session:
            async with session.begin():
                stmt = pg_insert(DeGlobalPrices).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["date", "ticker"],
                    set_={
                        "open": stmt.excluded.open,
                        "high": stmt.excluded.high,
                        "low": stmt.excluded.low,
                        "close": stmt.excluded.close,
                        "volume": stmt.excluded.volume,
                    },
                )
                await session.execute(stmt)
        total += len(batch)

    return total


async def _seed_macro_master(sf: async_sessionmaker) -> int:
    """Upsert macro series metadata into de_macro_master."""
    rows = [
        {
            "ticker": s["ticker"],
            "name": s["name"],
            "source": s["source"],
            "unit": s["unit"],
            "frequency": s["frequency"],
        }
        for s in MACRO_SERIES
    ]

    async with sf() as session:
        async with session.begin():
            stmt = pg_insert(DeMacroMaster).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["ticker"],
                set_={
                    "name": stmt.excluded.name,
                    "source": stmt.excluded.source,
                    "unit": stmt.excluded.unit,
                    "frequency": stmt.excluded.frequency,
                },
            )
            await session.execute(stmt)

    return len(rows)


async def _backfill_fred_history(sf: async_sessionmaker) -> int:
    """Fetch FRED series history and upsert into de_macro_values."""
    fred_api_key = os.environ.get("FRED_API_KEY") or get_settings().fred_api_key
    if not fred_api_key:
        logger.warning("stream_4_fred_skipped", reason="FRED_API_KEY not set")
        return 0

    try:
        from fredapi import Fred
    except ImportError:
        logger.warning("stream_4_fred_skipped", reason="fredapi not installed")
        return 0

    fred = Fred(api_key=fred_api_key)
    fred_tickers = [s["ticker"] for s in MACRO_SERIES if s["source"] == "FRED"]

    total = 0
    for series_id in fred_tickers:
        try:
            series = fred.get_series(series_id, observation_start="2016-01-01")
            rows = [
                {"date": idx.date(), "ticker": series_id, "value": _safe_decimal(val)}
                for idx, val in series.items()
                if val is not None and str(val) != "nan"
            ]
            if not rows:
                continue

            async with sf() as session:
                async with session.begin():
                    stmt = pg_insert(DeMacroValues).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["date", "ticker"],
                        set_={"value": stmt.excluded.value},
                    )
                    await session.execute(stmt)
            total += len(rows)
            logger.info("stream_4_fred_series_done", series=series_id, rows=len(rows))
        except Exception as exc:
            logger.error("stream_4_fred_series_failed", series=series_id, error=str(exc))

    return total


async def stream_4_global_macro(sf: async_sessionmaker) -> None:
    """Truncate, seed, and backfill global instruments and macro series."""
    logger.info("stream_4_start", stream="global_macro")

    # 4.1 Truncate bad global data
    try:
        await _truncate_global_tables(sf)
    except Exception as exc:
        logger.error("stream_4_truncate_failed", error=str(exc))

    # 4.2 Seed global instrument master
    try:
        n_instruments = await _seed_global_instruments(sf)
        logger.info("stream_4_global_master_seeded", count=n_instruments)
    except Exception as exc:
        logger.error("stream_4_global_master_failed", error=str(exc))
        return  # Cannot proceed without master rows (FK constraint)

    # 4.3 Backfill global prices
    try:
        n_prices = await _backfill_global_prices(sf)
        logger.info("stream_4_global_prices_done", rows=n_prices)
    except Exception as exc:
        logger.error("stream_4_global_prices_failed", error=str(exc))

    # 4.4 Seed macro master
    try:
        n_macro = await _seed_macro_master(sf)
        logger.info("stream_4_macro_master_seeded", count=n_macro)
    except Exception as exc:
        logger.error("stream_4_macro_master_failed", error=str(exc))
        return  # Cannot proceed without macro master rows (FK constraint)

    # 4.5 Backfill FRED history
    try:
        n_fred = await _backfill_fred_history(sf)
        logger.info("stream_4_fred_done", rows=n_fred)
    except Exception as exc:
        logger.error("stream_4_fred_failed", error=str(exc))

    logger.info("stream_4_done")


# ---------------------------------------------------------------------------
# Stream 5 — Flows
# ---------------------------------------------------------------------------

async def stream_5_flows(sf: async_sessionmaker) -> None:
    """FII/DII, F&O flow backfill + MF category flows backfill."""
    logger.info("stream_5_start", stream="flows")

    logger.info(
        "stream_5_fii_dii_skipped",
        note="FII/DII backfill — NSE API limited to ~3 months history. Run daily pipeline going forward.",
    )
    logger.info(
        "stream_5_fno_skipped",
        note="F&O summary backfill — NSE option chain is current-day only. Run daily pipeline going forward.",
    )

    # 5.1 MF Category Flows — monthly backfill (2020-01 → current)
    try:
        from app.pipelines.flows.mf_category_flows import MfCategoryFlowsPipeline

        pipeline = MfCategoryFlowsPipeline()
        months_ok, months_fail = 0, 0

        year, month = 2020, 1
        today = date.today()
        end_year, end_month = today.year, today.month

        while (year, month) <= (end_year, end_month):
            first_of_month = date(year, month, 1)
            run_log = _make_run_log("mf_category_flows")
            try:
                async with sf() as session:
                    async with session.begin():
                        await pipeline.execute(
                            business_date=first_of_month,
                            session=session,
                            run_log=run_log,
                        )
                months_ok += 1
            except Exception as exc:
                months_fail += 1
                logger.error(
                    "stream_5_mf_category_month_failed",
                    month=f"{year}-{month:02d}",
                    error=str(exc),
                )

            if month == 12:
                year, month = year + 1, 1
            else:
                month += 1

        logger.info("stream_5_mf_category_done", months_ok=months_ok, months_fail=months_fail)
    except Exception as exc:
        logger.error("stream_5_mf_category_failed", error=str(exc))

    logger.info("stream_5_done")


# ---------------------------------------------------------------------------
# Stream 6 — Cross-cutting
# ---------------------------------------------------------------------------

async def stream_6_crosscutting(sf: async_sessionmaker) -> None:
    """Market cap history + symbol history extraction."""
    logger.info("stream_6_start", stream="crosscutting")

    # 6.1 Market Cap History — semi-annual backfill (Jan & Jul, 2020 → current)
    try:
        from app.pipelines.equity.market_cap_history import MarketCapHistoryPipeline

        pipeline = MarketCapHistoryPipeline()
        periods_ok, periods_fail = 0, 0

        today = date.today()
        for year in range(2020, today.year + 1):
            for month in (1, 7):
                period_date = date(year, month, 1)
                if period_date > today:
                    break
                run_log = _make_run_log("market_cap_history")
                try:
                    async with sf() as session:
                        async with session.begin():
                            await pipeline.execute(
                                business_date=period_date,
                                session=session,
                                run_log=run_log,
                            )
                    periods_ok += 1
                except Exception as exc:
                    periods_fail += 1
                    logger.error(
                        "stream_6_market_cap_period_failed",
                        period=f"{year}-{month:02d}",
                        error=str(exc),
                    )

        logger.info("stream_6_market_cap_done", periods_ok=periods_ok, periods_fail=periods_fail)
    except Exception as exc:
        logger.error("stream_6_market_cap_failed", error=str(exc))

    # 6.2 Symbol History — one-shot extraction from corporate actions
    try:
        from app.pipelines.equity.symbol_history import SymbolHistoryPipeline

        pipeline = SymbolHistoryPipeline()
        run_log = _make_run_log("symbol_history")
        async with sf() as session:
            async with session.begin():
                result = await pipeline.execute(
                    business_date=date.today(),
                    session=session,
                    run_log=run_log,
                )
        logger.info(
            "stream_6_symbol_history_done",
            rows_processed=result.rows_processed if result else 0,
        )
    except Exception as exc:
        logger.error("stream_6_symbol_history_failed", error=str(exc))

    logger.info("stream_6_done")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main(streams: list[int] | None = None, dry_run: bool = False) -> None:
    """Run the full ingestion orchestration.

    Args:
        streams: Which stream numbers to run. None = all (0-6).
        dry_run: If True, log the plan but do not execute.
    """
    # Default: streams 0,1,2,3,5,6. Stream 4 (global+macro) is opt-in.
    target_streams = streams if streams is not None else [0, 1, 2, 3, 5, 6]

    logger.info(
        "orchestrator_start",
        target_streams=target_streams,
        dry_run=dry_run,
    )

    if dry_run:
        logger.info("dry_run_plan", streams=target_streams)
        stream_names = {
            0: "Foundation (trading calendar + instrument master)",
            1: "Equity (corporate actions backfill)",
            2: "Mutual Funds (NAV universe + returns)",
            3: "Indices (missing names + constituents)",
            4: "Global + Macro (yfinance + FRED)",
            5: "Flows (FII/DII + F&O deferred, MF category flows backfill)",
            6: "Cross-cutting (market cap history + symbol history extraction)",
        }
        for s in target_streams:
            logger.info("dry_run_stream", stream=s, description=stream_names.get(s, "unknown"))
        return

    settings = get_settings()
    database_url = os.environ.get("DATABASE_URL", settings.database_url)

    engine = create_async_engine(
        database_url,
        pool_size=12,
        max_overflow=5,
        pool_pre_ping=True,
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)

    try:
        # Stream 0 must run first — downstream streams depend on instruments
        if 0 in target_streams:
            await stream_0_foundation(sf)

        # Streams 1-5 run in parallel
        parallel = [s for s in target_streams if s in (1, 2, 3, 4, 5)]
        if parallel:
            tasks = []
            if 1 in parallel:
                tasks.append(stream_1_equity(sf))
            if 2 in parallel:
                tasks.append(stream_2_mf(sf))
            if 3 in parallel:
                tasks.append(stream_3_indices(sf))
            if 4 in parallel:
                tasks.append(stream_4_global_macro(sf))
            if 5 in parallel:
                tasks.append(stream_5_flows(sf))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    logger.error("stream_parallel_failed", error=str(r))

        # Stream 6 runs last
        if 6 in target_streams:
            await stream_6_crosscutting(sf)

    finally:
        await engine.dispose()

    logger.info("orchestrator_complete", target_streams=target_streams)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JIP ingestion orchestrator — seeds all historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--streams",
        type=str,
        default=None,
        help="Comma-separated stream numbers to run, e.g. 0,4. Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log the execution plan without running any pipelines.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    selected_streams: list[int] | None = None
    if args.streams:
        try:
            selected_streams = [int(s.strip()) for s in args.streams.split(",")]
        except ValueError:
            print(f"Invalid --streams value: {args.streams!r}", file=sys.stderr)
            sys.exit(1)

    asyncio.run(main(streams=selected_streams, dry_run=args.dry_run))
