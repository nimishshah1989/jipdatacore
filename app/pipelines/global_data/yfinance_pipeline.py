"""yfinance pipeline — global indices, bonds, commodities, FX, and crypto.

Trigger: 07:30 IST.
SLA: 08:00 IST.
"""

from __future__ import annotations


from datetime import date
from decimal import Decimal
from typing import Any

import yfinance as yf
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeGlobalPrices
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

# Global indices
GLOBAL_INDEX_TICKERS = [
    "^GSPC",    # S&P 500
    "^IXIC",    # NASDAQ Composite
    "^DJI",     # Dow Jones Industrial Average
    "^FTSE",    # FTSE 100
    "^GDAXI",   # DAX
    "^FCHI",    # CAC 40
    "^N225",    # Nikkei 225
    "^HSI",     # Hang Seng
    "000001.SS",  # Shanghai Composite
    "^AXJO",    # ASX 200
    "EEM",      # MSCI Emerging Markets ETF
    "URTH",     # MSCI World ETF
]

# Commodities and FX (original set)
COMMODITY_FX_TICKERS = [
    "DX-Y.NYB",   # US Dollar Index
    "CL=F",       # Crude Oil (WTI)
    "BZ=F",       # Crude Oil (Brent)
    "GC=F",       # Gold
    "SI=F",       # Silver
    "USDINR=X",   # USD/INR
    "USDJPY=X",   # USD/JPY
    "EURUSD=X",   # EUR/USD
    "USDCNH=X",   # USD/CNH (offshore RMB)
]

# US Treasury yield indices (CBOE)
BOND_TICKERS = [
    "^TNX",   # US 10-Year Treasury Yield
    "^TYX",   # US 30-Year Treasury Yield
    "^IRX",   # US 13-Week Treasury Bill
    "^FVX",   # US 5-Year Treasury Yield
]

# Additional commodity futures
COMMODITY_EXTRA_TICKERS = [
    "HG=F",   # Copper Futures
    "NG=F",   # Natural Gas Futures
    "ZC=F",   # Corn Futures
    "ZW=F",   # Wheat Futures
    "ZS=F",   # Soybean Futures
    "KC=F",   # Coffee Futures
    "CT=F",   # Cotton Futures
    "PL=F",   # Platinum Futures
]

# Additional FX pairs
FOREX_EXTRA_TICKERS = [
    "GBPUSD=X",   # GBP/USD
    "AUDUSD=X",   # AUD/USD
    "USDCAD=X",   # USD/CAD
    "USDCHF=X",   # USD/CHF
    "USDBRL=X",   # USD/BRL
    "USDKRW=X",   # USD/KRW
    "USDMXN=X",   # USD/MXN
]

# Crypto
CRYPTO_TICKERS = [
    "BTC-USD",   # Bitcoin
    "ETH-USD",   # Ethereum
]

ALL_TICKERS = (
    GLOBAL_INDEX_TICKERS
    + COMMODITY_FX_TICKERS
    + BOND_TICKERS
    + COMMODITY_EXTRA_TICKERS
    + FOREX_EXTRA_TICKERS
    + CRYPTO_TICKERS
)


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely; return None on failure or NaN."""
    if value is None:
        return None
    try:
        import math

        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return Decimal(str(round(f, 4)))
    except Exception:
        return None


def parse_yfinance_download(
    df: Any,
    business_date: date,
    tickers: list[str],
) -> list[dict[str, Any]]:
    """Parse a yfinance multi-ticker download DataFrame into DB row dicts.

    yfinance.download() returns a MultiIndex DataFrame when multiple tickers
    are requested: columns are (field, ticker).

    Returns list of row dicts for the specified business_date only.
    """
    rows: list[dict[str, Any]] = []

    if df is None or df.empty:
        return rows

    # Filter to the business_date — use nearest available date within ±3 days
    # Non-US tickers may not have data on the exact date (holidays, timezone lag)
    date_str = business_date.isoformat()
    try:
        day_df = df.loc[date_str:date_str]
    except Exception:
        day_df = None

    if day_df is None or day_df.empty:
        # Search for nearest date within ±3 trading days
        from datetime import timedelta

        for offset in range(1, 4):
            for delta in [timedelta(days=-offset), timedelta(days=offset)]:
                alt_date = (business_date + delta).isoformat()
                try:
                    day_df = df.loc[alt_date:alt_date]
                    if not day_df.empty:
                        break
                except Exception:
                    continue
            if day_df is not None and not day_df.empty:
                break

    if day_df is None or day_df.empty:
        return rows

    # yfinance multi-ticker: columns are MultiIndex (field, ticker)
    # Single ticker: flat columns
    if hasattr(df.columns, "levels") and len(df.columns.levels) > 1:
        # MultiIndex columns
        for ticker in tickers:
            try:
                open_val = _safe_decimal(day_df[("Open", ticker)].iloc[0])
                high_val = _safe_decimal(day_df[("High", ticker)].iloc[0])
                low_val = _safe_decimal(day_df[("Low", ticker)].iloc[0])
                close_val = _safe_decimal(day_df[("Close", ticker)].iloc[0])
                volume_raw = day_df[("Volume", ticker)].iloc[0]
                try:
                    volume_val = int(volume_raw) if volume_raw is not None else None
                except (TypeError, ValueError):
                    volume_val = None
            except (KeyError, IndexError):
                continue

            if close_val is None:
                continue

            rows.append(
                {
                    "date": business_date,
                    "ticker": ticker,
                    "open": open_val,
                    "high": high_val,
                    "low": low_val,
                    "close": close_val,
                    "volume": volume_val,
                }
            )
    else:
        # Single ticker flat columns (fallback)
        if len(tickers) == 1:
            ticker = tickers[0]
            try:
                close_val = _safe_decimal(day_df["Close"].iloc[0])
                if close_val is None:
                    return rows
                rows.append(
                    {
                        "date": business_date,
                        "ticker": ticker,
                        "open": _safe_decimal(day_df.get("Open", [None]).iloc[0]),
                        "high": _safe_decimal(day_df.get("High", [None]).iloc[0]),
                        "low": _safe_decimal(day_df.get("Low", [None]).iloc[0]),
                        "close": close_val,
                        "volume": None,
                    }
                )
            except (KeyError, IndexError):
                pass

    return rows


async def fetch_global_prices(
    tickers: list[str],
    business_date: date,
) -> list[dict[str, Any]]:
    """Download prices for the given tickers via yfinance.

    Uses period='5d' to ensure data for the business_date is available even
    if run early morning (yesterday's close).
    """
    if not tickers:
        return []

    df = yf.download(
        tickers=" ".join(tickers),
        period="5d",
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    return parse_yfinance_download(df, business_date, tickers)


async def upsert_global_prices(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert global price rows into de_global_prices.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeGlobalPrices).values(rows)
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
    return len(rows), 0


class YfinancePipeline(BasePipeline):
    """Downloads global index, bond, commodity, FX, and crypto prices via yfinance.

    Covers S&P 500, NASDAQ, Nikkei, Hang Seng, European indices,
    US Treasury yield indices (10Y/30Y/5Y/13W), crude oil, gold, silver,
    copper, natural gas, agricultural commodities, platinum, USD index,
    key FX pairs (12 pairs), and Bitcoin/Ethereum.

    Trigger: 07:30 IST.
    SLA: 08:00 IST.
    """

    pipeline_name = "yfinance_global"
    requires_trading_day = False  # Global markets have different calendars
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "yfinance_execute_start",
            ticker_count=len(ALL_TICKERS),
            business_date=business_date.isoformat(),
        )

        rows = await fetch_global_prices(ALL_TICKERS, business_date)

        logger.info(
            "yfinance_downloaded",
            rows_parsed=len(rows),
            business_date=business_date.isoformat(),
        )

        rows_processed, rows_failed = await upsert_global_prices(session, rows)

        logger.info(
            "yfinance_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
