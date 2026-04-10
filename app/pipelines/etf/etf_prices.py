"""ETF price ingestion pipeline — fetches daily OHLCV for all ETFs via yfinance.

Reuses the yfinance download + parse pattern from global_data.yfinance_pipeline.
Upserts into de_etf_ohlcv with ON CONFLICT on (date, ticker).

Trigger: daily after market close.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import yfinance as yf
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.etf import DeEtfOhlcv
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


def _safe_decimal(value: Any) -> Decimal | None:
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


async def fetch_etf_prices(
    tickers: list[str],
    business_date: date,
) -> list[dict[str, Any]]:
    """Download ETF prices via yfinance for the given business_date."""
    if not tickers:
        return []

    # Download in batches of 50 to avoid yfinance limits
    all_rows: list[dict[str, Any]] = []
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            df = yf.download(
                " ".join(batch),
                period="5d",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
            )
        except Exception as exc:
            logger.error("etf_yfinance_download_error", batch_start=i, error=str(exc))
            continue

        if df is None or df.empty:
            continue

        # Find best date (exact or nearest within ±3 days)
        from datetime import timedelta
        date_str = business_date.isoformat()
        day_df = None
        try:
            day_df = df.loc[date_str:date_str]
        except Exception:
            pass

        if day_df is None or day_df.empty:
            for offset in range(1, 4):
                for delta in [timedelta(days=-offset), timedelta(days=offset)]:
                    alt = (business_date + delta).isoformat()
                    try:
                        day_df = df.loc[alt:alt]
                        if not day_df.empty:
                            break
                    except Exception:
                        continue
                if day_df is not None and not day_df.empty:
                    break

        if day_df is None or day_df.empty:
            continue

        # Detect MultiIndex column order
        if hasattr(df.columns, "levels") and len(df.columns.levels) > 1:
            first_level = set(df.columns.get_level_values(0))
            ticker_first = bool(first_level & set(batch))

            for ticker in batch:
                try:
                    if ticker_first:
                        close_val = _safe_decimal(day_df[(ticker, "Close")].iloc[0])
                        if close_val is None:
                            continue
                        all_rows.append({
                            "date": business_date,
                            "ticker": ticker,
                            "open": _safe_decimal(day_df[(ticker, "Open")].iloc[0]),
                            "high": _safe_decimal(day_df[(ticker, "High")].iloc[0]),
                            "low": _safe_decimal(day_df[(ticker, "Low")].iloc[0]),
                            "close": close_val,
                            "volume": int(day_df[(ticker, "Volume")].iloc[0] or 0),
                        })
                    else:
                        close_val = _safe_decimal(day_df[("Close", ticker)].iloc[0])
                        if close_val is None:
                            continue
                        all_rows.append({
                            "date": business_date,
                            "ticker": ticker,
                            "open": _safe_decimal(day_df[("Open", ticker)].iloc[0]),
                            "high": _safe_decimal(day_df[("High", ticker)].iloc[0]),
                            "low": _safe_decimal(day_df[("Low", ticker)].iloc[0]),
                            "close": close_val,
                            "volume": int(day_df[("Volume", ticker)].iloc[0] or 0),
                        })
                except (KeyError, IndexError):
                    continue
        elif len(batch) == 1:
            ticker = batch[0]
            try:
                close_val = _safe_decimal(day_df["Close"].iloc[0])
                if close_val is not None:
                    all_rows.append({
                        "date": business_date,
                        "ticker": ticker,
                        "open": _safe_decimal(day_df.get("Open", [None]).iloc[0]),
                        "high": _safe_decimal(day_df.get("High", [None]).iloc[0]),
                        "low": _safe_decimal(day_df.get("Low", [None]).iloc[0]),
                        "close": close_val,
                        "volume": int(day_df.get("Volume", [0]).iloc[0] or 0),
                    })
            except (KeyError, IndexError):
                pass

    return all_rows


class EtfPricePipeline(BasePipeline):
    """Downloads daily OHLCV for all ETFs from de_etf_master via yfinance."""

    pipeline_name = "etf_prices"
    requires_trading_day = False
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        import sqlalchemy as sa

        # Get all active ETF tickers + exchange from master
        result = await session.execute(
            sa.text("SELECT ticker, exchange FROM de_etf_master WHERE is_active = TRUE ORDER BY ticker")
        )
        rows_raw = result.fetchall()

        # Build yfinance-compatible ticker list: NSE tickers need .NS suffix
        yf_to_db: dict[str, str] = {}
        for db_ticker, exchange in rows_raw:
            if exchange == "NSE":
                yf_ticker = db_ticker + ".NS"
            else:
                yf_ticker = db_ticker
            yf_to_db[yf_ticker] = db_ticker

        yf_tickers = list(yf_to_db.keys())

        logger.info("etf_prices_start", ticker_count=len(yf_tickers), business_date=business_date.isoformat())

        raw_rows = await fetch_etf_prices(yf_tickers, business_date)

        # Map yfinance tickers back to DB tickers
        rows: list[dict[str, Any]] = []
        for row in raw_rows:
            yf_t = row["ticker"]
            row["ticker"] = yf_to_db.get(yf_t, yf_t)
            rows.append(row)

        if not rows:
            logger.warning("etf_prices_zero_rows", business_date=business_date.isoformat())
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Upsert
        stmt = pg_insert(DeEtfOhlcv).values(rows)
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

        logger.info("etf_prices_done", rows=len(rows), business_date=business_date.isoformat())

        return ExecutionResult(rows_processed=len(rows), rows_failed=0)
