"""NSE T+1 delivery data ingestion pipeline.

Downloads NSE delivery volume data for the last trading day and updates
de_equity_ohlcv with delivery_vol and delivery_pct.
"""

from __future__ import annotations

import asyncio
import io
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import pandas as pd

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.models.pipeline import DePipelineLog
from app.models.prices import DeEquityOhlcv
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

# NSE delivery position URL
NSE_DELIVERY_URL = (
    "https://nsearchives.nseindia.com/products/content/MTO_{date_str}.DAT"
)

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
}

DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0


def build_delivery_url(target_date: date) -> str:
    """Build NSE delivery data URL for the given date."""
    date_str = target_date.strftime("%d%m%Y")
    return NSE_DELIVERY_URL.format(date_str=date_str)


def parse_delivery_data(content: bytes | str) -> pd.DataFrame:
    """Parse NSE MTO (Mark-to-Delivery) delivery CSV/DAT file.

    Args:
        content: Raw bytes or string content of the delivery file.

    Returns:
        DataFrame with columns: symbol, series, traded_qty, delivery_qty,
        delivery_pct. Filtered to EQ series only.
    """
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    rows: list[dict[str, Any]] = []
    lines = content.strip().splitlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        cols = [c.strip() for c in line.split(",")]
        if len(cols) < 7:
            continue

        rec_type = cols[0].strip()
        if not rec_type.isdigit():
            continue
        if rec_type not in ("1", "20"):
            continue

        symbol = cols[2].strip().upper() if len(cols) > 2 else ""
        series = cols[3].strip().upper() if len(cols) > 3 else "EQ"
        if not symbol:
            continue

        # Only EQ series
        if series != "EQ":
            continue

        traded_qty: Optional[int] = None
        delivery_qty: Optional[int] = None
        delivery_pct: Optional[Decimal] = None

        try:
            traded_qty_str = cols[4].strip() if len(cols) > 4 else ""
            if traded_qty_str:
                traded_qty = int(Decimal(traded_qty_str))
        except (InvalidOperation, ValueError):
            pass

        try:
            delivery_qty_str = cols[5].strip() if len(cols) > 5 else ""
            if delivery_qty_str:
                delivery_qty = int(Decimal(delivery_qty_str))
        except (InvalidOperation, ValueError):
            pass

        try:
            pct_str = cols[6].strip() if len(cols) > 6 else ""
            if pct_str:
                delivery_pct = Decimal(pct_str)
        except InvalidOperation:
            pass

        rows.append(
            {
                "symbol": symbol,
                "series": series,
                "traded_qty": traded_qty,
                "delivery_qty": delivery_qty,
                "delivery_pct": delivery_pct,
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=["symbol", "series", "traded_qty", "delivery_qty", "delivery_pct"]
        )

    return pd.DataFrame(rows)


async def get_last_trading_day(
    session: AsyncSession,
    before_date: date,
) -> Optional[date]:
    """Get the most recent trading day before the given date."""
    from app.pipelines.calendar import is_trading_day

    result = await is_trading_day(session, before_date)
    if result:
        return before_date
    # Simple lookback — check up to 10 days back
    from datetime import timedelta

    for i in range(1, 11):
        check = before_date - timedelta(days=i)
        if await is_trading_day(session, check):
            return check
    return None


async def fetch_with_retry(
    url: str,
    retries: int = DOWNLOAD_RETRIES,
) -> bytes:
    """Download URL content with retries and exponential backoff."""
    last_exc: Optional[Exception] = None
    async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0) as client:
        for attempt in range(retries):
            try:
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                return response.content
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_exc = exc
                wait_secs = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "delivery_download_retry",
                    url=url,
                    attempt=attempt + 1,
                    wait_secs=wait_secs,
                    error=str(exc),
                )
                if attempt < retries - 1:
                    await asyncio.sleep(wait_secs)

    raise last_exc or RuntimeError(f"Failed to download: {url}")


async def bulk_resolve_symbols(
    session: AsyncSession,
    symbols: list[str],
) -> dict[str, Any]:
    """Resolve list of symbols to instrument IDs.

    Returns:
        Dict mapping uppercase symbol → instrument UUID.
    """
    if not symbols:
        return {}
    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.current_symbol.in_(symbols),
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    return {row.current_symbol.upper(): row.id for row in result}


class DeliveryPipeline(BasePipeline):
    """NSE delivery data ingestion pipeline.

    Downloads T+1 delivery data and updates de_equity_ohlcv with
    delivery_vol and delivery_pct for the previous trading day.
    """

    pipeline_name = "equity_delivery"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Download delivery data and update OHLCV rows."""
        logger.info("delivery_execute_start", business_date=business_date.isoformat())

        # Get previous trading day (T+1 means we update yesterday's data)
        prev_day = await get_last_trading_day(session, business_date)
        if prev_day is None:
            logger.warning("delivery_no_previous_trading_day")
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Download delivery data
        url = build_delivery_url(prev_day)
        raw_content = await fetch_with_retry(url)

        # Parse
        df = parse_delivery_data(raw_content)
        if df.empty:
            logger.warning("delivery_no_rows", business_date=business_date.isoformat())
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Resolve symbols
        symbols = df["symbol"].unique().tolist()
        symbol_map = await bulk_resolve_symbols(session, symbols)

        rows_processed = 0
        rows_failed = 0

        for _, row in df.iterrows():
            symbol = row["symbol"]
            instrument_id = symbol_map.get(symbol)
            if instrument_id is None:
                rows_failed += 1
                continue

            delivery_qty = row.get("delivery_qty")
            delivery_pct = row.get("delivery_pct")

            update_values: dict[str, Any] = {}
            if delivery_qty is not None:
                update_values["delivery_vol"] = int(delivery_qty)
            if delivery_pct is not None:
                update_values["delivery_pct"] = Decimal(str(delivery_pct))

            if update_values:
                result = await session.execute(
                    update(DeEquityOhlcv)
                    .where(
                        DeEquityOhlcv.date == prev_day,
                        DeEquityOhlcv.instrument_id == instrument_id,
                    )
                    .values(**update_values)
                )
                if result.rowcount > 0:
                    rows_processed += 1

        logger.info(
            "delivery_execute_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """No specific anomaly detection for delivery data."""
        return []
