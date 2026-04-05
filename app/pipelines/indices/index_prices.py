"""NSE index OHLCV pipeline — fetches all NSE indices including India VIX.

Track C pipeline. Triggered at 18:30 IST after market close.
Inserts into de_index_prices with ON CONFLICT DO UPDATE.
India VIX values are also stored in de_macro_values under ticker 'INDIAVIX'.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instruments import DeIndexMaster
from app.models.pipeline import DePipelineLog
from app.models.prices import DeIndexPrices, DeMacroValues
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = structlog.get_logger(__name__)

# NSE API endpoints
NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# India VIX ticker in macro master
INDIA_VIX_TICKER = "INDIAVIX"
INDIA_VIX_INDEX_CODE = "INDIA VIX"


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal safely. Returns None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Convert a value to int safely. Returns None on failure."""
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def _parse_index_record(record: dict[str, Any]) -> dict[str, Any]:
    """Parse a single NSE allIndices record into a normalised dict."""
    return {
        "index_code": record.get("index", "").strip(),
        "open": _safe_decimal(record.get("open")),
        "high": _safe_decimal(record.get("high")),
        "low": _safe_decimal(record.get("low")),
        "close": _safe_decimal(record.get("last")),
        "volume": _safe_int(record.get("turnover")),  # turnover as proxy volume
        "pe_ratio": _safe_decimal(record.get("pe")),
        "pb_ratio": _safe_decimal(record.get("pb")),
        "div_yield": _safe_decimal(record.get("divYield")),
    }


async def _fetch_nse_all_indices(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch current snapshot of all NSE indices from allIndices endpoint."""
    # Establish session cookie first
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS)
    response = await client.get(NSE_ALL_INDICES_URL, headers=NSE_HEADERS)
    response.raise_for_status()
    data = response.json()
    records: list[dict[str, Any]] = data.get("data", [])
    logger.info("nse_indices_fetched", count=len(records))
    return records


async def _load_known_index_codes(session: AsyncSession) -> set[str]:
    """Return set of all index_codes currently in de_index_master."""
    result = await session.execute(select(DeIndexMaster.index_code))
    return {row[0] for row in result.fetchall()}


async def _upsert_index_prices(
    session: AsyncSession,
    trade_date: date,
    records: list[dict[str, Any]],
    known_codes: set[str],
) -> tuple[int, int]:
    """Upsert index price records into de_index_prices.

    Returns (rows_processed, rows_failed).
    """
    rows_processed = 0
    rows_failed = 0

    for rec in records:
        index_code = rec.get("index_code", "")
        if not index_code:
            rows_failed += 1
            continue

        # Only insert for indices that exist in master
        if index_code not in known_codes:
            logger.debug("index_code_not_in_master", index_code=index_code)
            rows_failed += 1
            continue

        stmt = (
            pg_insert(DeIndexPrices)
            .values(
                date=trade_date,
                index_code=index_code,
                open=rec["open"],
                high=rec["high"],
                low=rec["low"],
                close=rec["close"],
                volume=rec["volume"],
                pe_ratio=rec.get("pe_ratio"),
                pb_ratio=rec.get("pb_ratio"),
                div_yield=rec.get("div_yield"),
            )
            .on_conflict_do_update(
                index_elements=["date", "index_code"],
                set_={
                    "open": rec["open"],
                    "high": rec["high"],
                    "low": rec["low"],
                    "close": rec["close"],
                    "volume": rec["volume"],
                    "pe_ratio": rec.get("pe_ratio"),
                    "pb_ratio": rec.get("pb_ratio"),
                    "div_yield": rec.get("div_yield"),
                    "updated_at": DeIndexPrices.__table__.c.updated_at,
                },
            )
        )
        try:
            await session.execute(stmt)
            rows_processed += 1
        except Exception as exc:
            logger.warning(
                "index_price_upsert_failed",
                index_code=index_code,
                trade_date=trade_date.isoformat(),
                error=str(exc),
            )
            rows_failed += 1

    return rows_processed, rows_failed


async def _upsert_india_vix_macro(
    session: AsyncSession,
    trade_date: date,
    vix_value: Optional[Decimal],
) -> None:
    """Upsert India VIX value into de_macro_values."""
    if vix_value is None:
        logger.warning("india_vix_value_missing", trade_date=trade_date.isoformat())
        return

    stmt = (
        pg_insert(DeMacroValues)
        .values(
            date=trade_date,
            ticker=INDIA_VIX_TICKER,
            value=vix_value,
        )
        .on_conflict_do_update(
            index_elements=["date", "ticker"],
            set_={"value": vix_value},
        )
    )
    await session.execute(stmt)
    logger.info(
        "india_vix_upserted",
        trade_date=trade_date.isoformat(),
        value=str(vix_value),
    )


class IndexPricePipeline(BasePipeline):
    """Fetch all NSE index OHLCV data for a trading day.

    Track C — EOD pipeline. Triggered at 18:30 IST.
    Sources:
      - NSE allIndices API for current day snapshot
    Sinks:
      - de_index_prices (ON CONFLICT DO UPDATE)
      - de_macro_values where ticker = 'INDIAVIX'
    """

    pipeline_name = "index_prices"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch NSE indices and upsert into de_index_prices."""
        logger.info(
            "index_price_pipeline_start",
            business_date=business_date.isoformat(),
        )

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            raw_records = await _fetch_nse_all_indices(client)

        known_codes = await _load_known_index_codes(session)
        parsed = [_parse_index_record(r) for r in raw_records]

        rows_processed, rows_failed = await _upsert_index_prices(
            session, business_date, parsed, known_codes
        )

        # Extract and upsert India VIX separately into macro values
        vix_record = next(
            (r for r in parsed if r.get("index_code") == INDIA_VIX_INDEX_CODE),
            None,
        )
        if vix_record:
            await _upsert_india_vix_macro(
                session, business_date, vix_record.get("close")
            )

        logger.info(
            "index_price_pipeline_complete",
            business_date=business_date.isoformat(),
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
