"""NSE index prices pipeline — downloads daily OHLCV for all 60+ NSE indices."""

from __future__ import annotations


from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeIndexPrices
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely; return None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


async def _fetch_all_indices(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    """Fetch allIndices JSON from NSE. Returns the list of index dicts."""
    # NSE requires an initial page hit to set cookies before the API call
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(NSE_ALL_INDICES_URL, headers=NSE_HEADERS, timeout=15.0)
    response.raise_for_status()
    data = response.json()
    return data.get("data", [])


def _parse_index_row(
    record: dict[str, Any],
    business_date: date,
) -> dict[str, Any] | None:
    """Parse a single NSE index API record into a DB row dict.

    Returns None if the record is missing critical fields.
    """
    index_symbol = record.get("indexSymbol") or record.get("index")
    if not index_symbol:
        return None

    close = _safe_decimal(record.get("last"))
    if close is None:
        return None

    return {
        "date": business_date,
        "index_code": str(index_symbol).strip().upper(),
        "open": _safe_decimal(record.get("open")),
        "high": _safe_decimal(record.get("high")),
        "low": _safe_decimal(record.get("low")),
        "close": close,
        "pe_ratio": _safe_decimal(record.get("pe")),
        "pb_ratio": _safe_decimal(record.get("pb")),
        "div_yield": _safe_decimal(record.get("dy")),
    }


async def upsert_index_prices(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert index price rows into de_index_prices.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeIndexPrices).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "index_code"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "pe_ratio": stmt.excluded.pe_ratio,
            "pb_ratio": stmt.excluded.pb_ratio,
            "div_yield": stmt.excluded.div_yield,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class NseIndicesPipeline(BasePipeline):
    """Downloads daily OHLCV data for all NSE indices.

    Source: https://www.nseindia.com/api/allIndices
    Trigger: After market close (typically 16:00 IST).
    SLA: 17:00 IST.
    """

    pipeline_name = "nse_indices"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "nse_indices_execute_start",
            business_date=business_date.isoformat(),
        )

        async with httpx.AsyncClient() as client:
            try:
                raw_records = await _fetch_all_indices(client)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "nse_indices_http_error",
                    status_code=exc.response.status_code,
                    url=str(exc.request.url),
                )
                raise

        rows: list[dict[str, Any]] = []
        skipped = 0

        for record in raw_records:
            parsed = _parse_index_row(record, business_date)
            if parsed is None:
                skipped += 1
                continue
            rows.append(parsed)

        logger.info(
            "nse_indices_parsed",
            total_records=len(raw_records),
            valid_rows=len(rows),
            skipped=skipped,
            business_date=business_date.isoformat(),
        )

        rows_processed, rows_failed = await upsert_index_prices(session, rows)

        logger.info(
            "nse_indices_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
