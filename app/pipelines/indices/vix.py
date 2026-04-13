"""India VIX pipeline — fetches India VIX from NSE and stores in de_macro_values."""

from __future__ import annotations


from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeMacroValues
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.indices.nse_indices import NSE_HEADERS

logger = get_logger(__name__)

NSE_VIX_URL = "https://www.nseindia.com/api/allIndices"
INDIAVIX_TICKER = "INDIAVIX"

# NSE returns VIX under this index symbol
VIX_INDEX_SYMBOL = "INDIA VIX"


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert value to Decimal safely."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


async def _fetch_vix_value(client: httpx.AsyncClient) -> Decimal | None:
    """Fetch India VIX value from the NSE allIndices API.

    Returns the last (close) value, or None if not found.
    """
    # Warm up session to set cookies
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(NSE_VIX_URL, headers=NSE_HEADERS, timeout=15.0)
    response.raise_for_status()

    data = response.json()
    records: list[dict[str, Any]] = data.get("data", [])

    for record in records:
        index_symbol = (record.get("indexSymbol") or record.get("index") or "").strip().upper()
        if index_symbol == VIX_INDEX_SYMBOL:
            return _safe_decimal(record.get("last"))

    return None


async def _ensure_vix_master(session: AsyncSession) -> None:
    """Ensure the INDIAVIX row exists in de_macro_master.

    de_macro_values has a FK on de_macro_master.ticker, so inserting a VIX
    observation before the master row exists fails with a ForeignKeyViolation.
    This helper is idempotent.
    """
    import sqlalchemy as sa

    await session.execute(
        sa.text(
            """
            INSERT INTO de_macro_master (ticker, name, source, unit, frequency)
            VALUES ('INDIAVIX', 'India VIX', 'NSE', 'index', 'daily')
            ON CONFLICT (ticker) DO NOTHING
            """
        )
    )


async def upsert_vix_value(
    session: AsyncSession,
    business_date: date,
    vix_value: Decimal,
) -> None:
    """Upsert India VIX into de_macro_values for ticker=INDIAVIX."""
    await _ensure_vix_master(session)
    stmt = pg_insert(DeMacroValues).values(
        [{"date": business_date, "ticker": INDIAVIX_TICKER, "value": vix_value}]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "ticker"],
        set_={"value": stmt.excluded.value},
    )
    await session.execute(stmt)


class IndiaVixPipeline(BasePipeline):
    """Fetches India VIX from NSE and stores it in de_macro_values.

    Uses the allIndices API endpoint which includes the VIX alongside indices.
    Trigger: After market close.
    SLA: 17:00 IST.
    """

    pipeline_name = "india_vix"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "india_vix_execute_start",
            business_date=business_date.isoformat(),
        )

        async with httpx.AsyncClient() as client:
            try:
                vix_value = await _fetch_vix_value(client)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "india_vix_http_error",
                    status_code=exc.response.status_code,
                    url=str(exc.request.url),
                )
                raise

        if vix_value is None:
            logger.warning(
                "india_vix_not_found",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=1)

        await upsert_vix_value(session, business_date, vix_value)

        logger.info(
            "india_vix_upserted",
            vix_value=str(vix_value),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(rows_processed=1, rows_failed=0)
