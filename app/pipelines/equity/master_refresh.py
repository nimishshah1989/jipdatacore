"""NSE equity master refresh pipeline.

Fetches the current NSE equity listing, inserts new instruments,
handles symbol changes, and marks suspensions/delistings.

Must run BEFORE price ingestion (bhav.py) each trading day.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import httpx

from app.logging import get_logger
from app.models.instruments import DeInstrument, DeSymbolHistory
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

# NSE equity listing URL — returns JSON with securities list
NSE_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

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

DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0


def parse_equity_listing_csv(content: str) -> list[dict[str, Any]]:
    """Parse NSE EQUITY_L.csv into a list of instrument dicts.

    Expected columns: SYMBOL,NAME OF COMPANY,SERIES,DATE OF LISTING,
    PAID UP VALUE,MARKET LOT,ISIN NUMBER,FACE VALUE

    Args:
        content: Raw CSV text.

    Returns:
        List of dicts with keys: symbol, company_name, series, isin,
        listing_date, face_value.
    """
    lines = content.strip().splitlines()
    if not lines:
        return []

    # Parse header
    headers = [h.strip().upper() for h in lines[0].split(",")]
    header_map = {h: i for i, h in enumerate(headers)}

    def get(cols: list[str], key: str) -> str | None:
        idx = header_map.get(key)
        if idx is None or idx >= len(cols):
            return None
        return cols[idx].strip() or None

    instruments: list[dict[str, Any]] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        cols = [c.strip() for c in line.split(",")]
        if len(cols) < 3:
            continue

        symbol = get(cols, "SYMBOL")
        if not symbol:
            continue

        company_name = get(cols, "NAME OF COMPANY")
        series = get(cols, "SERIES") or "EQ"
        isin = get(cols, "ISIN NUMBER")

        listing_date: date | None = None
        date_str = get(cols, "DATE OF LISTING")
        if date_str:
            from datetime import datetime
            for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    listing_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    continue

        instruments.append(
            {
                "symbol": symbol.upper(),
                "company_name": company_name,
                "series": series,
                "isin": isin,
                "listing_date": listing_date,
                "exchange": "NSE",
                "is_active": True,
                "is_tradeable": True,
            }
        )

    return instruments


async def _download_equity_listing(client: httpx.AsyncClient) -> str:
    """Download NSE equity listing CSV with retry.

    Args:
        client: httpx async client.

    Returns:
        Raw CSV text content.
    """
    last_exc: Exception | None = None
    for attempt in range(DOWNLOAD_RETRIES):
        try:
            response = await client.get(NSE_EQUITY_LIST_URL, follow_redirects=True)
            response.raise_for_status()
            return response.text
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_exc = exc
            wait_secs = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "master_refresh_download_retry",
                attempt=attempt + 1,
                wait_secs=wait_secs,
                error=str(exc),
            )
            if attempt < DOWNLOAD_RETRIES - 1:
                await asyncio.sleep(wait_secs)

    raise last_exc or RuntimeError("Failed to download NSE equity listing")


class MasterRefreshPipeline(BasePipeline):
    """NSE equity master refresh pipeline.

    Inserts new instruments, handles symbol changes, marks suspensions/delistings.
    Runs daily before price ingestion.
    """

    pipeline_name = "equity_master_refresh"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch NSE listing and sync instruments table."""
        logger.info("master_refresh_execute_start", business_date=business_date.isoformat())

        async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0) as client:
            csv_text = await _download_equity_listing(client)

        listing = parse_equity_listing_csv(csv_text)
        logger.info(
            "master_refresh_parsed",
            count=len(listing),
            business_date=business_date.isoformat(),
        )

        if not listing:
            raise ValueError("NSE equity listing returned 0 instruments — aborting")

        # Load existing instruments (symbol → id + company_name) for comparison
        existing_map = await _load_existing_instruments(session)

        new_count = 0
        updated_count = 0
        symbol_change_count = 0
        rows_failed = 0

        for item in listing:
            symbol = item["symbol"]
            try:
                if symbol not in existing_map:
                    # New instrument — insert
                    await _insert_instrument(session, item)
                    new_count += 1
                else:
                    # Existing instrument — check for company name update
                    existing_id, existing_symbol = existing_map[symbol]
                    if existing_symbol != symbol:
                        # Symbol changed — update and record history
                        await _handle_symbol_change(
                            session,
                            instrument_id=existing_id,
                            old_symbol=existing_symbol,
                            new_symbol=symbol,
                            effective_date=business_date,
                        )
                        symbol_change_count += 1
                    updated_count += 1
            except Exception as exc:
                logger.error(
                    "master_refresh_row_error",
                    symbol=symbol,
                    error=str(exc),
                )
                rows_failed += 1

        rows_processed = new_count + updated_count + symbol_change_count

        logger.info(
            "master_refresh_execute_complete",
            new_instruments=new_count,
            updated=updated_count,
            symbol_changes=symbol_change_count,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
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
        """No anomaly detection for master refresh — structural data only."""
        return []


async def _load_existing_instruments(
    session: AsyncSession,
) -> dict[str, tuple[object, str]]:
    """Load existing instruments as symbol → (id, current_symbol) map.

    Returns:
        Dict mapping uppercase symbol to (instrument_id, current_symbol).
    """
    from app.models.instruments import DeInstrument

    result = await session.execute(
        select(DeInstrument.id, DeInstrument.current_symbol).where(
            DeInstrument.exchange == "NSE"
        )
    )
    return {row.current_symbol.upper(): (row.id, row.current_symbol) for row in result}


async def _insert_instrument(
    session: AsyncSession,
    item: dict[str, Any],
) -> None:
    """Insert a new instrument with ON CONFLICT DO NOTHING.

    Args:
        session: Async DB session.
        item: Parsed instrument dict.
    """
    stmt = pg_insert(DeInstrument).values(
        current_symbol=item["symbol"],
        company_name=item.get("company_name"),
        exchange=item.get("exchange", "NSE"),
        series=item.get("series", "EQ"),
        isin=item.get("isin"),
        listing_date=item.get("listing_date"),
        is_active=True,
        is_tradeable=True,
        is_suspended=False,
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["current_symbol"])
    await session.execute(stmt)


async def _handle_symbol_change(
    session: AsyncSession,
    instrument_id: object,
    old_symbol: str,
    new_symbol: str,
    effective_date: date,
) -> None:
    """Update current_symbol on instrument and insert a DeSymbolHistory record.

    Args:
        session: Async DB session.
        instrument_id: UUID of the instrument.
        old_symbol: Previous symbol string.
        new_symbol: New symbol string.
        effective_date: Date the change takes effect.
    """
    # Update current symbol
    await session.execute(
        update(DeInstrument)
        .where(DeInstrument.id == instrument_id)
        .values(current_symbol=new_symbol)
    )

    # Record history
    stmt = pg_insert(DeSymbolHistory).values(
        instrument_id=instrument_id,
        effective_date=effective_date,
        old_symbol=old_symbol,
        new_symbol=new_symbol,
        reason="NSE listing refresh",
    )
    # ON CONFLICT DO NOTHING — same symbol change on same date is idempotent
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["instrument_id", "effective_date"]
    )
    await session.execute(stmt)

    logger.info(
        "symbol_change_recorded",
        instrument_id=str(instrument_id),
        old_symbol=old_symbol,
        new_symbol=new_symbol,
        effective_date=effective_date.isoformat(),
    )


async def handle_suspension(
    session: AsyncSession,
    symbol: str,
    suspended_from: date,
) -> bool:
    """Mark an instrument as suspended.

    Args:
        session: Async DB session.
        symbol: NSE symbol.
        suspended_from: Date suspension takes effect.

    Returns:
        True if instrument found and updated, False if not found.
    """
    result = await session.execute(
        select(DeInstrument.id).where(
            DeInstrument.current_symbol == symbol.upper()
        )
    )
    row = result.first()
    if row is None:
        logger.warning("suspension_instrument_not_found", symbol=symbol)
        return False

    await session.execute(
        update(DeInstrument)
        .where(DeInstrument.current_symbol == symbol.upper())
        .values(is_suspended=True, suspended_from=suspended_from, is_tradeable=False)
    )
    logger.info(
        "instrument_suspended",
        symbol=symbol,
        suspended_from=suspended_from.isoformat(),
    )
    return True


async def handle_delisting(
    session: AsyncSession,
    symbol: str,
    delisted_on: date,
) -> bool:
    """Mark an instrument as delisted (inactive).

    Args:
        session: Async DB session.
        symbol: NSE symbol.
        delisted_on: Date of delisting.

    Returns:
        True if instrument found and updated, False if not found.
    """
    result = await session.execute(
        select(DeInstrument.id).where(
            DeInstrument.current_symbol == symbol.upper()
        )
    )
    row = result.first()
    if row is None:
        logger.warning("delisting_instrument_not_found", symbol=symbol)
        return False

    await session.execute(
        update(DeInstrument)
        .where(DeInstrument.current_symbol == symbol.upper())
        .values(is_active=False, is_tradeable=False, delisted_on=delisted_on)
    )
    logger.info(
        "instrument_delisted",
        symbol=symbol,
        delisted_on=delisted_on.isoformat(),
    )
    return True
