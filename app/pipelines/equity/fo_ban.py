"""NSE F&O Securities-in-Ban list pipeline.

Primary source: https://nsearchives.nseindia.com/content/fo/fo_secban.csv
Fallback:      https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES+IN+F%26O

Daily list of stocks breaching market-wide position limits (MWPL). The CSV is
overwritten each trading day and contains a single SYMBOL column. On days with
no bans the list is empty — this is treated as a successful run with zero rows.
"""

from __future__ import annotations


import csv
import io
from datetime import date
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeFoBanList
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_FO_SECBAN_CSV_URL = "https://nsearchives.nseindia.com/content/fo/fo_secban.csv"
NSE_FO_STOCKINDICES_URL = (
    "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES+IN+F%26O"
)
NSE_HOMEPAGE = "https://www.nseindia.com/"

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


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely, returning None on failure."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A"):
            return None
        return int(float(cleaned))
    except Exception:
        return None


def _parse_secban_csv(content: str, business_date: date) -> list[dict[str, Any]]:
    """Parse the fo_secban.csv file into DB row dicts.

    The CSV has a single SYMBOL column (and possibly a leading Sr.No column).
    On no-ban days the file may contain only a header or be empty.
    """
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(content))

    # Normalise header names (NSE occasionally ships "SYMBOL" or "Symbol",
    # and sometimes a leading Sr No column).
    fieldnames = [(fn or "").strip().upper() for fn in (reader.fieldnames or [])]

    symbol_key: str | None = None
    for original, upper in zip(reader.fieldnames or [], fieldnames):
        if upper == "SYMBOL":
            symbol_key = original
            break

    if symbol_key is None:
        # Headerless file — attempt naive line-per-symbol parse
        for line in content.splitlines():
            symbol = line.strip().strip('"').upper()
            if not symbol or symbol in ("SYMBOL", "SR. NO.", "SR NO"):
                continue
            # skip lines that look like serial-number,symbol pairs
            if "," in symbol:
                parts = [p.strip().strip('"') for p in symbol.split(",")]
                symbol = parts[-1].upper()
            if symbol:
                rows.append(
                    {
                        "business_date": business_date,
                        "symbol": symbol,
                        "ban_count": None,
                        "source": "NSE_CSV",
                    }
                )
        return rows

    for record in reader:
        raw = record.get(symbol_key)
        if not raw:
            continue
        symbol = str(raw).strip().upper()
        if not symbol or symbol == "SYMBOL":
            continue
        rows.append(
            {
                "business_date": business_date,
                "symbol": symbol,
                "ban_count": None,
                "source": "NSE_CSV",
            }
        )

    return rows


def _parse_stockindices_response(
    data: dict[str, Any],
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse NSE equity-stockIndices JSON for ban_count > 0 entries."""
    rows: list[dict[str, Any]] = []

    for record in data.get("data", []) or []:
        meta = record.get("meta") or {}
        symbol_raw = meta.get("symbol") or record.get("symbol")
        if not symbol_raw:
            continue
        symbol = str(symbol_raw).strip().upper()
        if not symbol or symbol.upper().startswith("NIFTY"):
            # skip index rows; only individual F&O securities
            continue

        ban_count = _safe_int(meta.get("ban_count"))
        if ban_count is None or ban_count <= 0:
            continue

        rows.append(
            {
                "business_date": business_date,
                "symbol": symbol,
                "ban_count": ban_count,
                "source": "NSE_API",
            }
        )

    return rows


async def _fetch_secban_csv(client: httpx.AsyncClient) -> str:
    """Download the daily fo_secban.csv from NSE archives."""
    response = await client.get(
        NSE_FO_SECBAN_CSV_URL, headers=NSE_HEADERS, timeout=30.0
    )
    response.raise_for_status()
    return response.text


async def _fetch_stockindices(client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch F&O universe from NSE equity-stockIndices API.

    Requires a session cookie obtained by hitting the NSE homepage first.
    """
    await client.get(NSE_HOMEPAGE, headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(
        NSE_FO_STOCKINDICES_URL, headers=NSE_HEADERS, timeout=20.0
    )
    response.raise_for_status()
    return response.json()


async def upsert_fo_ban_list(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert ban-list rows into de_fo_ban_list.

    ON CONFLICT (business_date, symbol) DO UPDATE ban_count, source.
    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeFoBanList).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["business_date", "symbol"],
        set_={
            "ban_count": stmt.excluded.ban_count,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class FoBanListPipeline(BasePipeline):
    """Fetches the NSE daily F&O Securities-in-Ban list.

    Primary source: fo_secban.csv published on nsearchives.nseindia.com.
    Fallback: equity-stockIndices API (SECURITIES IN F&O index) where each
    security carries a meta.ban_count — rows with ban_count > 0 are in ban.

    An empty ban list (no securities in ban) is a legitimate outcome; the
    pipeline still reports success with rows_processed=0.

    Trigger: End of day, after NSE publishes next-day ban list (~19:00 IST).
    """

    pipeline_name = "fo_ban_list"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "fo_ban_list_execute_start",
            business_date=business_date.isoformat(),
        )

        rows: list[dict[str, Any]] = []
        source_used = "NSE_CSV"

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                csv_content = await _fetch_secban_csv(client)
                rows = _parse_secban_csv(csv_content, business_date)
                logger.info(
                    "fo_ban_list_csv_success",
                    parsed_rows=len(rows),
                    business_date=business_date.isoformat(),
                )
            except Exception as csv_exc:
                logger.warning(
                    "fo_ban_list_csv_failed_falling_back_to_api",
                    error=str(csv_exc),
                    business_date=business_date.isoformat(),
                )
                try:
                    raw_data = await _fetch_stockindices(client)
                    rows = _parse_stockindices_response(raw_data, business_date)
                    source_used = "NSE_API"
                    logger.info(
                        "fo_ban_list_api_fallback_success",
                        parsed_rows=len(rows),
                        business_date=business_date.isoformat(),
                    )
                except Exception as api_exc:
                    logger.error(
                        "fo_ban_list_api_fallback_failed",
                        error=str(api_exc),
                        business_date=business_date.isoformat(),
                    )
                    raise api_exc

        if not rows:
            # Empty ban list is a valid success outcome — NSE publishes an
            # empty / header-only CSV on days with no MWPL breaches.
            logger.info(
                "fo_ban_list_empty_ban_day",
                business_date=business_date.isoformat(),
                source=source_used,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_fo_ban_list(session, rows)

        logger.info(
            "fo_ban_list_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
