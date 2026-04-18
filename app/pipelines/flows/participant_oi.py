"""NSE Participant-wise Open Interest pipeline.

Downloads the daily fao_participant_oi_DDMMYYYY.csv from NSE archives and
upserts FII/DII/Pro/Client/TOTAL long/short contract counts in futures and
options.
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
from app.models.flows import DeParticipantOi
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_HOMEPAGE_URL = "https://www.nseindia.com/"
NSE_PARTICIPANT_OI_URL_TEMPLATE = (
    "https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv"
)

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

VALID_CLIENT_TYPES = {"Client", "DII", "FII", "Pro", "TOTAL"}

# CSV header -> model column mapping
COLUMN_MAP: dict[str, str] = {
    "Future Index Long": "future_index_long",
    "Future Index Short": "future_index_short",
    "Future Stock Long": "future_stock_long",
    "Future Stock Short": "future_stock_short",
    "Option Index Call Long": "option_index_call_long",
    "Option Index Put Long": "option_index_put_long",
    "Option Index Call Short": "option_index_call_short",
    "Option Index Put Short": "option_index_put_short",
    "Option Stock Call Long": "option_stock_call_long",
    "Option Stock Put Long": "option_stock_put_long",
    "Option Stock Call Short": "option_stock_call_short",
    "Option Stock Put Short": "option_stock_put_short",
    "Total Long Contracts": "total_long_contracts",
    "Total Short Contracts": "total_short_contracts",
}


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely, stripping commas/whitespace."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A"):
            return None
        # CSV sometimes contains decimals like "123.00"
        return int(float(cleaned))
    except Exception:
        return None


def _normalize_client_type(raw: str) -> str | None:
    """Normalize the raw Client Type cell to one of the allowed values."""
    cleaned = raw.strip()
    if not cleaned:
        return None
    # Case-insensitive match against the allowed set
    lowered = cleaned.lower()
    for allowed in VALID_CLIENT_TYPES:
        if allowed.lower() == lowered:
            return allowed
    return None


def _parse_participant_oi_csv(
    content: str,
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse the NSE participant OI CSV into DB row dicts.

    The CSV may contain a disclaimer/header row before the column headers,
    so we detect the header line by scanning for 'Client Type'.
    """
    # Detect the header line — NSE sometimes prefixes a title row
    lines = content.splitlines()
    header_idx = None
    for idx, line in enumerate(lines):
        if "Client Type" in line:
            header_idx = idx
            break

    if header_idx is None:
        return []

    csv_body = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(csv_body))

    rows: list[dict[str, Any]] = []
    for raw_row in reader:
        # Normalize header whitespace
        row = {(k or "").strip(): (v or "") for k, v in raw_row.items()}

        client_type_raw = row.get("Client Type", "")
        client_type = _normalize_client_type(client_type_raw)
        if client_type is None:
            continue

        record: dict[str, Any] = {
            "trade_date": business_date,
            "client_type": client_type,
        }

        for csv_col, model_col in COLUMN_MAP.items():
            record[model_col] = _safe_int(row.get(csv_col))

        rows.append(record)

    return rows


async def _fetch_participant_oi_csv(
    client: httpx.AsyncClient,
    business_date: date,
) -> str:
    """Fetch participant OI CSV from NSE archives.

    Requires a session cookie obtained by hitting the NSE homepage first.
    """
    ddmmyyyy = business_date.strftime("%d%m%Y")
    url = NSE_PARTICIPANT_OI_URL_TEMPLATE.format(ddmmyyyy=ddmmyyyy)

    # Cookie handshake
    await client.get(NSE_HOMEPAGE_URL, headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
    response.raise_for_status()
    return response.text


async def upsert_participant_oi(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert participant OI rows into de_participant_oi.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeParticipantOi).values(rows)
    update_cols = {col: getattr(stmt.excluded, col) for col in COLUMN_MAP.values()}
    stmt = stmt.on_conflict_do_update(
        index_elements=["trade_date", "client_type"],
        set_=update_cols,
    )
    await session.execute(stmt)
    return len(rows), 0


class ParticipantOiPipeline(BasePipeline):
    """Fetches daily NSE participant-wise open interest (FII/DII/Pro/Client/TOTAL).

    Source: nsearchives.nseindia.com CSV (requires cookie handshake).
    Trigger: End of day, after NSE publishes the participant OI file.
    """

    pipeline_name = "participant_oi"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "participant_oi_execute_start",
            business_date=business_date.isoformat(),
        )

        async with httpx.AsyncClient() as client:
            try:
                csv_content = await _fetch_participant_oi_csv(client, business_date)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "participant_oi_fetch_failed",
                    status_code=exc.response.status_code,
                    business_date=business_date.isoformat(),
                )
                raise
            except httpx.HTTPError as exc:
                logger.error(
                    "participant_oi_fetch_error",
                    error=str(exc),
                    business_date=business_date.isoformat(),
                )
                raise

        rows = _parse_participant_oi_csv(csv_content, business_date)

        logger.info(
            "participant_oi_parsed",
            parsed_rows=len(rows),
            business_date=business_date.isoformat(),
        )

        if not rows:
            logger.warning(
                "participant_oi_no_rows_parsed",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_participant_oi(session, rows)

        logger.info(
            "participant_oi_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
