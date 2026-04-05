"""FII/DII flows pipeline — primary NSE API with SEBI CSV fallback."""

from __future__ import annotations


import csv
import io
from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.flows import DeInstitutionalFlows
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
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

# SEBI publishes daily FII/DII data as CSV
SEBI_FII_DII_CSV_URL = (
    "https://www.sebi.gov.in/sebiweb/other/OtherAction.do"
    "?doRecognisedFpi=yes&intmId=14"
)

# Market type mapping from NSE API field names
MARKET_TYPE_MAP: dict[str, str] = {
    "equity": "equity",
    "debt": "debt",
    "hybrid": "hybrid",
    "derivatives": "derivatives",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A"):
            return None
        return Decimal(cleaned)
    except Exception:
        return None


def _parse_nse_response(
    data: list[dict[str, Any]],
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse NSE fiidiiTradeReact response into DB row dicts.

    NSE returns a list of records with keys: category, buyValue, sellValue, netValue, type.
    """
    rows: list[dict[str, Any]] = []

    for record in data:
        category = str(record.get("category", "")).strip().upper()
        market_type_raw = str(record.get("type", "equity")).strip().lower()

        # Normalize market type to enum values
        market_type = MARKET_TYPE_MAP.get(market_type_raw, "equity")

        if category not in ("FII", "DII"):
            continue

        gross_buy = _safe_decimal(record.get("buyValue"))
        gross_sell = _safe_decimal(record.get("sellValue"))

        if gross_buy is None and gross_sell is None:
            continue

        rows.append(
            {
                "date": business_date,
                "category": category,
                "market_type": market_type,
                "gross_buy": gross_buy,
                "gross_sell": gross_sell,
                "source": "NSE",
            }
        )

    return rows


def _parse_sebi_csv(
    content: str,
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse SEBI FII/DII CSV fallback data.

    SEBI CSV format (approximate): Date, Category, Market, Gross Buy, Gross Sell, Net
    This is a best-effort parser for the publicly available SEBI CSV.
    """
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(content))

    for row in reader:
        # Try to find date column
        row_date_str = (
            row.get("Date") or row.get("date") or row.get("Trade Date", "")
        ).strip()
        if not row_date_str:
            continue

        # Only process rows matching our business_date
        try:
            from datetime import datetime as dt

            for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%Y-%m-%d"):
                try:
                    parsed_date = dt.strptime(row_date_str, fmt).date()
                    break
                except ValueError:
                    continue
            else:
                continue
        except Exception:
            continue

        if parsed_date != business_date:
            continue

        category_raw = str(
            row.get("Category") or row.get("category") or row.get("Investor Type", "")
        ).strip().upper()

        if "FII" in category_raw or "FPI" in category_raw:
            category = "FII"
        elif "DII" in category_raw:
            category = "DII"
        else:
            continue

        market_raw = str(
            row.get("Market") or row.get("market") or row.get("Segment", "equity")
        ).strip().lower()
        market_type = MARKET_TYPE_MAP.get(market_raw, "equity")

        gross_buy = _safe_decimal(
            row.get("Gross Buy") or row.get("gross_buy") or row.get("Purchase", "")
        )
        gross_sell = _safe_decimal(
            row.get("Gross Sell") or row.get("gross_sell") or row.get("Sales", "")
        )

        if gross_buy is None and gross_sell is None:
            continue

        rows.append(
            {
                "date": business_date,
                "category": category,
                "market_type": market_type,
                "gross_buy": gross_buy,
                "gross_sell": gross_sell,
                "source": "SEBI",
            }
        )

    return rows


async def _fetch_from_nse(
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Fetch FII/DII data from NSE API.

    Requires a session cookie obtained by hitting the NSE homepage first.
    """
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(NSE_FII_DII_URL, headers=NSE_HEADERS, timeout=15.0)
    response.raise_for_status()
    return response.json()


async def _fetch_from_sebi(
    client: httpx.AsyncClient,
) -> str:
    """Fetch FII/DII CSV from SEBI as fallback."""
    response = await client.get(SEBI_FII_DII_CSV_URL, timeout=30.0)
    response.raise_for_status()
    return response.text


async def upsert_institutional_flows(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert institutional flow rows into de_institutional_flows.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeInstitutionalFlows).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "category", "market_type"],
        set_={
            "gross_buy": stmt.excluded.gross_buy,
            "gross_sell": stmt.excluded.gross_sell,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class FiiDiiFlowsPipeline(BasePipeline):
    """Fetches daily FII/DII equity, debt, and hybrid flows.

    Primary source: NSE fiidiiTradeReact API (requires session cookie).
    Fallback on 403/error: SEBI CSV download.
    Trigger: End of day (after 17:00 IST).
    SLA: 18:00 IST.
    """

    pipeline_name = "fii_dii_flows"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "fii_dii_execute_start",
            business_date=business_date.isoformat(),
        )

        rows: list[dict[str, Any]] = []
        source_used = "NSE"

        async with httpx.AsyncClient() as client:
            try:
                raw_data = await _fetch_from_nse(client)
                rows = _parse_nse_response(raw_data, business_date)
                logger.info(
                    "fii_dii_nse_success",
                    raw_records=len(raw_data),
                    parsed_rows=len(rows),
                    business_date=business_date.isoformat(),
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 403:
                    logger.warning(
                        "fii_dii_nse_403_falling_back_to_sebi",
                        business_date=business_date.isoformat(),
                    )
                    try:
                        csv_content = await _fetch_from_sebi(client)
                        rows = _parse_sebi_csv(csv_content, business_date)
                        source_used = "SEBI"
                        logger.info(
                            "fii_dii_sebi_fallback_success",
                            parsed_rows=len(rows),
                            business_date=business_date.isoformat(),
                        )
                    except Exception as sebi_exc:
                        logger.error(
                            "fii_dii_sebi_fallback_failed",
                            error=str(sebi_exc),
                            business_date=business_date.isoformat(),
                        )
                        raise sebi_exc
                else:
                    raise

        if not rows:
            logger.warning(
                "fii_dii_no_rows_parsed",
                business_date=business_date.isoformat(),
                source=source_used,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_institutional_flows(session, rows)

        logger.info(
            "fii_dii_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
