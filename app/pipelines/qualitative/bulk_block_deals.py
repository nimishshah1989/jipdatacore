"""Bulk and block deals pipeline — NSE + BSE aggregated feed.

Four source feeds are fetched and merged into a single table:
  - NSE bulk deals (cookie handshake required)
  - NSE block deals (cookie handshake required)
  - BSE bulk deals (flag=B)
  - BSE block deals (flag=K)

Each feed is fetched independently; a 403 or transport error on any single
feed is logged and skipped so that the pipeline still succeeds when at least
one feed returns data. Deal rows are treated as immutable: on conflict with
the natural-key unique constraint we DO NOTHING.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.qualitative import DeBulkBlockDeal
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.utils.fetch_helpers import fetch_nse_json

logger = get_logger(__name__)


NSE_BULK_URL = "https://www.nseindia.com/api/historical/cm/bulk"
NSE_BLOCK_URL = "https://www.nseindia.com/api/historical/cm/block"
BSE_DEALS_URL = "https://api.bseindia.com/BseIndiaAPI/api/BulkDeals_Daily/w"

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

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal, tolerating commas, whitespace, dashes."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int, tolerating commas, whitespace, dashes."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        # handle values like "12345.00"
        return int(Decimal(cleaned))
    except (InvalidOperation, ValueError):
        return None


def _parse_nse_date(value: Any) -> date | None:
    """NSE returns dates like '15-Apr-2026' or 'DD-MMM-YYYY'."""
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_bse_date(value: Any) -> date | None:
    """BSE returns dates as ISO-ish strings or 'YYYY-MM-DDTHH:MM:SS'."""
    if not value:
        return None
    s = str(value).strip()
    # Trim possible time component
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[: len(fmt) + 2], fmt).date()
        except ValueError:
            continue
    return None


def _normalize_txn(value: Any) -> str | None:
    """Map buy/sell indicators from NSE/BSE feeds to 'BUY'/'SELL'."""
    if value is None:
        return None
    s = str(value).strip().upper()
    if s in ("B", "BUY", "P", "PURCHASE"):
        return "BUY"
    if s in ("S", "SELL", "SALE"):
        return "SELL"
    if "BUY" in s or "PURCH" in s:
        return "BUY"
    if "SELL" in s or "SALE" in s:
        return "SELL"
    return None


def _parse_nse_records(
    data: Any, deal_type: str
) -> list[dict[str, Any]]:
    """Parse NSE bulk/block JSON response.

    Response shape: {"data": [ {BD_DT_DATE, BD_SYMBOL, BD_CLIENT_NAME,
    BD_BUY_SELL, BD_QTY_TRD, BD_TP_WATP, BD_SCRIP_NAME}, ... ]}
    """
    rows: list[dict[str, Any]] = []
    records = data.get("data") if isinstance(data, dict) else data
    if not isinstance(records, list):
        return rows

    for rec in records:
        deal_date = _parse_nse_date(rec.get("BD_DT_DATE") or rec.get("date"))
        symbol = (rec.get("BD_SYMBOL") or rec.get("symbol") or "").strip()
        client_name = (
            rec.get("BD_CLIENT_NAME") or rec.get("clientName") or ""
        ).strip()
        txn = _normalize_txn(rec.get("BD_BUY_SELL") or rec.get("buySell"))
        quantity = _safe_int(rec.get("BD_QTY_TRD") or rec.get("quantityTraded"))
        price = _safe_decimal(
            rec.get("BD_TP_WATP")
            or rec.get("BD_DEAL_PRICE")
            or rec.get("tradePrice")
        )
        company_name = (
            rec.get("BD_SCRIP_NAME") or rec.get("scripName") or None
        )

        if not (deal_date and symbol and client_name and txn and quantity):
            continue

        rows.append(
            {
                "deal_date": deal_date,
                "symbol": symbol[:60],
                "company_name": (company_name or "").strip()[:255] or None,
                "client_name": client_name[:255],
                "deal_type": deal_type,
                "transaction_type": txn,
                "quantity": quantity,
                "traded_price": price,
                "exchange": "NSE",
                "source": "NSE",
            }
        )

    return rows


def _parse_bse_records(
    data: Any, deal_type: str
) -> list[dict[str, Any]]:
    """Parse BSE bulk/block JSON response.

    BSE response is typically {"Table": [ { ... } ]} with keys such as
    DealDate, SCRIP_CD, SCRIP_NAME, CLIENT_NAME, DEAL_TYPE (B/S), QTY_SHARES,
    TRADE_PRICE_WGTAVG. Field names vary; we try common aliases.
    """
    rows: list[dict[str, Any]] = []
    if isinstance(data, dict):
        records = data.get("Table") or data.get("data") or []
    else:
        records = data if isinstance(data, list) else []

    for rec in records:
        deal_date = _parse_bse_date(
            rec.get("DealDate") or rec.get("DEAL_DATE") or rec.get("Date")
        )
        symbol = str(
            rec.get("SCRIP_CD")
            or rec.get("SCRIP_CODE")
            or rec.get("Scrip_Code")
            or rec.get("scripCode")
            or ""
        ).strip()
        company_name = (
            rec.get("SCRIP_NAME")
            or rec.get("Scrip_Name")
            or rec.get("scripName")
            or None
        )
        client_name = (
            rec.get("CLIENT_NAME")
            or rec.get("Client_Name")
            or rec.get("clientName")
            or ""
        ).strip()
        txn = _normalize_txn(
            rec.get("DEAL_TYPE")
            or rec.get("Deal_Type")
            or rec.get("dealType")
            or rec.get("BUY_SELL")
        )
        quantity = _safe_int(
            rec.get("QTY_SHARES")
            or rec.get("Qty_Shares")
            or rec.get("quantity")
        )
        price = _safe_decimal(
            rec.get("TRADE_PRICE_WGTAVG")
            or rec.get("Trade_Price_Wgtavg")
            or rec.get("tradePrice")
            or rec.get("TradePrice")
        )

        if not (deal_date and symbol and client_name and txn and quantity):
            continue

        rows.append(
            {
                "deal_date": deal_date,
                "symbol": symbol[:60],
                "company_name": (company_name or "").strip()[:255] or None,
                "client_name": client_name[:255],
                "deal_type": deal_type,
                "transaction_type": txn,
                "quantity": quantity,
                "traded_price": price,
                "exchange": "BSE",
                "source": "BSE",
            }
        )

    return rows


async def _fetch_nse_feed(
    url: str,
    business_date: date,
    deal_type: str,
) -> list[dict[str, Any]]:
    """Fetch an NSE historical bulk/block feed using the shared fetch_nse_json
    utility (handles cookie warmup + retry internally).
    """
    dstr = business_date.strftime("%d-%m-%Y")
    full_url = f"{url}?from={dstr}&to={dstr}"
    data = await fetch_nse_json(full_url, max_retries=3, timeout=30.0)
    return _parse_nse_records(data, deal_type)


async def _fetch_bse_feed(
    client: httpx.AsyncClient,
    flag: str,
    business_date: date,
    deal_type: str,
) -> list[dict[str, Any]]:
    """Fetch a BSE bulk (flag=B) or block (flag=K) feed."""
    dstr = business_date.strftime("%Y%m%d")
    params = {
        "Mkttype": "M",
        "flag": flag,
        "FrmDt": dstr,
        "ToDt": dstr,
    }
    response = await client.get(
        BSE_DEALS_URL, params=params, headers=BSE_HEADERS, timeout=30.0
    )
    response.raise_for_status()
    return _parse_bse_records(response.json(), deal_type)


async def upsert_bulk_block_deals(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert deal rows with ON CONFLICT DO NOTHING.

    Returns (rows_processed, rows_failed). Deals are immutable historical
    records — we never overwrite existing rows.
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeBulkBlockDeal).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_bulk_block_deal",
    )
    await session.execute(stmt)
    return len(rows), 0


class BulkBlockDealsPipeline(BasePipeline):
    """Aggregates NSE + BSE bulk and block deals into de_bulk_block_deals.

    Four source feeds are fetched independently per run. Any feed returning
    403 (anti-bot) or a transport error is skipped with a warning — the
    pipeline still succeeds if at least one feed yields rows.
    """

    pipeline_name = "bulk_block_deals"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "bulk_block_deals_execute_start",
            business_date=business_date.isoformat(),
        )

        all_rows: list[dict[str, Any]] = []
        feed_results: dict[str, int] = {}
        feed_failures: list[str] = []

        async with httpx.AsyncClient(follow_redirects=True) as client:
            feeds: list[tuple[str, Any]] = [
                ("nse_bulk", _fetch_nse_feed(NSE_BULK_URL, business_date, "BULK")),
                ("nse_block", _fetch_nse_feed(NSE_BLOCK_URL, business_date, "BLOCK")),
                ("bse_bulk", _fetch_bse_feed(client, "B", business_date, "BULK")),
                ("bse_block", _fetch_bse_feed(client, "K", business_date, "BLOCK")),
            ]

            for name, coro in feeds:
                try:
                    feed_rows = await coro
                    all_rows.extend(feed_rows)
                    feed_results[name] = len(feed_rows)
                    logger.info(
                        "bulk_block_deals_feed_success",
                        feed=name,
                        rows=len(feed_rows),
                        business_date=business_date.isoformat(),
                    )
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status in (403, 401, 429):
                        logger.warning(
                            "bulk_block_deals_feed_blocked",
                            feed=name,
                            status=status,
                            business_date=business_date.isoformat(),
                        )
                        feed_failures.append(name)
                    else:
                        logger.error(
                            "bulk_block_deals_feed_http_error",
                            feed=name,
                            status=status,
                            business_date=business_date.isoformat(),
                        )
                        feed_failures.append(name)
                except Exception as exc:
                    logger.error(
                        "bulk_block_deals_feed_error",
                        feed=name,
                        error=str(exc),
                        business_date=business_date.isoformat(),
                    )
                    feed_failures.append(name)

        total_feeds = 4
        succeeded = total_feeds - len(feed_failures)
        logger.info(
            "bulk_block_deals_feeds_summary",
            feed_counts=feed_results,
            failed_feeds=feed_failures,
            succeeded=succeeded,
            business_date=business_date.isoformat(),
        )

        # Graceful-fail when all feeds 403/timeout — NSE + BSE anti-bot
        # can blanket-block an EC2 IP. Return 0 rows so backfill progresses
        # across all business days without log-spamming errors.
        if succeeded == 0:
            logger.warning(
                "bulk_block_deals_all_feeds_failed",
                failed_feeds=feed_failures,
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        if not all_rows:
            logger.warning(
                "bulk_block_deals_no_rows_parsed",
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_bulk_block_deals(
            session, all_rows
        )

        logger.info(
            "bulk_block_deals_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            feed_counts=feed_results,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
