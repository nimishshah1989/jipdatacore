"""SEBI PIT Regulation 7 insider trading disclosures pipeline.

Aggregates Reg 7 disclosures from two exchanges:
  - NSE: /api/corporates-pit (JSON; cookie handshake required)
  - BSE: /BseIndiaAPI/api/AnnSubCategoryGetData/w (JSON; browser UA)

Reg 7 requires promoters, directors, KMP and their immediate relatives
to disclose transactions in listed securities to the exchange within
2 trading days of the trade. Disclosures are immutable once filed, so
we use ON CONFLICT DO NOTHING on the natural key.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.qualitative import DeInsiderTrade
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_HOME_URL = "https://www.nseindia.com/"
NSE_PIT_URL = "https://www.nseindia.com/api/corporates-pit"
BSE_ANN_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
)

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

NSE_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
}

BSE_HEADERS = {
    "User-Agent": BROWSER_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely, handling Indian-formatted strings."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return Decimal(cleaned)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely, handling Indian-formatted strings."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return int(float(cleaned))
    except Exception:
        return None


def _parse_date(value: Any, fmts: tuple[str, ...]) -> date | None:
    """Parse a date string against a set of formats."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    # strip time component if any
    raw = raw.split("T")[0].split(" ")[0]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _classify_txn_type(anex: Any, sec_acq_qty: Any, remarks: Any) -> str:
    """Infer transaction_type from NSE 'anex'/'acqMode' fields and remarks.

    NSE uses 'anex' (acquisition/disposal indicator) plus remarks that often
    contain the word 'pledge' or 'invocation'. Defaults: positive quantity
    → BUY, negative → SELL.
    """
    text = " ".join(
        str(x or "").lower() for x in (anex, remarks)
    )
    if "invocation" in text:
        return "INVOCATION"
    if "pledge" in text or "encumbrance" in text:
        return "PLEDGE"
    if "dispos" in text or "sell" in text or "sale" in text:
        return "SELL"
    if "acqui" in text or "buy" in text or "purchase" in text:
        return "BUY"
    qty = _safe_int(sec_acq_qty)
    if qty is not None and qty < 0:
        return "SELL"
    return "BUY"


def _parse_nse_records(
    payload: dict[str, Any],
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse NSE corporates-pit JSON into row dicts."""
    data = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(data, list):
        return []

    rows: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            continue

        symbol = str(rec.get("symbol") or "").strip().upper()
        person_name = str(rec.get("acqName") or "").strip()
        if not symbol or not person_name:
            continue

        qty = _safe_int(rec.get("secAcq"))
        if qty is None:
            continue
        # store absolute quantity; direction captured in transaction_type
        quantity = abs(qty)

        txn_type = _classify_txn_type(
            rec.get("anex") or rec.get("acqMode"),
            qty,
            rec.get("remarks"),
        )

        disclosure_date = (
            _parse_date(rec.get("date"), ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"))
            or business_date
        )
        transaction_date = _parse_date(
            rec.get("acqfromDt") or rec.get("tdpDate"),
            ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"),
        )

        rows.append(
            {
                "disclosure_date": disclosure_date,
                "transaction_date": transaction_date,
                "symbol": symbol,
                "company_name": (rec.get("company") or "").strip() or None,
                "person_name": person_name,
                "person_category": (rec.get("personCategory") or "").strip() or None,
                "transaction_type": txn_type,
                "quantity": quantity,
                "value_inr": _safe_decimal(rec.get("secVal")),
                "pre_holding_pct": _safe_decimal(rec.get("tkdPct")),
                "post_holding_pct": _safe_decimal(rec.get("afterAcqSharesPer")),
                "exchange": "NSE",
                "source": "NSE",
                "raw_payload": rec,
            }
        )
    return rows


def _parse_bse_records(
    payload: dict[str, Any],
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse BSE AnnSubCategoryGetData JSON into row dicts.

    BSE returns announcement metadata, not fully structured trade fields;
    we capture what's available and keep the full payload in raw_payload for
    downstream parsing/enrichment.
    """
    if not isinstance(payload, dict):
        return []
    # BSE wraps rows under "Table" (common) or returns a list directly
    table = payload.get("Table") or payload.get("data") or []
    if not isinstance(table, list):
        return []

    rows: list[dict[str, Any]] = []
    for rec in table:
        if not isinstance(rec, dict):
            continue

        symbol = str(
            rec.get("SCRIP_CD") or rec.get("scrip_cd") or rec.get("SLONGNAME") or ""
        ).strip().upper()
        person_name = str(
            rec.get("ACQUIRER_NAME")
            or rec.get("NAME_OF_ACQUIRER")
            or rec.get("HEADLINE")
            or ""
        ).strip()
        if not symbol or not person_name:
            continue

        qty = _safe_int(
            rec.get("NO_OF_SHARES_ACQ")
            or rec.get("QTY")
            or rec.get("NO_OF_SECURITIES")
        )
        if qty is None:
            continue
        quantity = abs(qty)

        txn_type = _classify_txn_type(
            rec.get("ACQUISITION_MODE") or rec.get("ANNEXURE"),
            qty,
            rec.get("HEADLINE") or rec.get("REMARKS"),
        )

        disclosure_date = (
            _parse_date(
                rec.get("NEWS_DT") or rec.get("DT_TM") or rec.get("DissemDT"),
                ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"),
            )
            or business_date
        )
        transaction_date = _parse_date(
            rec.get("ACQUISITION_DT") or rec.get("TRANS_DT"),
            ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d-%m-%Y"),
        )

        rows.append(
            {
                "disclosure_date": disclosure_date,
                "transaction_date": transaction_date,
                "symbol": symbol,
                "company_name": (rec.get("SLONGNAME") or rec.get("COMPANY_NAME") or "").strip() or None,
                "person_name": person_name,
                "person_category": (rec.get("CATEGORY_OF_PERSON") or "").strip() or None,
                "transaction_type": txn_type,
                "quantity": quantity,
                "value_inr": _safe_decimal(rec.get("VALUE") or rec.get("TRANS_VALUE")),
                "pre_holding_pct": _safe_decimal(rec.get("BEF_HOLDING_PER")),
                "post_holding_pct": _safe_decimal(rec.get("AFT_HOLDING_PER")),
                "exchange": "BSE",
                "source": "BSE",
                "raw_payload": rec,
            }
        )
    return rows


async def _fetch_from_nse(
    client: httpx.AsyncClient,
    business_date: date,
) -> dict[str, Any]:
    """Fetch NSE Reg 7 disclosures for business_date.

    Requires a session cookie obtained by hitting the NSE homepage first.
    """
    date_str = business_date.strftime("%d-%m-%Y")
    await client.get(NSE_HOME_URL, headers=NSE_HEADERS, timeout=15.0)
    params = {
        "index": "equities",
        "from_date": date_str,
        "to_date": date_str,
    }
    response = await client.get(
        NSE_PIT_URL, headers=NSE_HEADERS, params=params, timeout=20.0
    )
    response.raise_for_status()
    return response.json()


async def _fetch_from_bse(
    client: httpx.AsyncClient,
    business_date: date,
) -> dict[str, Any]:
    """Fetch BSE insider trading announcements for business_date."""
    date_str = business_date.strftime("%Y%m%d")
    params = {
        "pageno": "1",
        "strCat": "Insider Trading Disclosure",
        "strPrevDate": date_str,
        "strScrip": "",
        "strSearch": "",
        "strToDate": date_str,
        "strType": "C",
    }
    response = await client.get(
        BSE_ANN_URL, headers=BSE_HEADERS, params=params, timeout=20.0
    )
    response.raise_for_status()
    return response.json()


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedupe on (symbol, disclosure_date, person_name, transaction_type, quantity).

    When the same disclosure lands on both NSE and BSE, prefer NSE (more
    structured fields) and drop the BSE duplicate to keep the row count clean.
    """
    seen: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row["symbol"],
            row["disclosure_date"],
            row["person_name"],
            row["transaction_type"],
            row["quantity"],
        )
        existing = seen.get(key)
        if existing is None:
            seen[key] = row
            continue
        # prefer NSE over BSE
        if existing["exchange"] == "BSE" and row["exchange"] == "NSE":
            seen[key] = row
    return list(seen.values())


async def upsert_insider_trades(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert insider trade rows with ON CONFLICT DO NOTHING (immutable).

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeInsiderTrade).values(rows)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_insider_trades_natural_key",
    )
    await session.execute(stmt)
    return len(rows), 0


class InsiderTradesPipeline(BasePipeline):
    """Fetches daily SEBI PIT Reg 7 insider trading disclosures.

    Sources (aggregated): NSE corporates-pit API + BSE announcement API.
    Trigger: End of day (after 18:00 IST, giving exchanges time to publish).
    SLA: 19:30 IST.
    """

    pipeline_name = "insider_trades"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "insider_trades_execute_start",
            business_date=business_date.isoformat(),
        )

        nse_rows: list[dict[str, Any]] = []
        bse_rows: list[dict[str, Any]] = []

        async with httpx.AsyncClient(follow_redirects=True) as client:
            # NSE (cookie handshake)
            try:
                nse_payload = await _fetch_from_nse(client, business_date)
                nse_rows = _parse_nse_records(nse_payload, business_date)
                logger.info(
                    "insider_trades_nse_success",
                    parsed_rows=len(nse_rows),
                    business_date=business_date.isoformat(),
                )
            except Exception as exc:
                logger.warning(
                    "insider_trades_nse_failed",
                    error=str(exc),
                    business_date=business_date.isoformat(),
                )

            # BSE (independent; known-flaky, so don't fail the whole run)
            try:
                bse_payload = await _fetch_from_bse(client, business_date)
                bse_rows = _parse_bse_records(bse_payload, business_date)
                logger.info(
                    "insider_trades_bse_success",
                    parsed_rows=len(bse_rows),
                    business_date=business_date.isoformat(),
                )
            except Exception as exc:
                logger.warning(
                    "insider_trades_bse_failed",
                    error=str(exc),
                    business_date=business_date.isoformat(),
                )

        combined = _dedupe_rows(nse_rows + bse_rows)

        if not combined:
            logger.warning(
                "insider_trades_no_rows_parsed",
                business_date=business_date.isoformat(),
                nse_raw=len(nse_rows),
                bse_raw=len(bse_rows),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_insider_trades(session, combined)

        logger.info(
            "insider_trades_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            nse_raw=len(nse_rows),
            bse_raw=len(bse_rows),
            deduped=len(combined),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
