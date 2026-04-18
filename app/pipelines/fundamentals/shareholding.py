"""BSE/NSE shareholding pattern pipeline — quarterly Regulation 31 disclosures.

Primary source: NSE corporate-shareholdings-master API (JSON, cookie handshake).
Fallback: BSE CorpCategoryData (Shareholding+Pattern category, returns XBRL URLs).

Only top-level category percentages are ingested in v1 — full XBRL parsing is
deferred because BSE XBRL documents are notoriously inconsistent across filers.
Filings typically appear 21 days after quarter end; on days with zero new
filings the pipeline exits with rows_processed=0 (SUCCESS, not failure).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.holdings import DeShareholdingPattern
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_HOME_URL = "https://www.nseindia.com/"
NSE_SHAREHOLDING_URL = (
    "https://www.nseindia.com/api/corporate-shareholdings-master"
    "?index=equities&from_date={from_date}&to_date={to_date}"
)
BSE_SHAREHOLDING_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/CorpCategoryData/w"
    "?scripcode=&strCategory=Shareholding+Pattern"
    "&strPrevDate={from_date}&strToDate={to_date}"
)

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern",
}

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely. Returns None on any parse failure."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").replace("%", "").strip()
        if cleaned in ("", "-", "N/A", "NA", "null", "None"):
            return None
        return Decimal(cleaned)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return int(float(cleaned))
    except Exception:
        return None


def _parse_quarter_end(value: Any) -> date | None:
    """Parse a quarter-end date from NSE/BSE responses. Expects DD-MMM-YYYY,
    DD-MM-YYYY, or YYYY-MM-DD style strings."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_nse_response(
    data: list[dict[str, Any]] | dict[str, Any],
) -> list[dict[str, Any]]:
    """Parse NSE corporate-shareholdings-master response into DB row dicts.

    NSE returns a list (sometimes wrapped in {"data": [...]}) of filings with
    keys like: symbol, companyName, date, xbrl, promoter, public, ... The
    exact schema is undocumented — we extract known category % fields and
    preserve the full record in raw_payload for downstream reprocessing.
    """
    if isinstance(data, dict):
        records = data.get("data") or data.get("rows") or []
    else:
        records = data or []

    rows: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        symbol = str(record.get("symbol") or record.get("Symbol") or "").strip().upper()
        if not symbol:
            continue

        as_of_date = _parse_quarter_end(
            record.get("date")
            or record.get("asOnDate")
            or record.get("broadcastDate")
            or record.get("qtrDate")
        )
        if as_of_date is None:
            continue

        filing_url = (
            record.get("xbrl")
            or record.get("xbrlFile")
            or record.get("attachmentFile")
            or None
        )

        rows.append(
            {
                "symbol": symbol,
                "as_of_date": as_of_date,
                "promoter_pct": _safe_decimal(
                    record.get("promoter") or record.get("promoterPct")
                ),
                "promoter_pledged_pct": _safe_decimal(
                    record.get("promoterPledged")
                    or record.get("pledgedPct")
                ),
                "public_pct": _safe_decimal(
                    record.get("public") or record.get("publicPct")
                ),
                "fii_pct": _safe_decimal(
                    record.get("fii") or record.get("fiiPct")
                ),
                "dii_pct": _safe_decimal(
                    record.get("dii") or record.get("diiPct")
                ),
                "mf_pct": _safe_decimal(record.get("mf") or record.get("mfPct")),
                "insurance_pct": _safe_decimal(record.get("insurance")),
                "banks_fi_pct": _safe_decimal(
                    record.get("banksFi") or record.get("banks")
                ),
                "retail_pct": _safe_decimal(record.get("retail")),
                "hni_pct": _safe_decimal(record.get("hni")),
                "other_pct": _safe_decimal(record.get("other")),
                "total_shares": _safe_int(
                    record.get("totalShares") or record.get("totalSharesOutstanding")
                ),
                "exchange": "NSE",
                "source": "NSE",
                "filing_url": str(filing_url)[:500] if filing_url else None,
                "raw_payload": record,
            }
        )

    return rows


def _parse_bse_response(
    data: list[dict[str, Any]] | dict[str, Any],
) -> list[dict[str, Any]]:
    """Parse BSE CorpCategoryData (Shareholding Pattern) response.

    BSE response shape: {"Table": [{scripcode, scrip_name, HEADLINE, NEWS_DT,
    ATTACHMENTNAME, QUARTER_END, ...}]}. The API only returns the filing
    metadata + XBRL URL — category percentages require XBRL parsing (deferred
    in v1). Rows are recorded with filing_url populated; pct columns NULL.
    """
    if isinstance(data, dict):
        records = data.get("Table") or data.get("data") or []
    else:
        records = data or []

    rows: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        symbol = str(
            record.get("scrip_name")
            or record.get("SLONGNAME")
            or record.get("scripcode")
            or ""
        ).strip().upper()
        if not symbol:
            continue

        as_of_date = _parse_quarter_end(
            record.get("QUARTER_END")
            or record.get("quarter_end")
            or record.get("NEWS_DT")
        )
        if as_of_date is None:
            continue

        attachment = record.get("ATTACHMENTNAME") or record.get("attachmentFile")
        filing_url = None
        if attachment:
            filing_url = (
                f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attachment}"
            )

        rows.append(
            {
                "symbol": symbol,
                "as_of_date": as_of_date,
                "promoter_pct": None,
                "promoter_pledged_pct": None,
                "public_pct": None,
                "fii_pct": None,
                "dii_pct": None,
                "mf_pct": None,
                "insurance_pct": None,
                "banks_fi_pct": None,
                "retail_pct": None,
                "hni_pct": None,
                "other_pct": None,
                "total_shares": None,
                "exchange": "BSE",
                "source": "BSE",
                "filing_url": str(filing_url)[:500] if filing_url else None,
                "raw_payload": record,
            }
        )

    return rows


async def _fetch_from_nse(
    client: httpx.AsyncClient,
    business_date: date,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Fetch shareholding filings from NSE for business_date (single-day window).

    NSE expects DD-MM-YYYY. Cookie handshake required via homepage hit.
    """
    date_str = business_date.strftime("%d-%m-%Y")
    url = NSE_SHAREHOLDING_URL.format(from_date=date_str, to_date=date_str)

    await client.get(NSE_HOME_URL, headers=NSE_HEADERS, timeout=15.0)
    response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
    response.raise_for_status()
    return response.json()


async def _fetch_from_bse(
    client: httpx.AsyncClient,
    business_date: date,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Fetch shareholding filings from BSE for business_date (YYYYMMDD format)."""
    date_str = business_date.strftime("%Y%m%d")
    url = BSE_SHAREHOLDING_URL.format(from_date=date_str, to_date=date_str)

    response = await client.get(url, headers=BSE_HEADERS, timeout=30.0)
    response.raise_for_status()
    return response.json()


async def upsert_shareholding(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert shareholding rows into de_shareholding_pattern.

    ON CONFLICT (symbol, as_of_date) DO UPDATE — filings can be revised.
    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeShareholdingPattern).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "as_of_date"],
        set_={
            "promoter_pct": stmt.excluded.promoter_pct,
            "promoter_pledged_pct": stmt.excluded.promoter_pledged_pct,
            "public_pct": stmt.excluded.public_pct,
            "fii_pct": stmt.excluded.fii_pct,
            "dii_pct": stmt.excluded.dii_pct,
            "mf_pct": stmt.excluded.mf_pct,
            "insurance_pct": stmt.excluded.insurance_pct,
            "banks_fi_pct": stmt.excluded.banks_fi_pct,
            "retail_pct": stmt.excluded.retail_pct,
            "hni_pct": stmt.excluded.hni_pct,
            "other_pct": stmt.excluded.other_pct,
            "total_shares": stmt.excluded.total_shares,
            "exchange": stmt.excluded.exchange,
            "source": stmt.excluded.source,
            "filing_url": stmt.excluded.filing_url,
            "raw_payload": stmt.excluded.raw_payload,
            "updated_at": datetime.utcnow(),
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class ShareholdingPatternPipeline(BasePipeline):
    """Fetches daily-published quarterly shareholding pattern filings.

    Primary: NSE corporate-shareholdings-master (JSON + cookie handshake).
    Fallback: BSE CorpCategoryData (returns XBRL metadata only; pct columns NULL).

    Trigger: Daily. Filings cluster 21-45 days post quarter-end.
    SLA: None hard — zero filings on a given day is SUCCESS, not failure.
    """

    pipeline_name = "shareholding_pattern"
    requires_trading_day = False
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "shareholding_execute_start",
            business_date=business_date.isoformat(),
        )

        rows: list[dict[str, Any]] = []
        source_used = "NSE"

        async with httpx.AsyncClient() as client:
            try:
                raw_data = await _fetch_from_nse(client, business_date)
                rows = _parse_nse_response(raw_data)
                logger.info(
                    "shareholding_nse_success",
                    parsed_rows=len(rows),
                    business_date=business_date.isoformat(),
                )
            except Exception as nse_exc:
                logger.warning(
                    "shareholding_nse_failed_falling_back_to_bse",
                    error=str(nse_exc),
                    business_date=business_date.isoformat(),
                )
                try:
                    raw_data = await _fetch_from_bse(client, business_date)
                    rows = _parse_bse_response(raw_data)
                    source_used = "BSE"
                    logger.info(
                        "shareholding_bse_fallback_success",
                        parsed_rows=len(rows),
                        business_date=business_date.isoformat(),
                    )
                except Exception as bse_exc:
                    logger.error(
                        "shareholding_bse_fallback_failed",
                        error=str(bse_exc),
                        business_date=business_date.isoformat(),
                    )
                    raise bse_exc

        if not rows:
            # Zero filings on a given day is expected (filings cluster near
            # quarter-end + 21 days). Return SUCCESS with rows_processed=0.
            logger.info(
                "shareholding_no_filings_today",
                business_date=business_date.isoformat(),
                source=source_used,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_shareholding(session, rows)

        logger.info(
            "shareholding_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
