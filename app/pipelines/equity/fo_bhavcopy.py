"""NSE F&O UDiFF Bhavcopy pipeline — daily futures & options contract data.

Primary source: nsearchives.nseindia.com new UDiFF archive ZIP (2024+).
Fallback source: legacy nseindia.com reports API for F&O UDiFF Common Bhavcopy Final.
"""

from __future__ import annotations


import csv
import io
import json
import urllib.parse
import zipfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeFoBhavcopy
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

# Primary URL (new UDiFF format, 2024+)
NSE_FO_UDIFF_URL = (
    "https://nsearchives.nseindia.com/content/fo/"
    "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

# Fallback URL — legacy reports API with an `archives` JSON query parameter
NSE_FO_REPORTS_URL = "https://www.nseindia.com/api/reports"
NSE_FO_REPORTS_ARCHIVE_DESC = {
    "name": "F&O - UDiFF Common Bhavcopy Final (zip)",
    "type": "archives",
    "category": "derivatives",
    "section": "equity",
}

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

# Map CSV FinInstrmTp codes -> normalised instrument_type stored in the DB.
# NSE uses:
#   IDF = Index Future, STF = Stock Future,
#   IDO = Index Option, STO = Stock Option
_INSTRUMENT_TYPE_MAP: dict[str, str] = {
    "IDF": "FUTIDX",
    "STF": "FUTSTK",
    "IDO": "OPTIDX",
    "STO": "OPTSTK",
    "FUTIDX": "FUTIDX",
    "FUTSTK": "FUTSTK",
    "OPTIDX": "OPTIDX",
    "OPTSTK": "OPTSTK",
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely. Returns None on failure/blank."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "nan", "NaN"):
            return None
        return Decimal(cleaned)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely. Returns None on failure/blank."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "nan", "NaN"):
            return None
        # Route via Decimal to accept scientific notation / decimals
        return int(Decimal(cleaned))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _parse_date(value: Any) -> date | None:
    """Parse a date from the UDiFF CSV. NSE uses YYYY-MM-DD in UDiFF output."""
    if value is None:
        return None
    raw = str(value).strip()
    if raw in ("", "-", "N/A"):
        return None
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _extract_csv_from_zip(raw_bytes: bytes) -> str:
    """Extract the single CSV file bundled inside the UDiFF ZIP."""
    with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
        csv_name = next(
            (n for n in zf.namelist() if n.lower().endswith(".csv")),
            None,
        )
        if csv_name is None:
            raise ValueError("No CSV file found inside F&O bhavcopy zip archive")
        return zf.read(csv_name).decode("utf-8", errors="replace")


def _parse_udiff_csv(
    content: str,
    business_date: date,
) -> list[dict[str, Any]]:
    """Parse the UDiFF F&O CSV into DB row dicts.

    Expected columns (UDiFF schema): TradDt, BizDt, Sgmt, Src, FinInstrmTp,
    FinInstrmId, ISIN, TckrSymb, SctySrs, XpryDt, FininstrmActlXpryDt, StrkPric,
    OptnTp, FinInstrmNm, OpnPric, HghPric, LwPric, ClsPric, LastPric,
    PrvsClsgPric, UndrlygPric, SttlmPric, OpnIntrst, ChngInOpnIntrst,
    TtlTradgVol, TtlTrfVal, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks, ...
    """
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(content))

    for record in reader:
        # Normalise keys (strip whitespace on headers)
        rec = {
            (k.strip() if isinstance(k, str) else k): (v.strip() if isinstance(v, str) else v)
            for k, v in record.items()
            if k is not None
        }

        fin_instrm_tp = str(rec.get("FinInstrmTp", "")).strip().upper()
        instrument_type = _INSTRUMENT_TYPE_MAP.get(fin_instrm_tp)
        if instrument_type is None:
            # Not a F&O derivative row we care about — skip silently
            continue

        symbol = str(rec.get("TckrSymb", "")).strip()
        if not symbol:
            continue

        trade_dt = _parse_date(rec.get("TradDt")) or business_date
        expiry_dt = _parse_date(rec.get("XpryDt"))
        if expiry_dt is None:
            # Every F&O contract must have an expiry — skip malformed row
            continue

        option_type_raw = str(rec.get("OptnTp", "")).strip().upper()
        if instrument_type in ("OPTIDX", "OPTSTK"):
            option_type = option_type_raw if option_type_raw in ("CE", "PE") else "--"
            strike_price = _safe_decimal(rec.get("StrkPric")) or Decimal("0")
        else:
            # Futures: no option type, no strike
            option_type = "--"
            strike_price = Decimal("0")

        rows.append(
            {
                "trade_date": trade_dt,
                "symbol": symbol,
                "instrument_type": instrument_type,
                "expiry_date": expiry_dt,
                "strike_price": strike_price,
                "option_type": option_type,
                "open": _safe_decimal(rec.get("OpnPric")),
                "high": _safe_decimal(rec.get("HghPric")),
                "low": _safe_decimal(rec.get("LwPric")),
                "close": _safe_decimal(rec.get("ClsPric")),
                "settle_price": _safe_decimal(rec.get("SttlmPric")),
                "prev_close": _safe_decimal(rec.get("PrvsClsgPric")),
                "underlying_price": _safe_decimal(rec.get("UndrlygPric")),
                "open_interest": _safe_int(rec.get("OpnIntrst")),
                "change_in_oi": _safe_int(rec.get("ChngInOpnIntrst")),
                "contracts_traded": _safe_int(rec.get("TtlTradgVol")),
                "turnover_lakh": _safe_decimal(rec.get("TtlTrfVal")),
                "num_trades": _safe_int(rec.get("TtlNbOfTxsExctd")),
                "source": "NSE",
            }
        )

    return rows


def _build_primary_url(business_date: date) -> str:
    """Build the primary nsearchives URL for the given trade date."""
    return NSE_FO_UDIFF_URL.format(yyyymmdd=business_date.strftime("%Y%m%d"))


def _build_reports_fallback_url(business_date: date) -> str:
    """Build the legacy reports-API fallback URL."""
    archives_param = json.dumps([NSE_FO_REPORTS_ARCHIVE_DESC], separators=(",", ":"))
    params = {
        "archives": archives_param,
        "date": business_date.strftime("%d-%b-%Y"),
        "type": "equity",
        "mode": "single",
    }
    return f"{NSE_FO_REPORTS_URL}?{urllib.parse.urlencode(params)}"


async def _warm_nse_cookies(client: httpx.AsyncClient) -> None:
    """Hit the NSE homepage to obtain the session cookies required for archives."""
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=15.0)


async def _fetch_primary(
    client: httpx.AsyncClient,
    business_date: date,
) -> bytes:
    """Download the new UDiFF ZIP with one retry on 403."""
    url = _build_primary_url(business_date)
    await _warm_nse_cookies(client)
    try:
        response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
        response.raise_for_status()
        return response.content
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            logger.warning(
                "fo_bhavcopy_primary_403_retry",
                url=url,
                business_date=business_date.isoformat(),
            )
            # Re-warm cookies and retry once
            await _warm_nse_cookies(client)
            response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
            response.raise_for_status()
            return response.content
        raise


async def _fetch_fallback(
    client: httpx.AsyncClient,
    business_date: date,
) -> bytes:
    """Download via the legacy reports API fallback with one retry on 403."""
    url = _build_reports_fallback_url(business_date)
    await _warm_nse_cookies(client)
    try:
        response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
        response.raise_for_status()
        return response.content
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 403:
            logger.warning(
                "fo_bhavcopy_fallback_403_retry",
                url=url,
                business_date=business_date.isoformat(),
            )
            await _warm_nse_cookies(client)
            response = await client.get(url, headers=NSE_HEADERS, timeout=30.0)
            response.raise_for_status()
            return response.content
        raise


async def upsert_fo_bhavcopy(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Bulk upsert F&O bhavcopy rows. Returns (rows_processed, rows_failed)."""
    if not rows:
        return 0, 0

    stmt = pg_insert(DeFoBhavcopy).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            "trade_date",
            "symbol",
            "instrument_type",
            "expiry_date",
            "strike_price",
            "option_type",
        ],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "settle_price": stmt.excluded.settle_price,
            "prev_close": stmt.excluded.prev_close,
            "underlying_price": stmt.excluded.underlying_price,
            "open_interest": stmt.excluded.open_interest,
            "change_in_oi": stmt.excluded.change_in_oi,
            "contracts_traded": stmt.excluded.contracts_traded,
            "turnover_lakh": stmt.excluded.turnover_lakh,
            "num_trades": stmt.excluded.num_trades,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class FoBhavcopyPipeline(BasePipeline):
    """NSE F&O UDiFF Bhavcopy ingestion pipeline.

    Primary source: nsearchives.nseindia.com UDiFF ZIP (new 2024 format).
    Fallback: legacy nseindia.com reports API archive download.
    NSE requires a warm session cookie (GET homepage first with browser UA).
    Idempotent via ON CONFLICT on the natural key.
    Trigger: End of day, after NSE publishes the F&O bhavcopy (~18:00 IST).
    """

    pipeline_name = "fo_bhavcopy"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "fo_bhavcopy_execute_start",
            business_date=business_date.isoformat(),
        )

        raw_bytes: bytes | None = None
        source_used = "NSE_ARCHIVES"

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                raw_bytes = await _fetch_primary(client, business_date)
                logger.info(
                    "fo_bhavcopy_primary_success",
                    bytes=len(raw_bytes),
                    business_date=business_date.isoformat(),
                )
            except (httpx.HTTPStatusError, httpx.HTTPError) as exc:
                logger.warning(
                    "fo_bhavcopy_primary_failed_trying_fallback",
                    error=str(exc),
                    business_date=business_date.isoformat(),
                )
                try:
                    raw_bytes = await _fetch_fallback(client, business_date)
                    source_used = "NSE_REPORTS_API"
                    logger.info(
                        "fo_bhavcopy_fallback_success",
                        bytes=len(raw_bytes),
                        business_date=business_date.isoformat(),
                    )
                except Exception as fallback_exc:
                    logger.error(
                        "fo_bhavcopy_fallback_failed",
                        error=str(fallback_exc),
                        business_date=business_date.isoformat(),
                    )
                    raise fallback_exc

        if not raw_bytes:
            logger.warning(
                "fo_bhavcopy_empty_download",
                business_date=business_date.isoformat(),
                source=source_used,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Extract CSV from ZIP (both primary and fallback ship a ZIP)
        try:
            csv_content = _extract_csv_from_zip(raw_bytes)
        except (zipfile.BadZipFile, ValueError) as exc:
            # If the response was a raw CSV (rare), decode directly
            logger.warning(
                "fo_bhavcopy_not_a_zip_falling_back_to_raw_csv",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            csv_content = raw_bytes.decode("utf-8", errors="replace")

        rows = _parse_udiff_csv(csv_content, business_date)

        if not rows:
            logger.warning(
                "fo_bhavcopy_no_rows_parsed",
                business_date=business_date.isoformat(),
                source=source_used,
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_fo_bhavcopy(session, rows)

        logger.info(
            "fo_bhavcopy_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
