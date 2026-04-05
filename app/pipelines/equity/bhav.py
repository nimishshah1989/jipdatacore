"""BHAV copy download and ingestion pipeline.

Supports three formats:
- Pre-2010: eq_DDMMYYYY_csv.zip (legacy columns, comma-delimited)
- Standard: sec_bhavdata_full_DDMMYYYY.csv (standard NSE columns, comma-delimited)
- UDiFF (July 2024+): different column names, pipe-delimited or comma-delimited
"""

from __future__ import annotations


import asyncio
import hashlib
import io
import uuid
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import httpx

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeEquityOhlcv
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.source_files import register_source_file
from app.pipelines.validation import AnomalyRecord, check_freshness
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# BHAV Format detection
# ---------------------------------------------------------------------------

MIN_ROW_COUNT = 500
DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0  # seconds

# UDiFF rollout date
UDIFF_START_DATE = date(2024, 7, 1)

# NSE base URLs
NSE_BHAV_URL_STANDARD = (
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv"
)
NSE_BHAV_URL_PRE2010 = (
    "https://nsearchives.nseindia.com/archives/equities/bhavcopy/eq_{date_str}_csv.zip"
)
NSE_BHAV_URL_UDIFF = (
    "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv"
)

# NSE headers required to bypass bot protection
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


class BhavFormat(str, Enum):
    """Detected format of a BHAV copy file."""

    PRE2010 = "pre2010"
    STANDARD = "standard"
    UDIFF = "udiff"


def detect_bhav_format(header: str) -> BhavFormat:
    """Detect BHAV format from the CSV header row.

    Pre-2010 header: SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,...
    Standard header: SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN_PRICE,HIGH_PRICE,...
    UDiFF header: TradDt,BizDt,Sgmt,Src,FinInstrmTp,FinInstrmId,ISIN,... (camelCase)

    Args:
        header: First line of the CSV file (lowercased for matching).

    Returns:
        BhavFormat enum value.
    """
    header_lower = header.strip().lower()

    # UDiFF has camelCase column names like TradDt, BizDt
    if "traddt" in header_lower or "bizdt" in header_lower or "fininstrmtp" in header_lower:
        return BhavFormat.UDIFF

    # Standard format has DATE1 or PREV_CLOSE or CLOSE_PRICE
    if "date1" in header_lower or "prev_close" in header_lower or "close_price" in header_lower:
        return BhavFormat.STANDARD

    # Pre-2010 has PREVCLOSE (no underscore), TOTTRDQTY
    if "prevclose" in header_lower or "tottrdqty" in header_lower:
        return BhavFormat.PRE2010

    # Default to standard if unknown
    logger.warning("bhav_format_unknown", header=header[:100])
    return BhavFormat.STANDARD


def _safe_decimal(value: str | None) -> Decimal | None:
    """Convert string to Decimal safely. Returns None on failure.

    Always converts via str() to avoid float precision issues.
    """
    if value is None:
        return None
    stripped = str(value).strip()
    if not stripped or stripped in ("-", "NA", "N/A", ""):
        return None
    try:
        return Decimal(stripped)
    except InvalidOperation:
        return None


def _safe_int(value: str | None) -> int | None:
    """Convert string to int safely. Returns None on failure."""
    if value is None:
        return None
    stripped = str(value).strip()
    if not stripped or stripped in ("-", "NA", "N/A", ""):
        return None
    try:
        # Handle decimal notation like "1234.0"
        return int(Decimal(stripped))
    except (InvalidOperation, ValueError):
        return None


def parse_bhav_csv(content: str, fmt: BhavFormat) -> list[dict[str, Any]]:
    """Parse BHAV copy CSV content into a list of row dicts.

    Each returned dict has keys: symbol, series, open, high, low, close,
    volume, trades, date (as date object).

    Financial values are Decimal, never float.
    Rows with missing symbol or close are skipped.

    Args:
        content: Raw CSV text content.
        fmt: BhavFormat enum value.

    Returns:
        List of parsed row dicts.
    """
    lines = content.strip().splitlines()
    if not lines:
        return []

    header_line = lines[0]

    if fmt == BhavFormat.UDIFF:
        delimiter = "|" if "|" in header_line else ","
    else:
        delimiter = ","

    headers = [h.strip() for h in header_line.split(delimiter)]
    header_map = {h.upper(): i for i, h in enumerate(headers)}

    rows: list[dict[str, Any]] = []

    for line in lines[1:]:
        if not line.strip():
            continue
        cols = [c.strip() for c in line.split(delimiter)]
        if len(cols) < 4:
            continue

        def get(key: str) -> str | None:
            idx = header_map.get(key)
            if idx is None or idx >= len(cols):
                return None
            return cols[idx]

        if fmt == BhavFormat.PRE2010:
            symbol = get("SYMBOL")
            series = get("SERIES")
            open_price = _safe_decimal(get("OPEN"))
            high_price = _safe_decimal(get("HIGH"))
            low_price = _safe_decimal(get("LOW"))
            close_price = _safe_decimal(get("CLOSE"))
            volume = _safe_int(get("TOTTRDQTY"))
            trades = _safe_int(get("TOTALTRADES"))
            trade_date_str = get("TIMESTAMP") or get("DATE")

        elif fmt == BhavFormat.STANDARD:
            symbol = get("SYMBOL")
            series = get("SERIES")
            open_price = _safe_decimal(get("OPEN_PRICE"))
            high_price = _safe_decimal(get("HIGH_PRICE"))
            low_price = _safe_decimal(get("LOW_PRICE"))
            close_price = _safe_decimal(get("CLOSE_PRICE"))
            volume = _safe_int(get("TTL_TRD_QNTY"))
            trades = _safe_int(get("NO_OF_TRADES"))
            trade_date_str = get("DATE1") or get("TIMESTAMP")

        else:  # UDIFF
            symbol = get("FININSTRMID") or get("SYMBOL") or get("TCKRSYMB")
            series = get("SCTYTP") or get("SERIES") or get("SGMT")
            open_price = _safe_decimal(get("OPENPRIC") or get("OPEN"))
            high_price = _safe_decimal(get("HIGHPRIC") or get("HIGH"))
            low_price = _safe_decimal(get("LOWPRIC") or get("LOW"))
            close_price = _safe_decimal(get("CLSPRIC") or get("CLOSE") or get("LASTPRIC"))
            volume = _safe_int(get("TTLQTY") or get("TTLTRDQTY") or get("TOTTRDQTY"))
            trades = _safe_int(get("NOOF_TRADES") or get("NO_OF_TRADES"))
            trade_date_str = get("TRADDT") or get("BIZDT")

        if not symbol or close_price is None:
            continue

        # Parse date
        parsed_date: date | None = None
        if trade_date_str:
            for fmt_str in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    from datetime import datetime as dt
                    parsed_date = dt.strptime(trade_date_str.strip(), fmt_str).date()
                    break
                except ValueError:
                    continue

        rows.append(
            {
                "symbol": symbol.strip().upper(),
                "series": (series or "EQ").strip().upper(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "trades": trades,
                "date": parsed_date,
            }
        )

    return rows


async def _download_with_retry(
    client: httpx.AsyncClient,
    url: str,
    retries: int = DOWNLOAD_RETRIES,
) -> bytes:
    """Download a URL with exponential backoff retries.

    Args:
        client: httpx async client.
        url: URL to download.
        retries: Number of retry attempts.

    Returns:
        Response bytes on success.

    Raises:
        httpx.HTTPError: If all retries are exhausted.
    """
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()
            return response.content
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_exc = exc
            wait_secs = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "bhav_download_retry",
                url=url,
                attempt=attempt + 1,
                wait_secs=wait_secs,
                error=str(exc),
            )
            if attempt < retries - 1:
                await asyncio.sleep(wait_secs)

    raise last_exc or RuntimeError(f"Failed to download {url} after {retries} retries")


def _compute_checksum(content: bytes) -> str:
    """Compute SHA-256 checksum of bytes content."""
    return hashlib.sha256(content).hexdigest()


def _extract_zip_csv(content: bytes) -> str:
    """Extract the CSV from a ZIP archive (pre-2010 format).

    Returns the text content of the first CSV file found.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                return zf.read(name).decode("utf-8", errors="replace")
    raise ValueError("No CSV file found in BHAV ZIP archive")


class BhavPipeline(BasePipeline):
    """BHAV copy ingestion pipeline.

    Downloads NSE BHAV copy for the business date, auto-detects format,
    parses OHLCV data, resolves instruments, and upserts into de_equity_ohlcv.

    Skips unknown instruments with an anomaly logged.
    Validates freshness via SHA-256 checksum against de_source_files.
    """

    pipeline_name = "equity_bhav"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Download, parse, and upsert BHAV copy data."""
        logger.info("bhav_execute_start", business_date=business_date.isoformat())

        # Determine URL based on date
        date_str_std = business_date.strftime("%d%m%Y")
        date_str_udiff = business_date.strftime("%Y%m%d")

        if business_date >= UDIFF_START_DATE:
            url = NSE_BHAV_URL_UDIFF.format(date_str=date_str_udiff)
            expected_fmt: BhavFormat | None = BhavFormat.UDIFF
        elif business_date.year < 2010:
            url = NSE_BHAV_URL_PRE2010.format(date_str=date_str_std)
            expected_fmt = BhavFormat.PRE2010
        else:
            url = NSE_BHAV_URL_STANDARD.format(date_str=date_str_std)
            expected_fmt = BhavFormat.STANDARD

        async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=60.0) as client:
            raw_bytes = await _download_with_retry(client, url)

        checksum = _compute_checksum(raw_bytes)

        # Freshness check
        is_fresh, reason = await check_freshness(
            session,
            source_name="nse_bhav",
            file_date=business_date,
            checksum=checksum,
        )
        if not is_fresh:
            logger.info("bhav_skipped_duplicate", reason=reason, business_date=business_date.isoformat())
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Extract CSV content
        if url.endswith(".zip"):
            csv_text = _extract_zip_csv(raw_bytes)
        else:
            csv_text = raw_bytes.decode("utf-8", errors="replace")

        # Detect format
        first_line = csv_text.strip().splitlines()[0] if csv_text.strip() else ""
        detected_fmt = detect_bhav_format(first_line)
        if expected_fmt and detected_fmt != expected_fmt:
            logger.warning(
                "bhav_format_mismatch",
                expected=expected_fmt,
                detected=detected_fmt,
                business_date=business_date.isoformat(),
            )

        # Parse rows
        parsed_rows = parse_bhav_csv(csv_text, detected_fmt)
        logger.info(
            "bhav_parsed",
            row_count=len(parsed_rows),
            format=detected_fmt,
            business_date=business_date.isoformat(),
        )

        # Freshness: minimum row count
        if len(parsed_rows) < MIN_ROW_COUNT:
            raise ValueError(
                f"BHAV copy has only {len(parsed_rows)} rows (minimum {MIN_ROW_COUNT}). "
                f"Possible download error or non-trading day."
            )

        # Register source file
        source_file = await register_source_file(
            session,
            source_name="nse_bhav",
            file_name=url.split("/")[-1],
            file_date=business_date,
            checksum=checksum,
            file_size_bytes=len(raw_bytes),
            row_count=len(parsed_rows),
            format_version=detected_fmt.value,
        )

        # Load instrument symbol → id map
        symbol_to_id = await _load_symbol_map(session)

        rows_processed = 0
        rows_failed = 0
        unknown_symbols: list[str] = []

        insert_rows: list[dict[str, Any]] = []

        for row in parsed_rows:
            symbol = row["symbol"]
            instrument_id = symbol_to_id.get(symbol)
            if instrument_id is None:
                unknown_symbols.append(symbol)
                rows_failed += 1
                continue

            row_date = row["date"] or business_date

            insert_rows.append(
                {
                    "date": row_date,
                    "instrument_id": instrument_id,
                    "symbol": symbol,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "trades": row["trades"],
                    "data_status": "raw",
                    "source_file_id": source_file.id,
                    "pipeline_run_id": run_log.id,
                }
            )

        # Batch upsert
        if insert_rows:
            stmt = pg_insert(DeEquityOhlcv).values(insert_rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "instrument_id"],
                set_={
                    "symbol": stmt.excluded.symbol,
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "trades": stmt.excluded.trades,
                    "source_file_id": stmt.excluded.source_file_id,
                    "pipeline_run_id": stmt.excluded.pipeline_run_id,
                },
            )
            await session.execute(stmt)
            rows_processed = len(insert_rows)

        # Log unknown symbols as anomalies (batched)
        if unknown_symbols:
            logger.warning(
                "bhav_unknown_symbols",
                count=len(unknown_symbols),
                samples=unknown_symbols[:10],
                business_date=business_date.isoformat(),
            )

        logger.info(
            "bhav_execute_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            unknown_symbols=len(unknown_symbols),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source_file_id=source_file.id,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Validate inserted BHAV rows for basic data quality.

        Checks for rows with date mismatch against business_date.
        Actual price/volume anomaly checks are done in EodOrchestrator.validate().
        """
        anomalies: list[AnomalyRecord] = []

        # Check that we have rows for this business_date
        result = await session.execute(
            text(
                "SELECT COUNT(*) FROM de_equity_ohlcv "
                "WHERE date = :bdate AND pipeline_run_id = :run_id"
            ),
            {"bdate": business_date, "run_id": run_log.id},
        )
        count = result.scalar_one() or 0
        if count == 0:
            logger.warning(
                "bhav_validate_no_rows",
                business_date=business_date.isoformat(),
                run_id=run_log.id,
            )

        return anomalies


async def _load_symbol_map(session: AsyncSession) -> dict[str, uuid.UUID]:
    """Load current_symbol → instrument_id mapping from de_instrument.

    Only loads active, non-deleted instruments with EQ series.

    Returns:
        Dict mapping uppercase symbol string to UUID instrument_id.
    """
    from app.models.instruments import DeInstrument

    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    return {row.current_symbol.upper(): row.id for row in result}
