"""BHAV Copy ingestion pipeline — NSE daily equity OHLCV data."""

from __future__ import annotations


import io
import zipfile
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeEquityOhlcv
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.source_files import register_source_file
from app.pipelines.validation import AnomalyRecord, check_freshness
from app.utils.fetch_helpers import NSE_ARCHIVES_HEADERS, fetch_with_retry
from app.utils.symbol_resolver import bulk_resolve_symbols, _clear_symbol_cache

logger = get_logger(__name__)

# Minimum rows expected in a valid BHAV copy file
BHAV_MIN_ROWS = 500

# NSE BHAV copy URL patterns
_BHAV_URL_PRE2010 = (
    "https://archives.nseindia.com/content/historical/EQUITIES"
    "/{yyyy}/{mmm}/eq_{dd}{mm}{yyyy}_csv.zip"
)
_BHAV_URL_STANDARD = (
    "https://archives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv"
)
_BHAV_URL_UDIFF = (
    "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv"
)

# Column mappings per format version
_COL_MAP_PRE2010 = {
    "SYMBOL": "symbol",
    "SERIES": "series",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
    "LAST": "last",
    "PREVCLOSE": "prev_close",
    "TOTTRDQTY": "volume",
    "TOTTRDVAL": "trd_val",
    "TIMESTAMP": "trade_date",
    "TOTALTRADES": "trades",
    "ISIN": "isin",
}

_COL_MAP_STANDARD = {
    "SYMBOL": "symbol",
    "SERIES": "series",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
    "LAST": "last",
    "PREVCLOSE": "prev_close",
    "TOTTRDQTY": "volume",
    "TOTTRDVAL": "trd_val",
    "TIMESTAMP": "trade_date",
    "TOTALTRADES": "trades",
    "ISIN": "isin",
}

_COL_MAP_UDIFF = {
    "TradDt": "trade_date",
    "TckrSymb": "symbol",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "LastPric": "last",
    "PrvsClsgPric": "prev_close",
    "TtlTradgVol": "volume",
    "TtlTrfVal": "trd_val",
    "NbOfTxs": "trades",
    "ISIN": "isin",
}


def detect_bhav_format(header_row: str, business_date: date) -> str:
    """Detect BHAV copy format from the CSV header row.

    Returns one of: "pre2010", "standard", "udiff"
    """
    # UDiFF format has camelCase headers
    if "TckrSymb" in header_row or "TradDt" in header_row or "ClsPric" in header_row:
        return "udiff"

    # Pre-2010 and standard both have uppercase SYMBOL/CLOSE
    # Distinguish by date: pre-2010 format was used before 2010
    if business_date.year < 2010:
        return "pre2010"

    return "standard"


def build_bhav_url(business_date: date) -> tuple[str, str]:
    """Build the NSE BHAV copy URL for a given business date.

    Returns (url, expected_format) where format is "pre2010", "standard", or "udiff".
    """
    dd = business_date.strftime("%d")
    mm = business_date.strftime("%m")
    yyyy = business_date.strftime("%Y")
    mmm = business_date.strftime("%b").upper()  # e.g. JAN, FEB

    # July 2024+ uses UDiFF format (new archive URL)
    if business_date >= date(2024, 7, 1):
        url = _BHAV_URL_UDIFF.format(dd=dd, mm=mm, yyyy=yyyy)
        return url, "udiff"

    # Pre-2010 uses zip format
    if business_date.year < 2010:
        url = _BHAV_URL_PRE2010.format(dd=dd, mm=mm, yyyy=yyyy, mmm=mmm)
        return url, "pre2010"

    # Standard format 2010 – June 2024
    url = _BHAV_URL_STANDARD.format(dd=dd, mm=mm, yyyy=yyyy)
    return url, "standard"


def parse_bhav_csv(
    raw_bytes: bytes,
    expected_format: str,
    business_date: date,
) -> pd.DataFrame:
    """Parse raw BHAV copy bytes into a normalized DataFrame.

    Handles zip extraction for pre-2010 format.
    Auto-detects actual format from header row regardless of expected_format.

    Returns DataFrame with normalized lowercase column names.
    """
    # Extract from zip if needed
    if expected_format == "pre2010" or raw_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            csv_name = next(
                (n for n in zf.namelist() if n.lower().endswith(".csv")),
                None,
            )
            if csv_name is None:
                raise ValueError("No CSV file found inside BHAV zip archive")
            raw_bytes = zf.read(csv_name)

    # Decode
    text = raw_bytes.decode("utf-8", errors="replace")
    first_line = text.split("\n")[0].strip()

    # Detect actual format from header
    actual_format = detect_bhav_format(first_line, business_date)

    # Parse CSV
    df = pd.read_csv(
        io.StringIO(text),
        low_memory=False,
    )

    # Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    # Select column mapping
    if actual_format == "udiff":
        col_map = _COL_MAP_UDIFF
    elif actual_format == "pre2010":
        col_map = _COL_MAP_PRE2010
    else:
        col_map = _COL_MAP_STANDARD

    # Rename only columns that exist
    rename_map = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Normalize trade_date
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="mixed", errors="coerce").dt.date
    else:
        df["trade_date"] = business_date

    # Filter to EQ series only (main equity series)
    if "series" in df.columns:
        df = df[df["series"].str.strip() == "EQ"].copy()

    # Strip symbol whitespace
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].str.strip()

    logger.info(
        "bhav_parsed",
        format=actual_format,
        rows=len(df),
        business_date=business_date.isoformat(),
    )
    return df


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely. Returns None on failure."""
    if pd.isna(value):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    """Convert a value to int safely. Returns None on failure."""
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class BhavCopyPipeline(BasePipeline):
    """NSE BHAV Copy ingestion pipeline.

    Downloads and ingests the daily equity OHLCV BHAV copy from NSE archives.
    Supports three format variants: pre-2010, standard (2010–June 2024), and UDiFF (July 2024+).
    """

    pipeline_name = "equity_bhav_copy"
    requires_trading_day = True
    exchange = "NSE"

    def __init__(self, http_client: Any = None) -> None:
        """Initialize with optional injected HTTP client for testing."""
        self._http_client = http_client

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Download, parse, and ingest BHAV copy data for business_date."""
        # Clear symbol cache at the start of each execute to force fresh lookup
        _clear_symbol_cache()

        # Build URL and expected format
        url, expected_format = build_bhav_url(business_date)

        logger.info(
            "bhav_download_start",
            url=url,
            expected_format=expected_format,
            business_date=business_date.isoformat(),
        )

        # Download with retry
        raw_bytes = await fetch_with_retry(
            url,
            headers=NSE_ARCHIVES_HEADERS,
            max_retries=3,
            client=self._http_client,
        )

        # Compute checksum from raw bytes
        import hashlib
        checksum = hashlib.sha256(raw_bytes).hexdigest()

        # Freshness check — skip if already ingested
        is_fresh, reason = await check_freshness(
            session,
            source_name="nse_bhav_copy",
            file_date=business_date,
            checksum=checksum,
        )
        if not is_fresh:
            logger.info(
                "bhav_skipped_duplicate",
                reason=reason,
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        # Parse CSV
        df = parse_bhav_csv(raw_bytes, expected_format, business_date)

        # Freshness validation: minimum row count
        if len(df) < BHAV_MIN_ROWS:
            logger.warning(
                "bhav_low_row_count",
                row_count=len(df),
                min_expected=BHAV_MIN_ROWS,
                business_date=business_date.isoformat(),
            )
            # Don't fail — flag as anomaly but continue with what we have
            # The validate() method handles anomaly detection

        # Register source file
        source_file = await register_source_file(
            session,
            source_name="nse_bhav_copy",
            file_name=url.split("/")[-1],
            file_date=business_date,
            checksum=checksum,
            file_size_bytes=len(raw_bytes),
            row_count=len(df),
            format_version=expected_format,
        )

        # Bulk resolve symbols
        symbols = df["symbol"].dropna().unique().tolist()
        symbol_map = await bulk_resolve_symbols(symbols, session)

        # Build rows for upsert
        rows_to_insert = []
        rows_failed = 0

        for _, row in df.iterrows():
            symbol = row.get("symbol")
            if not symbol or pd.isna(symbol):
                rows_failed += 1
                continue

            instrument_id = symbol_map.get(str(symbol).strip())
            if instrument_id is None:
                logger.warning(
                    "bhav_unknown_symbol_skipped",
                    symbol=symbol,
                    business_date=business_date.isoformat(),
                )
                rows_failed += 1
                continue

            trade_date = row.get("trade_date", business_date)
            if not isinstance(trade_date, date):
                trade_date = business_date

            open_price = _safe_decimal(row.get("open"))
            high_price = _safe_decimal(row.get("high"))
            low_price = _safe_decimal(row.get("low"))
            close_price = _safe_decimal(row.get("close"))
            volume = _safe_int(row.get("volume"))
            trades = _safe_int(row.get("trades"))

            rows_to_insert.append({
                "date": trade_date,
                "instrument_id": instrument_id,
                "symbol": str(symbol).strip(),  # Immutable historical snapshot
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "trades": trades,
                "data_status": "raw",
                "source_file_id": source_file.id,
                "pipeline_run_id": run_log.id,
            })

        if not rows_to_insert:
            logger.warning(
                "bhav_no_valid_rows",
                total_rows=len(df),
                failed_rows=rows_failed,
                business_date=business_date.isoformat(),
            )
            return ExecutionResult(
                rows_processed=0,
                rows_failed=rows_failed,
                source_file_id=source_file.id,
            )

        # Upsert — ON CONFLICT (date, instrument_id) DO UPDATE
        stmt = pg_insert(DeEquityOhlcv).values(rows_to_insert)
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
                "data_status": stmt.excluded.data_status,
                "source_file_id": stmt.excluded.source_file_id,
                "pipeline_run_id": stmt.excluded.pipeline_run_id,
            },
        )
        await session.execute(stmt)
        await session.flush()

        logger.info(
            "bhav_upserted",
            rows_inserted=len(rows_to_insert),
            rows_failed=rows_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=len(rows_to_insert),
            rows_failed=rows_failed,
            source_file_id=source_file.id,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Delegate to BhavValidator for anomaly detection."""
        from app.pipelines.equity.validation import BhavValidator
        validator = BhavValidator()
        return await validator.validate(business_date, session, run_log)
