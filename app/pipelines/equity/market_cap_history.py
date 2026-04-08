"""AMFI market cap category history ingestion pipeline.

Downloads AMFI's semi-annual large/mid/small cap classification list
(published every January and July per SEBI regulations) and populates the
de_market_cap_history table with effective date ranges.

Source URL pattern:
  Short (2025+):
    https://www.amfiindia.com/Themes/Theme1/downloads/AverageMarketCapitalization{date}.xlsx
  Long (pre-2025 fallback):
    https://www.amfiindia.com/Themes/Theme1/downloads/
      AverageMarketCapitalizationoflistedcompaniesduringthesixmonthsended{date}.xlsx

Date format in URL:
  H1 (Jan–Jun): 30Jun{YYYY}
  H2 (Jul–Dec): 31Dec{YYYY}

XLSX columns (0-indexed):
  0  Sr. No. (rank)
  1  Company name
  2  ISIN
  3  BSE Symbol
  4  BSE 6-month Avg Market Cap (crore)
  5  NSE Symbol
  6  NSE 6-month Avg Market Cap (crore)
  7-8 MSEI (ignored)
  9  Average of All Exchanges (crore)
  10 Categorization: "Large Cap", "Mid Cap", "Small Cap"

SEBI cap classification rules (SEBI circular SEBI/HO/IMD/DF3/CIR/P/2017/114):
  Rank 1-100   = large cap
  Rank 101-250 = mid cap
  Rank 251-500 = small cap
  Rank 501+    = micro cap (not in AMFI list — derived from rank)

AMFI publishes two lists per year:
  - January 1  (H2 of preceding year period ending 31-Dec)
  - July 1     (H1 of current year period ending 30-Jun)
"""

from __future__ import annotations

import asyncio
import io
import uuid
from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument, DeMarketCapHistory
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# URL templates
# ---------------------------------------------------------------------------
AMFI_SHORT_URL_TEMPLATE = (
    "https://www.amfiindia.com/Themes/Theme1/downloads/"
    "AverageMarketCapitalization{date_str}.xlsx"
)
AMFI_LONG_URL_TEMPLATE = (
    "https://www.amfiindia.com/Themes/Theme1/downloads/"
    "AverageMarketCapitalizationoflistedcompaniesduringthesixmonthsended{date_str}.xlsx"
)

AMFI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.amfiindia.com/",
}

DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0

# SEBI rank thresholds
LARGE_CAP_MAX_RANK = 100
MID_CAP_MAX_RANK = 250
SMALL_CAP_MAX_RANK = 500

# Expected count tolerances (±30% of ideal)
EXPECTED_LARGE_CAP = 100
EXPECTED_MID_CAP = 150
EXPECTED_SMALL_CAP = 250

# Category label normalisation
_AMFI_CATEGORY_MAP: dict[str, str] = {
    "large cap": "large",
    "mid cap": "mid",
    "small cap": "small",
}

# Category ordering for jump detection
_CATEGORY_ORDER: dict[str, int] = {
    "large": 0,
    "mid": 1,
    "small": 2,
    "micro": 3,
}


# ---------------------------------------------------------------------------
# Pure helper functions (also exported for backward-compat with existing tests)
# ---------------------------------------------------------------------------

def rank_to_cap_category(rank: int) -> str:
    """Convert SEBI market cap rank to cap_category string.

    Per SEBI circular SEBI/HO/IMD/DF3/CIR/P/2017/114:
      Rank 1-100   → large
      Rank 101-250 → mid
      Rank 251-500 → small
      Rank 501+    → micro

    Args:
        rank: Integer rank from AMFI list (1-based).

    Returns:
        One of 'large', 'mid', 'small', 'micro'.
    """
    if rank <= LARGE_CAP_MAX_RANK:
        return "large"
    elif rank <= MID_CAP_MAX_RANK:
        return "mid"
    elif rank <= SMALL_CAP_MAX_RANK:
        return "small"
    else:
        return "micro"


def determine_effective_from(reference_date: date) -> date:
    """Determine the effective_from date for the current AMFI cap list period.

    AMFI publishes lists effective January 1 and July 1 each year.
    Given any reference date, returns the most recent publication start date.

    Args:
        reference_date: The business_date passed to the pipeline.

    Returns:
        date object — either Jan 1 or Jul 1 of the appropriate year.
    """
    year = reference_date.year
    if reference_date.month >= 7:
        return date(year, 7, 1)
    else:
        return date(year, 1, 1)


def _build_amfi_date_str(effective_from: date) -> str:
    """Build the date string used in the AMFI XLSX filename.

    Conventions:
      H1 (effective_from = Jan 1) → file covers Jan–Jun → "30Jun{YYYY}"
      H2 (effective_from = Jul 1) → file covers Jul–Dec → "31Dec{YYYY}"

    Args:
        effective_from: Jan 1 or Jul 1 of the relevant year.

    Returns:
        Date string like "30Jun2025" or "31Dec2025".
    """
    if effective_from.month == 1:
        # Effective Jan 1 → file covers Jan–Jun → "30Jun{YYYY}"
        return f"30Jun{effective_from.year}"
    else:
        # Effective Jul 1 → file covers Jul–Dec → "31Dec{YYYY}"
        return f"31Dec{effective_from.year}"


def _build_amfi_urls(effective_from: date) -> list[str]:
    """Return ordered list of URLs to try for the AMFI XLSX.

    Short URL is tried first (newer files), long URL is the fallback
    for older periods (pre-2025).

    Args:
        effective_from: Jan 1 or Jul 1 of the relevant year.

    Returns:
        List of two URL strings.
    """
    date_str = _build_amfi_date_str(effective_from)
    return [
        AMFI_SHORT_URL_TEMPLATE.format(date_str=date_str),
        AMFI_LONG_URL_TEMPLATE.format(date_str=date_str),
    ]


def _normalise_category(raw: str) -> str | None:
    """Normalise AMFI categorization label to internal cap_category value.

    Args:
        raw: Raw string from the XLSX categorization column.

    Returns:
        One of 'large', 'mid', 'small', or None if unrecognised.
    """
    return _AMFI_CATEGORY_MAP.get(raw.strip().lower())


def parse_amfi_xlsx(content: bytes) -> list[dict[str, Any]]:
    """Parse AMFI AverageMarketCapitalization XLSX content.

    Reads the spreadsheet using pandas/openpyxl and extracts:
      - Sr. No. (rank) — column index 0
      - Company name — column index 1
      - ISIN — column index 2
      - NSE Symbol — column index 5
      - Categorization — last column (index 10 when full; detected by header)

    The categorization column is detected by the header containing
    "categor" (case-insensitive). If not found, falls back to column index 10.

    Args:
        content: Raw bytes of the downloaded .xlsx file.

    Returns:
        List of dicts with keys: rank, company_name, isin, nse_symbol,
        cap_category. Empty list if parsing fails.
    """
    try:
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl", header=None)
    except Exception as exc:
        logger.warning("amfi_xlsx_load_failed", error=str(exc))
        return []

    if df.empty:
        return []

    # Locate header row (first row where any cell contains "ISIN" or "isin")
    header_row_idx: int | None = None
    for idx, row in df.iterrows():
        row_str = [str(v).strip().lower() for v in row.values]
        if any("isin" in v for v in row_str):
            header_row_idx = int(str(idx))
            break

    if header_row_idx is None:
        logger.warning("amfi_xlsx_no_header_row_found", shape=str(df.shape))
        return []

    # Re-read with the detected header row
    try:
        df = pd.read_excel(
            io.BytesIO(content),
            engine="openpyxl",
            header=header_row_idx,
        )
    except Exception as exc:
        logger.warning("amfi_xlsx_reread_failed", error=str(exc))
        return []

    # Normalise column names
    df.columns = [str(c).strip() for c in df.columns]
    col_names_lower = [c.lower() for c in df.columns]

    # Detect categorization column (contains "categor")
    cat_col_idx: int | None = None
    for i, c in enumerate(col_names_lower):
        if "categor" in c:
            cat_col_idx = i
            break
    # Fallback: last column
    if cat_col_idx is None:
        cat_col_idx = len(df.columns) - 1

    # Detect NSE Symbol column (contains "nse" and "symbol")
    nse_col_idx: int | None = None
    for i, c in enumerate(col_names_lower):
        if "nse" in c and "symbol" in c:
            nse_col_idx = i
            break
    # Fallback: column index 5
    if nse_col_idx is None and len(df.columns) > 5:
        nse_col_idx = 5

    # Detect rank column (contains "sr" or "rank" or "no")
    rank_col_idx: int | None = None
    for i, c in enumerate(col_names_lower):
        if "sr" in c or "rank" in c:
            rank_col_idx = i
            break
    if rank_col_idx is None:
        rank_col_idx = 0

    # Detect ISIN column
    isin_col_idx: int | None = None
    for i, c in enumerate(col_names_lower):
        if c == "isin" or c.endswith("isin"):
            isin_col_idx = i
            break
    if isin_col_idx is None:
        isin_col_idx = 2

    # Detect Company name column
    name_col_idx: int | None = None
    for i, c in enumerate(col_names_lower):
        if "company" in c or "name" in c:
            name_col_idx = i
            break
    if name_col_idx is None:
        name_col_idx = 1

    rows: list[dict[str, Any]] = []
    col_count = len(df.columns)

    for _, row in df.iterrows():
        vals = list(row.values)

        # Rank
        rank_raw = vals[rank_col_idx] if rank_col_idx < col_count else None
        if rank_raw is None or (isinstance(rank_raw, float) and pd.isna(rank_raw)):
            continue
        try:
            rank = int(str(rank_raw).strip().split(".")[0])
        except (ValueError, TypeError):
            continue
        if rank <= 0:
            continue

        # ISIN
        isin_raw = vals[isin_col_idx] if isin_col_idx < col_count else None
        isin = str(isin_raw).strip().upper() if isin_raw and not (
            isinstance(isin_raw, float) and pd.isna(isin_raw)
        ) else ""

        # NSE Symbol
        nse_raw = (
            vals[nse_col_idx]
            if nse_col_idx is not None and nse_col_idx < col_count
            else None
        )
        nse_symbol = str(nse_raw).strip() if nse_raw and not (
            isinstance(nse_raw, float) and pd.isna(nse_raw)
        ) else ""

        # Company name
        name_raw = vals[name_col_idx] if name_col_idx < col_count else ""
        company_name = str(name_raw).strip() if name_raw and not (
            isinstance(name_raw, float) and pd.isna(name_raw)
        ) else ""

        # Categorization
        cat_raw = vals[cat_col_idx] if cat_col_idx < col_count else None
        if cat_raw is None or (isinstance(cat_raw, float) and pd.isna(cat_raw)):
            # Fall back to rank-based classification
            cap_category = rank_to_cap_category(rank)
        else:
            normalised = _normalise_category(str(cat_raw))
            cap_category = normalised if normalised else rank_to_cap_category(rank)

        rows.append(
            {
                "rank": rank,
                "company_name": company_name,
                "isin": isin,
                "nse_symbol": nse_symbol,
                "cap_category": cap_category,
            }
        )

    return rows


def parse_amfi_cap_list(content: bytes) -> list[dict[str, Any]]:
    """Parse AMFI capitalization list content into structured rows.

    Tries xlsx parsing first (using parse_amfi_xlsx). Falls back to
    text/CSV parsing for older formats.

    Each returned dict has keys:
      rank (int), company_name (str), isin (str), cap_category (str)
    Plus optionally: nse_symbol (str)

    Args:
        content: Raw bytes of the downloaded file.

    Returns:
        List of dicts with parsed rows. Empty list if parsing fails.
    """
    rows = parse_amfi_xlsx(content)
    if rows:
        return rows

    # Fallback: try CSV/text parsing
    rows = _parse_text(content)
    return rows


def _parse_text(content: bytes) -> list[dict[str, Any]]:
    """Parse CSV/text format AMFI cap list (fallback for non-xlsx content).

    Tries to detect delimiter (comma, tab, pipe) and locate columns by header.
    Rank-based categorization is used when the text format lacks a category col.

    Args:
        content: Raw bytes of the file.

    Returns:
        Parsed rows or empty list.
    """
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    # Detect delimiter
    sample = lines[0]
    if "\t" in sample:
        delimiter = "\t"
    elif "|" in sample:
        delimiter = "|"
    else:
        delimiter = ","

    header_idx: int | None = None
    rank_col = isin_col = name_col = None

    for i, line in enumerate(lines):
        cols = [c.strip().lower() for c in line.split(delimiter)]
        if any("isin" in c for c in cols):
            header_idx = i
            for idx, cell in enumerate(cols):
                if "rank" in cell:
                    rank_col = idx
                elif "isin" in cell:
                    isin_col = idx
                elif "company" in cell or "name" in cell:
                    name_col = idx
            break

    if header_idx is None or isin_col is None:
        logger.warning("amfi_text_no_header_found", line_count=len(lines))
        return []

    rows: list[dict[str, Any]] = []
    for line in lines[header_idx + 1:]:
        parts = [c.strip() for c in line.split(delimiter)]
        if len(parts) <= isin_col:
            continue

        rank_val = parts[rank_col] if rank_col is not None and rank_col < len(parts) else ""
        isin_val = parts[isin_col]
        name_val = parts[name_col] if name_col is not None and name_col < len(parts) else ""

        try:
            rank = int(rank_val)
        except (ValueError, TypeError):
            continue

        isin = isin_val.strip().upper()
        if not isin or len(isin) != 12:
            continue

        cap_category = rank_to_cap_category(rank)
        rows.append(
            {
                "rank": rank,
                "company_name": name_val,
                "isin": isin,
                "nse_symbol": "",
                "cap_category": cap_category,
            }
        )

    return rows


async def fetch_amfi_cap_list(
    client: httpx.AsyncClient,
    period_date: date,
) -> bytes:
    """Download the AMFI AverageMarketCapitalization XLSX with retry.

    Tries the short URL first (newer naming convention), then the long URL
    fallback for older periods.

    Args:
        client: httpx async client.
        period_date: The effective_from date (Jan 1 or Jul 1).

    Returns:
        Raw bytes of the downloaded file.

    Raises:
        RuntimeError: If all download attempts fail.
    """
    urls_to_try = _build_amfi_urls(period_date)
    last_exc: Exception | None = None

    for url in urls_to_try:
        for attempt in range(DOWNLOAD_RETRIES):
            try:
                logger.info(
                    "amfi_cap_list_download_attempt",
                    url=url,
                    attempt=attempt + 1,
                    period_date=period_date.isoformat(),
                )
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                content = response.content
                if content:
                    logger.info(
                        "amfi_cap_list_downloaded",
                        url=url,
                        bytes=len(content),
                        period_date=period_date.isoformat(),
                    )
                    return content
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_exc = exc
                wait_secs = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "amfi_cap_list_retry",
                    url=url,
                    attempt=attempt + 1,
                    wait_secs=wait_secs,
                    error=str(exc),
                )
                if attempt < DOWNLOAD_RETRIES - 1:
                    await asyncio.sleep(wait_secs)

    raise last_exc or RuntimeError(
        f"Failed to download AMFI cap list for period {period_date.isoformat()}"
    )


async def _load_symbol_to_instrument_map(
    session: AsyncSession,
) -> dict[str, uuid.UUID]:
    """Query de_instrument for current_symbol → id mapping.

    Only active instruments are included.

    Args:
        session: Async SQLAlchemy session.

    Returns:
        Dict mapping uppercase NSE symbol to instrument UUID.
    """
    result = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    return {row.current_symbol.upper(): row.id for row in result if row.current_symbol}


async def _load_isin_to_instrument_map(
    session: AsyncSession,
) -> dict[str, uuid.UUID]:
    """Query de_instrument for isin → id mapping (fallback lookup).

    Only instruments with a non-null ISIN are included.

    Args:
        session: Async SQLAlchemy session.

    Returns:
        Dict mapping uppercase 12-char ISIN to instrument UUID.
    """
    result = await session.execute(
        select(DeInstrument.isin, DeInstrument.id).where(
            DeInstrument.isin.isnot(None),
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    return {row.isin.upper(): row.id for row in result if row.isin}


class MarketCapHistoryPipeline(BasePipeline):
    """AMFI semi-annual market cap classification ingestion pipeline.

    Downloads AMFI's AverageMarketCapitalization XLSX and populates
    de_market_cap_history with effective date ranges per instrument.

    Period logic:
      - effective_from = Jan 1 or Jul 1 of the relevant half-year
      - Previous period rows have effective_to set to effective_from - 1 day
      - New rows have effective_to = NULL (current period)

    Instrument matching order:
      1. NSE Symbol → de_instrument.current_symbol (primary)
      2. ISIN → de_instrument.isin (fallback)
    """

    pipeline_name = "market_cap_history"
    requires_trading_day = False

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch AMFI XLSX and upsert into de_market_cap_history.

        Steps:
          1. Determine effective_from (Jan 1 or Jul 1)
          2. Build dated URL and download XLSX
          3. Parse XLSX using parse_amfi_cap_list()
          4. Load symbol → instrument_id and ISIN → instrument_id maps
          5. Match each row; primary = NSE Symbol, fallback = ISIN
          6. Close out previous period (set effective_to = effective_from - 1)
          7. Upsert new rows ON CONFLICT (instrument_id, effective_from)

        Args:
            business_date: Pipeline execution date.
            session: Async DB session.
            run_log: Pipeline log entry.

        Returns:
            ExecutionResult with rows_processed and rows_failed counts.
        """
        effective_from = determine_effective_from(business_date)
        logger.info(
            "market_cap_history_execute_start",
            business_date=business_date.isoformat(),
            effective_from=effective_from.isoformat(),
        )

        # Download and parse
        async with httpx.AsyncClient(headers=AMFI_HEADERS, timeout=60.0) as client:
            content = await fetch_amfi_cap_list(client, effective_from)

        parsed_rows = parse_amfi_cap_list(content)

        if not parsed_rows:
            logger.error(
                "market_cap_history_no_rows_parsed",
                effective_from=effective_from.isoformat(),
                content_bytes=len(content),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        logger.info(
            "market_cap_history_parsed",
            row_count=len(parsed_rows),
            effective_from=effective_from.isoformat(),
        )

        # Load lookup maps
        symbol_to_id = await _load_symbol_to_instrument_map(session)
        isin_to_id = await _load_isin_to_instrument_map(session)

        rows_processed = 0
        rows_failed = 0
        insert_rows: list[dict[str, Any]] = []

        for row in parsed_rows:
            instrument_id: uuid.UUID | None = None

            # Primary: NSE Symbol match
            nse_symbol = row.get("nse_symbol", "").strip().upper()
            if nse_symbol:
                instrument_id = symbol_to_id.get(nse_symbol)

            # Fallback: ISIN match
            if instrument_id is None:
                isin = row.get("isin", "").strip().upper()
                if isin and len(isin) == 12:
                    instrument_id = isin_to_id.get(isin)

            if instrument_id is None:
                logger.debug(
                    "market_cap_history_no_instrument_match",
                    nse_symbol=row.get("nse_symbol", ""),
                    isin=row.get("isin", ""),
                    company_name=row.get("company_name", ""),
                )
                rows_failed += 1
                continue

            insert_rows.append(
                {
                    "instrument_id": instrument_id,
                    "effective_from": effective_from,
                    "cap_category": row["cap_category"],
                    "effective_to": None,
                    "source": "AMFI",
                }
            )

        if not insert_rows:
            logger.warning(
                "market_cap_history_no_matching_instruments",
                total_parsed=len(parsed_rows),
                rows_failed=rows_failed,
            )
            return ExecutionResult(rows_processed=0, rows_failed=rows_failed)

        # Close out previous period — set effective_to = effective_from - 1 day
        # Only update rows where effective_to IS NULL and effective_from < current period
        close_stmt = (
            sa.update(DeMarketCapHistory)
            .where(
                DeMarketCapHistory.effective_to.is_(None),
                DeMarketCapHistory.effective_from < effective_from,
            )
            .values(effective_to=effective_from - timedelta(days=1))
        )
        close_result = await session.execute(close_stmt)
        closed_count = close_result.rowcount

        logger.info(
            "market_cap_history_previous_period_closed",
            closed_count=closed_count,
            effective_from=effective_from.isoformat(),
        )

        # Deduplicate by (instrument_id, effective_from) — AMFI list can have
        # duplicate entries when a stock is matched by both symbol and ISIN
        deduped: dict[tuple, dict] = {}
        for row in insert_rows:
            key = (row["instrument_id"], row["effective_from"])
            deduped[key] = row  # last wins
        insert_rows = list(deduped.values())

        # Upsert new rows
        stmt = pg_insert(DeMarketCapHistory).values(insert_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["instrument_id", "effective_from"],
            set_={
                "cap_category": stmt.excluded.cap_category,
                "effective_to": stmt.excluded.effective_to,
                "source": stmt.excluded.source,
            },
        )
        await session.execute(stmt)
        rows_processed = len(insert_rows)

        logger.info(
            "market_cap_history_execute_complete",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            effective_from=effective_from.isoformat(),
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
        """Validate the ingested cap classification data.

        Checks:
          1. Large cap count should be ~100 (within 30% tolerance)
          2. Mid cap count should be ~150 (within 30% tolerance)
          3. Small cap count should be ~250 (within 30% tolerance)
          4. Flag instruments that jumped more than one category vs prior period

        Args:
            business_date: Pipeline execution date.
            session: Async DB session.
            run_log: Pipeline log entry.

        Returns:
            List of AnomalyRecord for detected issues.
        """
        effective_from = determine_effective_from(business_date)
        anomalies: list[AnomalyRecord] = []

        # Count rows by cap_category for this period
        count_result = await session.execute(
            select(
                DeMarketCapHistory.cap_category,
                sa.func.count(DeMarketCapHistory.instrument_id).label("cnt"),
            )
            .where(DeMarketCapHistory.effective_from == effective_from)
            .group_by(DeMarketCapHistory.cap_category)
        )
        counts: dict[str, int] = {row.cap_category: row.cnt for row in count_result}

        if not counts:
            logger.warning(
                "market_cap_history_validate_no_rows",
                effective_from=effective_from.isoformat(),
            )
            return anomalies

        # Validate expected counts
        tolerance = 0.30  # ±30%
        expected_map = {
            "large": EXPECTED_LARGE_CAP,
            "mid": EXPECTED_MID_CAP,
            "small": EXPECTED_SMALL_CAP,
        }

        for category, expected in expected_map.items():
            actual = counts.get(category, 0)
            lower = int(expected * (1 - tolerance))
            upper = int(expected * (1 + tolerance))
            if not (lower <= actual <= upper):
                severity = "high" if actual == 0 else "medium"
                anomalies.append(
                    AnomalyRecord(
                        entity_type="equity",
                        anomaly_type="cap_category_count_mismatch",
                        severity=severity,
                        expected_range=f"{lower} to {upper}",
                        actual_value=str(actual),
                    )
                )
                logger.warning(
                    "market_cap_history_count_anomaly",
                    category=category,
                    expected=expected,
                    actual=actual,
                    lower=lower,
                    upper=upper,
                    effective_from=effective_from.isoformat(),
                )

        # Detect instruments that jumped more than one category
        prev_period_subq = (
            select(
                DeMarketCapHistory.instrument_id,
                DeMarketCapHistory.cap_category.label("prev_category"),
            )
            .where(
                DeMarketCapHistory.effective_to == effective_from - timedelta(days=1),
            )
            .subquery("prev_period")
        )

        curr_period_subq = (
            select(
                DeMarketCapHistory.instrument_id,
                DeMarketCapHistory.cap_category.label("curr_category"),
            )
            .where(DeMarketCapHistory.effective_from == effective_from)
            .subquery("curr_period")
        )

        jump_result = await session.execute(
            select(
                curr_period_subq.c.instrument_id,
                prev_period_subq.c.prev_category,
                curr_period_subq.c.curr_category,
            ).join(
                prev_period_subq,
                curr_period_subq.c.instrument_id == prev_period_subq.c.instrument_id,
            )
        )

        for row in jump_result:
            prev_order = _CATEGORY_ORDER.get(row.prev_category, -1)
            curr_order = _CATEGORY_ORDER.get(row.curr_category, -1)
            if prev_order < 0 or curr_order < 0:
                continue
            jump = abs(curr_order - prev_order)
            if jump > 1:
                anomalies.append(
                    AnomalyRecord(
                        entity_type="equity",
                        anomaly_type="cap_category_jump",
                        severity="medium",
                        instrument_id=row.instrument_id,
                        expected_range="adjacent category only",
                        actual_value=f"{row.prev_category} → {row.curr_category}",
                    )
                )
                logger.warning(
                    "market_cap_history_category_jump",
                    instrument_id=str(row.instrument_id),
                    prev_category=row.prev_category,
                    curr_category=row.curr_category,
                    jump=jump,
                )

        logger.info(
            "market_cap_history_validate_complete",
            anomaly_count=len(anomalies),
            counts=counts,
            effective_from=effective_from.isoformat(),
        )

        return anomalies


# ---------------------------------------------------------------------------
# Standalone backfill runner
# ---------------------------------------------------------------------------

async def main() -> None:
    """Backfill market cap history from AMFI for all half-year periods.

    Loops 2020-H2, 2021-H1, 2021-H2, ..., through the current period.
    For each period, calls pipeline.run(business_date=period_start).
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.config import get_settings

    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_size=5, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    pipeline = MarketCapHistoryPipeline()

    today = date.today()
    ok_count = 0
    fail_count = 0

    # Generate all half-year period start dates from 2020-H2 to now
    periods: list[date] = []
    for year in range(2020, today.year + 1):
        for month in (1, 7):
            period_start = date(year, month, 1)
            if period_start > today:
                break
            periods.append(period_start)

    # AMFI only has files from 31Dec2022 onward. Start from 2023-H1.
    periods = [p for p in periods if p >= date(2023, 1, 1)]

    for period_start in periods:
        half = "H1" if period_start.month == 1 else "H2"
        label = f"{period_start.year}-{half}"
        try:
            async with session_factory() as session:
                async with session.begin():
                    from app.models.pipeline import DePipelineLog
                    from datetime import datetime, timezone
                    run_log = DePipelineLog(
                        pipeline_name="market_cap_history",
                        business_date=period_start,
                        run_number=1,
                        status="running",
                        started_at=datetime.now(tz=timezone.utc),
                    )
                    result = await pipeline.execute(period_start, session, run_log)
                    print(
                        f"  {label}: OK — "
                        f"{result.rows_processed} rows processed, "
                        f"{result.rows_failed} unmatched"
                    )
                    ok_count += 1
        except Exception as exc:
            print(f"  {label}: FAILED — {exc}")
            fail_count += 1

    await engine.dispose()
    print(f"\nDone. {ok_count} periods OK, {fail_count} failed.")


if __name__ == "__main__":
    asyncio.run(main())
