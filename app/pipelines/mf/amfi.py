"""AMFI NAV download, parse, and insert pipeline component."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeMfMaster
from app.models.prices import DeMfNavDaily

logger = get_logger(__name__)

AMFI_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
MIN_ROW_COUNT = 1000

# Universe filters: equity-oriented Growth Regular plans (~450-550 funds)
_EQUITY_KEYWORDS = ("equity", "elss", "flexi", "multi cap", "mid cap", "small cap", "large cap")
_GROWTH_KEYWORDS = ("growth",)
_EXCLUDE_KEYWORDS = ("dividend", "idcw", "weekly", "monthly", "quarterly", "annual")


@dataclass
class AmfiNavRow:
    """Parsed row from AMFI NAVAll.txt."""

    amfi_code: str
    isin_div_payout: Optional[str]
    isin_div_reinvestment: Optional[str]
    scheme_name: str
    nav: Decimal
    nav_date: date


def compute_checksum(content: bytes) -> str:
    """Compute SHA-256 checksum of raw bytes. Returns hex digest."""
    return hashlib.sha256(content).hexdigest()


def parse_amfi_date(date_str: str) -> Optional[date]:
    """Parse AMFI date string 'DD-Mon-YYYY' → date. Returns None on failure."""
    import datetime as dt

    date_str = date_str.strip()
    try:
        return dt.datetime.strptime(date_str, "%d-%b-%Y").date()
    except ValueError:
        try:
            return dt.datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            logger.warning("amfi_date_parse_failed", raw=date_str)
            return None


def parse_amfi_nav_content(content: str) -> list[AmfiNavRow]:
    """Parse AMFI NAVAll.txt content into a list of AmfiNavRow.

    Format (semicolon-delimited):
      Scheme Code;ISIN Div Payout;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

    Skips header lines, section headers (AMC names), and blank lines.
    Returns only rows with valid numeric NAV and parseable date.
    """
    rows: list[AmfiNavRow] = []

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split(";")
        if len(parts) < 6:
            # Could be a section header (AMC name) — skip silently
            continue

        amfi_code = parts[0].strip()
        # Skip lines where scheme code is not numeric (header / AMC name rows)
        if not amfi_code.isdigit():
            continue

        isin_payout = parts[1].strip() or None
        isin_reinvest = parts[2].strip() or None
        scheme_name = parts[3].strip()
        nav_raw = parts[4].strip()
        date_raw = parts[5].strip()

        # Parse NAV
        try:
            nav = Decimal(str(nav_raw))
        except InvalidOperation:
            logger.debug("amfi_nav_parse_skip_invalid_nav", amfi_code=amfi_code, nav_raw=nav_raw)
            continue

        if nav <= Decimal("0"):
            logger.debug("amfi_nav_skip_zero_nav", amfi_code=amfi_code)
            continue

        # Parse date
        nav_date = parse_amfi_date(date_raw)
        if nav_date is None:
            logger.debug("amfi_nav_skip_invalid_date", amfi_code=amfi_code, date_raw=date_raw)
            continue

        # Normalize ISIN placeholders
        if isin_payout in ("-", "N.A.", ""):
            isin_payout = None
        if isin_reinvest in ("-", "N.A.", ""):
            isin_reinvest = None

        rows.append(
            AmfiNavRow(
                amfi_code=amfi_code,
                isin_div_payout=isin_payout,
                isin_div_reinvestment=isin_reinvest,
                scheme_name=scheme_name,
                nav=nav,
                nav_date=nav_date,
            )
        )

    return rows


def filter_universe(rows: list[AmfiNavRow]) -> list[AmfiNavRow]:
    """Filter AMFI rows to target universe: equity Growth Regular plans.

    Includes rows whose scheme_name (lowercased) contains at least one equity
    keyword AND at least one growth keyword, while excluding explicit dividend
    plan variants.

    Returns ~450-550 rows for a normal trading day.
    """
    filtered: list[AmfiNavRow] = []
    for row in rows:
        name_lower = row.scheme_name.lower()
        has_equity = any(kw in name_lower for kw in _EQUITY_KEYWORDS)
        has_growth = any(kw in name_lower for kw in _GROWTH_KEYWORDS)
        has_exclude = any(kw in name_lower for kw in _EXCLUDE_KEYWORDS)

        if has_equity and has_growth and not has_exclude:
            filtered.append(row)

    logger.info("amfi_universe_filter", total_input=len(rows), filtered=len(filtered))
    return filtered


async def fetch_amfi_content(client: httpx.AsyncClient) -> bytes:
    """Download AMFI NAVAll.txt. Raises httpx.HTTPStatusError on non-2xx."""
    logger.info("amfi_fetch_start", url=AMFI_URL)
    response = await client.get(AMFI_URL, timeout=60.0, follow_redirects=True)
    response.raise_for_status()
    logger.info("amfi_fetch_complete", bytes=len(response.content))
    return response.content


async def build_amfi_code_to_mstar_map(session: AsyncSession) -> dict[str, str]:
    """Load amfi_code → mstar_id mapping from de_mf_master.

    Only includes active funds that have an amfi_code set.
    Returns dict[amfi_code, mstar_id].
    """
    result = await session.execute(
        select(DeMfMaster.amfi_code, DeMfMaster.mstar_id).where(
            DeMfMaster.amfi_code.is_not(None),
            DeMfMaster.is_active == True,  # noqa: E712
        )
    )
    mapping: dict[str, str] = {}
    for amfi_code, mstar_id in result:
        if amfi_code:
            mapping[str(amfi_code)] = mstar_id

    logger.info("amfi_mstar_map_loaded", fund_count=len(mapping))
    return mapping


async def upsert_nav_rows(
    session: AsyncSession,
    rows: list[AmfiNavRow],
    amfi_to_mstar: dict[str, str],
    pipeline_run_id: int,
) -> tuple[int, int]:
    """Upsert parsed AMFI rows into de_mf_nav_daily.

    Only inserts rows whose amfi_code has a matching mstar_id in the mapping.
    Uses ON CONFLICT (nav_date, mstar_id) DO UPDATE.

    Returns (rows_inserted, rows_skipped_no_mapping).
    """
    insert_rows: list[dict] = []
    skipped = 0

    for row in rows:
        mstar_id = amfi_to_mstar.get(row.amfi_code)
        if mstar_id is None:
            skipped += 1
            continue

        insert_rows.append(
            {
                "nav_date": row.nav_date,
                "mstar_id": mstar_id,
                "nav": row.nav,
                "nav_adj": row.nav,  # Initially same as nav; adjusted by dividends.py
                "data_status": "raw",
                "pipeline_run_id": pipeline_run_id,
            }
        )

    if not insert_rows:
        logger.warning("amfi_upsert_no_rows", skipped=skipped)
        return 0, skipped

    stmt = pg_insert(DeMfNavDaily).values(insert_rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["nav_date", "mstar_id"],
        set_={
            "nav": stmt.excluded.nav,
            "nav_adj": stmt.excluded.nav_adj,
            "data_status": stmt.excluded.data_status,
            "pipeline_run_id": stmt.excluded.pipeline_run_id,
        },
    )
    await session.execute(stmt)

    logger.info(
        "amfi_upsert_complete",
        inserted=len(insert_rows),
        skipped_no_mapping=skipped,
    )
    return len(insert_rows), skipped


def validate_freshness(rows: list[AmfiNavRow], expected_date: date) -> tuple[bool, str]:
    """Validate that fetched rows meet freshness criteria.

    Checks:
    1. Row count >= MIN_ROW_COUNT (1000)
    2. At least one row's nav_date matches expected_date

    Returns (is_valid, reason).
    """
    if len(rows) < MIN_ROW_COUNT:
        reason = f"Row count {len(rows)} below minimum {MIN_ROW_COUNT}"
        logger.warning("amfi_freshness_fail_row_count", count=len(rows), minimum=MIN_ROW_COUNT)
        return False, reason

    dates_present = {row.nav_date for row in rows}
    if expected_date not in dates_present:
        reason = (
            f"Expected date {expected_date.isoformat()} not found in parsed data. "
            f"Dates seen: {sorted(dates_present)[-3:]}"
        )
        logger.warning(
            "amfi_freshness_fail_date_mismatch",
            expected=expected_date.isoformat(),
            latest_dates=str(sorted(dates_present)[-3:]),
        )
        return False, reason

    return True, "ok"
