"""G-Sec daily yield curve pipeline — primary CCIL NDS-OM with RBI DBIE fallback.

Fetches end-of-day benchmark G-Sec yields across standard tenor buckets
(1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 15Y, 30Y, 40Y) and upserts into de_gsec_yield.
"""

from __future__ import annotations

import io
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeGsecYield
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)


CCIL_NDSOM_URL = "https://www.ccilindia.com/web/ccil/ndsom-end-day-snapshot"
RBI_DBIE_URL = "https://dbie.rbi.org.in/DBIE/dbie.rbi?site=statistics"

BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.ccilindia.com/",
}

# Benchmark tenor buckets (in years). Maturities will be rounded to the
# nearest bucket within TENOR_TOLERANCE_YEARS.
STANDARD_TENORS_YEARS: list[int] = [1, 2, 3, 5, 7, 10, 15, 30, 40]
TENOR_TOLERANCE_YEARS: float = 0.75  # +/- 9 months from the bucket

REQUEST_TIMEOUT_SECONDS: float = 30.0


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert value to Decimal safely; strip commas/percent signs."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").replace("%", "").strip()
        if cleaned in ("", "-", "N/A", "NA"):
            return None
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _parse_maturity_date(value: Any) -> date | None:
    """Parse maturity date strings from CCIL (e.g. '22-Feb-2033', '2033-02-22')."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _years_to_maturity(maturity: date, business_date: date) -> float:
    """Approximate years-to-maturity as a float."""
    delta_days = (maturity - business_date).days
    return delta_days / 365.25


def _map_to_tenor_bucket(years: float) -> str | None:
    """Map years-to-maturity to the nearest standard tenor bucket.

    Returns the tenor string (e.g. '10Y') if within tolerance, else None.
    """
    if years <= 0:
        return None
    nearest = min(STANDARD_TENORS_YEARS, key=lambda t: abs(t - years))
    if abs(nearest - years) <= TENOR_TOLERANCE_YEARS:
        return f"{nearest}Y"
    return None


def _normalise_header(text: str) -> str:
    """Lowercase + strip non-alphanumerics for robust column matching."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _pick_yield(lty: Decimal | None, wavg: Decimal | None) -> Decimal | None:
    """Prefer weighted average yield when available, else last traded yield."""
    if wavg is not None:
        return wavg
    return lty


def _parse_ccil_html(html: str, business_date: date) -> list[dict[str, Any]]:
    """Parse the CCIL NDS-OM end-of-day HTML table.

    Typical columns: 'Security', 'Maturity Date', 'LTP', 'LTY', 'Weighted Avg Yield'.
    Uses pandas.read_html to extract tables, then picks the one with relevant
    columns. Returns a list of row dicts keyed by (yield_date, tenor).
    """
    # Import lazily — pandas is a heavy dep and may not be needed on every path.
    import pandas as pd

    try:
        tables = pd.read_html(io.StringIO(html))
    except ValueError as exc:
        raise RuntimeError(f"No HTML tables found in CCIL response: {exc}") from exc

    # Locate the table containing yield columns.
    chosen: "pd.DataFrame | None" = None
    for tbl in tables:
        if tbl is None or tbl.empty:
            continue
        cols_norm = {_normalise_header(str(c)): str(c) for c in tbl.columns}
        has_maturity = any("maturity" in k for k in cols_norm)
        has_yield = any(("yield" in k) or k in ("lty", "wayield") for k in cols_norm)
        if has_maturity and has_yield:
            chosen = tbl
            break

    if chosen is None:
        raise RuntimeError("CCIL page did not contain a recognisable yields table")

    # Resolve column names flexibly.
    cols_norm = {_normalise_header(str(c)): str(c) for c in chosen.columns}

    def _find(*keys: str) -> str | None:
        for key in keys:
            for norm, original in cols_norm.items():
                if key in norm:
                    return original
        return None

    col_security = _find("security", "isin", "descr")
    col_maturity = _find("maturity")
    col_lty = _find("lty", "lasttradedyield", "lastyield")
    col_wavg = _find("weightedavgyield", "weightedaverageyield", "wayield", "wavgyield")

    if col_maturity is None or (col_lty is None and col_wavg is None):
        raise RuntimeError(
            f"CCIL table missing required columns; got {list(chosen.columns)!r}"
        )

    # Pick the best yield per tenor bucket.
    # Heuristic: within a bucket, prefer the row whose maturity is closest to
    # the bucket's nominal years (i.e. the on-the-run benchmark).
    best_per_tenor: dict[str, tuple[float, dict[str, Any]]] = {}

    for _, row in chosen.iterrows():
        maturity = _parse_maturity_date(row.get(col_maturity))
        if maturity is None:
            continue

        years = _years_to_maturity(maturity, business_date)
        tenor = _map_to_tenor_bucket(years)
        if tenor is None:
            continue

        lty = _safe_decimal(row.get(col_lty)) if col_lty else None
        wavg = _safe_decimal(row.get(col_wavg)) if col_wavg else None
        yield_pct = _pick_yield(lty, wavg)
        if yield_pct is None:
            continue

        # Sanity range: Indian G-Sec yields ~ 3-15%.
        if yield_pct <= Decimal("0") or yield_pct >= Decimal("30"):
            continue

        security_name = None
        if col_security is not None:
            sec_raw = row.get(col_security)
            if sec_raw is not None and str(sec_raw).strip() not in ("", "nan"):
                security_name = str(sec_raw).strip()[:100]

        nominal_years = int(tenor.rstrip("Y"))
        distance = abs(nominal_years - years)

        candidate = {
            "yield_date": business_date,
            "tenor": tenor,
            "yield_pct": yield_pct,
            "security_name": security_name,
            "source": "CCIL",
        }

        existing = best_per_tenor.get(tenor)
        if existing is None or distance < existing[0]:
            best_per_tenor[tenor] = (distance, candidate)

    return [entry[1] for entry in best_per_tenor.values()]


async def _fetch_from_ccil(client: httpx.AsyncClient) -> str:
    """Fetch the CCIL NDS-OM end-of-day snapshot HTML page."""
    response = await client.get(
        CCIL_NDSOM_URL,
        headers=BROWSER_HEADERS,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.text


async def _fetch_from_rbi_dbie(
    client: httpx.AsyncClient,
    business_date: date,
) -> list[dict[str, Any]]:
    """RBI DBIE fallback — intentionally best-effort.

    The DBIE site requires stateful navigation to fetch the G-Sec yield
    series and is not practical to implement inline. We log and return
    an empty list so the caller can decide whether to fail.
    """
    logger.warning(
        "gsec_yields_rbi_dbie_fallback_not_implemented",
        url=RBI_DBIE_URL,
        business_date=business_date.isoformat(),
    )
    return []


async def upsert_gsec_yields(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert yield rows into de_gsec_yield. Returns (processed, failed)."""
    if not rows:
        return 0, 0

    stmt = pg_insert(DeGsecYield).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["yield_date", "tenor"],
        set_={
            "yield_pct": stmt.excluded.yield_pct,
            "security_name": stmt.excluded.security_name,
            "source": stmt.excluded.source,
        },
    )
    await session.execute(stmt)
    return len(rows), 0


class GsecYieldsPipeline(BasePipeline):
    """Fetches daily G-Sec benchmark yields across the tenor curve.

    Primary source: CCIL NDS-OM end-of-day snapshot (HTML table).
    Fallback: RBI DBIE weekly series (best-effort; currently a stub).
    If both sources fail to produce any rows, the pipeline raises so the
    run is marked failed.
    """

    pipeline_name = "gsec_yields"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "gsec_yields_execute_start",
            business_date=business_date.isoformat(),
        )

        rows: list[dict[str, Any]] = []
        source_used = "CCIL"
        primary_error: Exception | None = None

        async with httpx.AsyncClient(follow_redirects=True) as client:
            # ---- PRIMARY: CCIL NDS-OM end-of-day snapshot ----
            try:
                html = await _fetch_from_ccil(client)
                rows = _parse_ccil_html(html, business_date)
                logger.info(
                    "gsec_yields_ccil_success",
                    parsed_rows=len(rows),
                    business_date=business_date.isoformat(),
                )
            except Exception as exc:
                primary_error = exc
                logger.warning(
                    "gsec_yields_ccil_failed",
                    error=str(exc),
                    business_date=business_date.isoformat(),
                )

            # ---- FALLBACK: RBI DBIE (best-effort) ----
            if not rows:
                try:
                    rows = await _fetch_from_rbi_dbie(client, business_date)
                    if rows:
                        source_used = "RBI"
                        logger.info(
                            "gsec_yields_rbi_fallback_success",
                            parsed_rows=len(rows),
                            business_date=business_date.isoformat(),
                        )
                except Exception as exc:
                    logger.error(
                        "gsec_yields_rbi_fallback_failed",
                        error=str(exc),
                        business_date=business_date.isoformat(),
                    )

        if not rows:
            logger.error(
                "gsec_yields_no_rows_all_sources_failed",
                business_date=business_date.isoformat(),
                primary_error=str(primary_error) if primary_error else None,
            )
            raise RuntimeError(
                "G-Sec yields unavailable: CCIL primary failed"
                f"{f' ({primary_error})' if primary_error else ''}"
                " and RBI DBIE fallback produced no data"
            )

        rows_processed, rows_failed = await upsert_gsec_yields(session, rows)

        logger.info(
            "gsec_yields_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            source=source_used,
            tenors=[r["tenor"] for r in rows],
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
