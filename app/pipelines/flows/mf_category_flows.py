"""MF Category Flows pipeline — monthly AMFI category-level AUM and flows via XLS."""

from __future__ import annotations

import io
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import httpx
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.flows import DeMfCategoryFlows
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.validation import AnomalyRecord

logger = get_logger(__name__)

# AMFI publishes monthly category-level MIS reports as XLS files.
# URL pattern: https://portal.amfiindia.com/spages/am{mon}{yyyy}repo.xls
# Example:     https://portal.amfiindia.com/spages/amfeb2026repo.xls
AMFI_XLS_URL_TEMPLATE = "https://portal.amfiindia.com/spages/am{mon}{yyyy}repo.xls"

MONTH_ABBREVS = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr",
    5: "may", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "oct", 11: "nov", 12: "dec",
}

# Validation bounds
MIN_CATEGORY_COUNT = 20
MAX_CATEGORY_COUNT = 60
AUM_DROP_THRESHOLD_PCT = Decimal("30")

# Columns in the AMFI XLS "AMFI MONTHLY" sheet (0-indexed positions):
# 0: Sr (serial)
# 1: Scheme Name (category label)
# 2: No. of Schemes
# 3: No. of Folios
# 4: Funds Mobilized (gross inflow, crore)
# 5: Repurchase/Redemption (gross outflow, crore)
# 6: Net Inflow/Outflow (crore)
# 7: Net AUM (crore)
# 8: Average Net AUM (crore)
# 9, 10: Segregated portfolio (ignored)
_COL_SR = 0
_COL_CATEGORY = 1
_COL_FOLIOS = 3
_COL_GROSS_INFLOW = 4
_COL_GROSS_OUTFLOW = 5
_COL_NET_FLOW = 6
_COL_NET_AUM = 7


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal, stripping commas. Returns None on failure."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA", "--", "nan"):
            return None
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Convert a value to int, stripping commas. Returns None on failure."""
    if value is None:
        return None
    try:
        cleaned = str(value).replace(",", "").strip()
        if cleaned in ("", "-", "N/A", "NA", "--", "nan"):
            return None
        return int(Decimal(cleaned))
    except (InvalidOperation, ValueError):
        return None


def build_amfi_xls_url(month_date: date) -> str:
    """Build the AMFI XLS download URL for a given month_date (first of month)."""
    mon = MONTH_ABBREVS[month_date.month]
    return AMFI_XLS_URL_TEMPLATE.format(mon=mon, yyyy=month_date.year)


def parse_amfi_xls(content: bytes, month_date: date) -> list[dict[str, Any]]:
    """Parse AMFI monthly XLS report bytes into DB row dicts.

    Reads the first sheet of the XLS file and locates the header row by
    searching for a row whose second column contains "Scheme Name" or similar.
    Skips totals / subtotals rows (blank Sr, numeric-only Sr like "Total").

    Returns a list of dicts mapping to DeMfCategoryFlows columns.
    SIP fields (sip_flow_cr, sip_accounts) are not in this file — set to None.
    """
    rows: list[dict[str, Any]] = []

    # Try xlrd for .xls (legacy format); fall back to openpyxl for .xlsx
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None, engine="xlrd")
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None, engine="openpyxl")
        except Exception as exc:
            logger.error(
                "amfi_xls_parse_failed",
                month_date=month_date.isoformat(),
                error=str(exc),
            )
            return []

    if df.empty:
        logger.warning("amfi_xls_empty_dataframe", month_date=month_date.isoformat())
        return []

    # Find the header row — look for "Scheme Name" in column B (index 1)
    header_idx: Optional[int] = None
    for i, row_vals in df.iterrows():
        cell = str(row_vals.iloc[1] if len(row_vals) > 1 else "").strip().lower()
        if "scheme" in cell or "category" in cell:
            header_idx = int(i)  # type: ignore[arg-type]
            break

    if header_idx is None:
        logger.warning(
            "amfi_xls_header_not_found",
            month_date=month_date.isoformat(),
            total_rows=len(df),
        )
        return []

    data_df = df.iloc[header_idx + 1 :].reset_index(drop=True)

    for _, row in data_df.iterrows():
        # Column B: category / scheme name
        if len(row) <= _COL_CATEGORY:
            continue

        sr_raw = str(row.iloc[_COL_SR]).strip() if len(row) > _COL_SR else ""
        category_raw = str(row.iloc[_COL_CATEGORY]).strip() if len(row) > _COL_CATEGORY else ""

        # Skip blank, NaN, or suspiciously short category names
        if not category_raw or category_raw.lower() in ("nan", "none", ""):
            continue
        if len(category_raw) < 3:
            continue

        # Skip total / subtotal / header-repeat rows
        cat_lower = category_raw.lower()
        sr_lower = sr_raw.lower()
        if any(kw in cat_lower for kw in ("total", "grand total", "industry", "sub total")):
            continue
        if any(kw in sr_lower for kw in ("total", "grand total", "sub total")):
            continue
        # Skip rows where Sr is blank or NaN (these are typically subtotal spacers)
        if sr_raw in ("", "nan", "None") or sr_raw.lower() == "sr":
            continue

        def _get(col_idx: int) -> Any:
            if len(row) > col_idx:
                val = row.iloc[col_idx]
                return None if str(val).strip().lower() == "nan" else val
            return None

        db_row: dict[str, Any] = {
            "month_date": month_date,
            "category": category_raw,
            "gross_inflow_cr": _safe_decimal(_get(_COL_GROSS_INFLOW)),
            "gross_outflow_cr": _safe_decimal(_get(_COL_GROSS_OUTFLOW)),
            "net_flow_cr": _safe_decimal(_get(_COL_NET_FLOW)),
            "aum_cr": _safe_decimal(_get(_COL_NET_AUM)),
            "folios": _safe_int(_get(_COL_FOLIOS)),
            # SIP data is not present in this XLS report
            "sip_flow_cr": None,
            "sip_accounts": None,
        }
        rows.append(db_row)

        logger.debug(
            "amfi_xls_row_parsed",
            category=category_raw,
            aum_cr=str(db_row["aum_cr"]),
            net_flow_cr=str(db_row["net_flow_cr"]),
        )

    logger.info(
        "amfi_xls_parse_complete",
        month_date=month_date.isoformat(),
        category_count=len(rows),
    )
    return rows


async def fetch_amfi_xls(client: httpx.AsyncClient, month_date: date) -> bytes:
    """Download the AMFI monthly XLS report for the given month_date.

    Raises httpx.HTTPStatusError on non-2xx responses so callers can handle
    missing months gracefully (AMFI sometimes publishes late).
    """
    url = build_amfi_xls_url(month_date)
    logger.info(
        "amfi_xls_fetch_start",
        url=url,
        month_date=month_date.isoformat(),
    )
    response = await client.get(url, timeout=60.0)
    response.raise_for_status()
    logger.info(
        "amfi_xls_fetch_complete",
        url=url,
        bytes=len(response.content),
        month_date=month_date.isoformat(),
    )
    return response.content


async def upsert_mf_category_flows(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert MF category flow rows into de_mf_category_flows.

    Uses ON CONFLICT (month_date, category) DO UPDATE for idempotent ingestion.
    Returns (rows_processed, rows_failed).
    """
    if not rows:
        logger.warning("mf_category_flows_upsert_empty_rows")
        return 0, 0

    # Deduplicate by (month_date, category) — AMFI XLS has open-ended and
    # closed-ended sections with overlapping category names. Keep the row
    # with the larger AUM (the open-ended section, which is the main one).
    deduped: dict[tuple[date, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["month_date"], row["category"])
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = row
        else:
            # Keep the row with larger AUM, or sum them if both have values
            existing_aum = existing.get("aum_cr") or Decimal(0)
            new_aum = row.get("aum_cr") or Decimal(0)
            if new_aum > existing_aum:
                deduped[key] = row
    rows = list(deduped.values())

    stmt = pg_insert(DeMfCategoryFlows).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["month_date", "category"],
        set_={
            "net_flow_cr": stmt.excluded.net_flow_cr,
            "gross_inflow_cr": stmt.excluded.gross_inflow_cr,
            "gross_outflow_cr": stmt.excluded.gross_outflow_cr,
            "aum_cr": stmt.excluded.aum_cr,
            "sip_flow_cr": stmt.excluded.sip_flow_cr,
            "sip_accounts": stmt.excluded.sip_accounts,
            "folios": stmt.excluded.folios,
            "updated_at": sa.func.now(),
        },
    )
    await session.execute(stmt)
    logger.info("mf_category_flows_upsert_complete", row_count=len(rows))
    return len(rows), 0


class MfCategoryFlowsPipeline(BasePipeline):
    """Fetches and stores monthly AMFI MF category-level AUM and flows from XLS.

    Data source: AMFI portal XLS report at
      https://portal.amfiindia.com/spages/am{mon}{yyyy}repo.xls

    Trigger: First business day of each month (or manual backfill).
    SLA: End of second business day of each month.

    requires_trading_day=False — monthly data, can run any day after AMFI
    publishes (~3-5 days after month end).
    """

    pipeline_name = "mf_category_flows"
    requires_trading_day = False

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch and upsert AMFI monthly category flows for the month of business_date."""
        month_date = business_date.replace(day=1)

        logger.info(
            "mf_category_flows_execute_start",
            business_date=business_date.isoformat(),
            month_date=month_date.isoformat(),
        )

        async with httpx.AsyncClient() as client:
            content = await fetch_amfi_xls(client, month_date)

        rows = parse_amfi_xls(content, month_date)

        if not rows:
            logger.warning(
                "mf_category_flows_no_rows_parsed",
                month_date=month_date.isoformat(),
            )
            return ExecutionResult(rows_processed=0, rows_failed=0)

        rows_processed, rows_failed = await upsert_mf_category_flows(session, rows)

        logger.info(
            "mf_category_flows_execute_complete",
            month_date=month_date.isoformat(),
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Validate monthly MF category data.

        Checks:
        1. Category count is within expected bounds (20-60).
        2. No negative AUM values.
        3. No single category dropped >30% in AUM vs prior month.
        """
        month_date = business_date.replace(day=1)
        anomalies: list[AnomalyRecord] = []

        # --- Check 1: category count ------------------------------------------
        count_result = await session.execute(
            sa.select(sa.func.count()).where(DeMfCategoryFlows.month_date == month_date)
        )
        category_count = count_result.scalar_one() or 0

        if category_count < MIN_CATEGORY_COUNT or category_count > MAX_CATEGORY_COUNT:
            severity = "high" if category_count == 0 else "medium"
            anomalies.append(
                AnomalyRecord(
                    entity_type="flow",
                    anomaly_type="category_count_out_of_range",
                    severity=severity,
                    expected_range=f"{MIN_CATEGORY_COUNT}-{MAX_CATEGORY_COUNT}",
                    actual_value=str(category_count),
                )
            )
            logger.warning(
                "mf_category_flows_category_count_anomaly",
                month_date=month_date.isoformat(),
                category_count=category_count,
            )

        # --- Check 2: negative AUM --------------------------------------------
        neg_aum_result = await session.execute(
            sa.select(DeMfCategoryFlows.category, DeMfCategoryFlows.aum_cr).where(
                DeMfCategoryFlows.month_date == month_date,
                DeMfCategoryFlows.aum_cr < 0,
            )
        )
        for cat, aum_cr in neg_aum_result.all():
            anomalies.append(
                AnomalyRecord(
                    entity_type="flow",
                    anomaly_type="negative_aum",
                    severity="high",
                    expected_range=">=0",
                    actual_value=str(aum_cr),
                    ticker=cat,
                )
            )
            logger.warning(
                "mf_category_flows_negative_aum",
                month_date=month_date.isoformat(),
                category=cat,
                aum_cr=str(aum_cr),
            )

        # --- Check 3: >30% AUM drop vs prior month ----------------------------
        if month_date.month == 1:
            prior_month_date = month_date.replace(year=month_date.year - 1, month=12)
        else:
            prior_month_date = month_date.replace(month=month_date.month - 1)

        prior_result = await session.execute(
            sa.select(DeMfCategoryFlows.category, DeMfCategoryFlows.aum_cr).where(
                DeMfCategoryFlows.month_date == prior_month_date,
                DeMfCategoryFlows.aum_cr.is_not(None),
                DeMfCategoryFlows.aum_cr > 0,
            )
        )
        prior_aum_map: dict[str, Decimal] = {
            cat: aum for cat, aum in prior_result.all()
        }

        if prior_aum_map:
            current_result = await session.execute(
                sa.select(DeMfCategoryFlows.category, DeMfCategoryFlows.aum_cr).where(
                    DeMfCategoryFlows.month_date == month_date,
                    DeMfCategoryFlows.aum_cr.is_not(None),
                )
            )
            for cat, current_aum in current_result.all():
                prior_aum = prior_aum_map.get(cat)
                if prior_aum is None or prior_aum == 0 or current_aum is None:
                    continue
                drop_pct = ((prior_aum - current_aum) / prior_aum) * Decimal("100")
                if drop_pct > AUM_DROP_THRESHOLD_PCT:
                    anomalies.append(
                        AnomalyRecord(
                            entity_type="flow",
                            anomaly_type="aum_drop_exceeded_threshold",
                            severity="medium",
                            expected_range=f"drop<{AUM_DROP_THRESHOLD_PCT}%",
                            actual_value=f"drop={drop_pct:.2f}%",
                            ticker=cat,
                        )
                    )
                    logger.warning(
                        "mf_category_flows_aum_drop_anomaly",
                        month_date=month_date.isoformat(),
                        category=cat,
                        drop_pct=float(drop_pct),
                    )

        logger.info(
            "mf_category_flows_validate_complete",
            month_date=month_date.isoformat(),
            anomaly_count=len(anomalies),
        )
        return anomalies


# ---------------------------------------------------------------------------
# Standalone backfill runner
# ---------------------------------------------------------------------------

async def main() -> None:
    """Backfill MF category flows from 2020-01 to the current month."""
    import argparse

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.config import get_settings

    parser = argparse.ArgumentParser(description="Backfill AMFI MF category flows via XLS")
    parser.add_argument("--start", default="2020-01", help="Start month YYYY-MM")
    parser.add_argument("--end", default=None, help="End month YYYY-MM (default: current month)")
    args = parser.parse_args()

    start_year, start_month = (int(x) for x in args.start.split("-"))
    if args.end:
        end_year, end_month = (int(x) for x in args.end.split("-"))
    else:
        today = date.today()
        end_year, end_month = today.year, today.month

    settings = get_settings()
    engine = create_async_engine(settings.database_url, pool_size=5, pool_pre_ping=True)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    pipeline = MfCategoryFlowsPipeline()

    year, month = start_year, start_month
    ok, fail = 0, 0

    while (year, month) <= (end_year, end_month):
        business_date = date(year, month, 1)
        try:
            async with sf() as session:
                async with session.begin():
                    result = await pipeline.run(business_date, session)
                    print(f"  {year}-{month:02d}: {result.status} — {result.rows_processed} rows")
                    ok += 1
        except Exception as exc:
            print(f"  {year}-{month:02d}: FAILED — {exc}")
            fail += 1

        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

    await engine.dispose()
    print(f"\nDone. {ok} months OK, {fail} failed.")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
