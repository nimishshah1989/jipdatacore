"""Morningstar monthly holdings fetch pipeline.

Fetches portfolio holdings for each fund in the target universe and upserts
into de_mf_holdings with ON CONFLICT (mstar_id, as_of_date, isin) DO UPDATE.

ISIN resolution: attempts to link each holding ISIN to de_instrument.id.
Unresolved ISINs are stored with instrument_id = NULL (allowed).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.holdings import DeMfHoldings
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded
from app.pipelines.morningstar.fund_master import load_target_universe
from app.pipelines.morningstar.isin_resolver import resolve_isin_batch

logger = get_logger(__name__)

# Datapoints for holdings (Morningstar returns portfolio holdings as structured list)
HOLDINGS_DATAPOINTS: list[str] = [
    "Holdings",
    "HoldingDate",
]

# Batch size for DB upserts
UPSERT_BATCH_SIZE: int = 500


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal safely via str(). Returns None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def parse_holdings_response(
    mstar_id: str,
    data: dict[str, Any],
    report_date: date,
) -> list[dict[str, Any]]:
    """Parse Morningstar holdings API response into upsert-ready dicts.

    The API returns a list of holding objects under a "Holdings" key.
    Each holding object is expected to have:
      - ExternalId or ISIN: the holding ISIN
      - HoldingName or Name: holding display name
      - Weighting or Weight: portfolio weight %
      - SharesHeld: number of shares
      - MarketValue: market value in fund currency
      - GlobalSectorCode or Sector: sector code

    Args:
        mstar_id: The fund's Morningstar ID.
        data: Raw API response dict.
        report_date: The as_of_date for this holdings snapshot.

    Returns:
        List of dicts ready for pg_insert(DeMfHoldings).values(...).
    """
    raw_holdings = data.get("Holdings") or []
    holding_date_raw = data.get("HoldingDate")

    # Use API-provided holding date if available, else fall back to report_date
    if holding_date_raw:
        try:
            parsed_date = date.fromisoformat(str(holding_date_raw)[:10])
            report_date = parsed_date
        except (ValueError, TypeError):
            logger.warning(
                "holdings_parse_date_fallback",
                mstar_id=mstar_id,
                raw=holding_date_raw,
            )

    if not isinstance(raw_holdings, list):
        logger.warning(
            "holdings_response_unexpected_format",
            mstar_id=mstar_id,
            type=type(raw_holdings).__name__,
        )
        return []

    parsed: list[dict[str, Any]] = []

    for item in raw_holdings:
        if not isinstance(item, dict):
            continue

        # ISIN extraction — try multiple field names
        isin: Optional[str] = (
            item.get("ExternalId")
            or item.get("ISIN")
            or item.get("Isin")
            or None
        )
        if isin:
            isin = str(isin).strip() or None

        holding_name: Optional[str] = (
            item.get("HoldingName")
            or item.get("Name")
            or item.get("SecurityName")
            or None
        )

        weight_raw = item.get("Weighting") or item.get("Weight")
        weight_pct = _safe_decimal(weight_raw)

        shares_raw = item.get("SharesHeld") or item.get("Shares")
        shares_held: Optional[int] = None
        if shares_raw is not None:
            try:
                shares_held = int(float(str(shares_raw)))
            except (ValueError, TypeError):
                pass

        market_value = _safe_decimal(
            item.get("MarketValue") or item.get("Value")
        )

        sector_code: Optional[str] = (
            item.get("GlobalSectorCode")
            or item.get("SectorCode")
            or item.get("Sector")
            or None
        )

        parsed.append(
            {
                "mstar_id": mstar_id,
                "as_of_date": report_date,
                "isin": isin,
                "holding_name": holding_name,
                "weight_pct": weight_pct,
                "shares_held": shares_held,
                "market_value": market_value,
                "sector_code": sector_code,
                "is_mapped": False,  # Updated after ISIN resolution
                "instrument_id": None,  # Filled after resolution
            }
        )

    logger.debug(
        "holdings_parse_complete",
        mstar_id=mstar_id,
        count=len(parsed),
        report_date=report_date.isoformat(),
    )
    return parsed


async def resolve_and_mark(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Resolve ISINs to instrument_ids and set is_mapped flag.

    Updates each row dict in-place with instrument_id and is_mapped fields.
    Returns the same list with updated values.
    """
    isins = [row["isin"] for row in rows if row.get("isin")]
    if not isins:
        return rows

    isin_map = await resolve_isin_batch(session, isins)

    for row in rows:
        isin = row.get("isin")
        if isin and isin in isin_map:
            instrument_id = isin_map[isin]
            row["instrument_id"] = instrument_id
            row["is_mapped"] = instrument_id is not None

    return rows


async def upsert_holdings_batch(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert a batch of holdings rows into de_mf_holdings.

    Uses ON CONFLICT (mstar_id, as_of_date, isin) DO UPDATE.
    Rows with isin=NULL use a partial conflict strategy on (mstar_id, as_of_date, holding_name).

    Returns number of rows upserted.
    """
    if not rows:
        return 0

    # Split into rows with ISIN and rows without
    rows_with_isin = [r for r in rows if r.get("isin")]
    rows_without_isin = [r for r in rows if not r.get("isin")]

    total_upserted = 0

    # Upsert rows that have an ISIN
    for i in range(0, len(rows_with_isin), UPSERT_BATCH_SIZE):
        batch = rows_with_isin[i : i + UPSERT_BATCH_SIZE]
        stmt = pg_insert(DeMfHoldings).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_mf_holdings",
            set_={
                "holding_name": stmt.excluded.holding_name,
                "weight_pct": stmt.excluded.weight_pct,
                "shares_held": stmt.excluded.shares_held,
                "market_value": stmt.excluded.market_value,
                "sector_code": stmt.excluded.sector_code,
                "instrument_id": stmt.excluded.instrument_id,
                "is_mapped": stmt.excluded.is_mapped,
            },
        )
        await session.execute(stmt)
        total_upserted += len(batch)

    # For rows without ISIN, do a simple insert (they won't conflict on the unique constraint
    # since the constraint requires mstar_id + as_of_date + isin and isin cannot be NULL
    # in a unique constraint match). Insert only if not already present.
    for row in rows_without_isin:
        stmt = pg_insert(DeMfHoldings).values([row])
        stmt = stmt.on_conflict_do_nothing()
        await session.execute(stmt)
        total_upserted += 1

    return total_upserted


class HoldingsPipeline(BasePipeline):
    """Monthly pipeline: fetch fund portfolio holdings from Morningstar.

    For each active fund, fetches the holdings list and upserts into
    de_mf_holdings with ISIN-to-instrument_id resolution.

    Does NOT require a trading day (runs on the 1st of each month or on demand).
    """

    pipeline_name = "morningstar_holdings"
    requires_trading_day = False

    def __init__(
        self,
        client: Optional[MorningstarClient] = None,
        max_per_second: int = 5,
        max_per_day: int = 10_000,
        report_date: Optional[date] = None,
    ) -> None:
        self._client = client
        self._max_per_second = max_per_second
        self._max_per_day = max_per_day
        self._report_date = report_date  # Override for backfill; None = use business_date

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch holdings for all active funds and upsert into de_mf_holdings.

        Returns ExecutionResult with rows_processed = total holdings upserted,
        rows_failed = funds with fetch errors.
        """
        mstar_ids = await load_target_universe(session)

        if not mstar_ids:
            logger.warning("holdings_empty_universe")
            return ExecutionResult(rows_processed=0, rows_failed=0)

        effective_date = self._report_date or business_date
        rows_processed = 0
        rows_failed = 0

        client = self._client or MorningstarClient(
            max_per_second=self._max_per_second,
            max_per_day=self._max_per_day,
        )
        use_context_manager = self._client is None

        try:
            if use_context_manager:
                await client.__aenter__()

            for mstar_id in mstar_ids:
                try:
                    data = await client.fetch(
                        id_type="FundId",
                        identifier=mstar_id,
                        datapoints=HOLDINGS_DATAPOINTS,
                    )

                    if not data:
                        logger.debug("holdings_no_data", mstar_id=mstar_id)
                        rows_failed += 1
                        continue

                    parsed_rows = parse_holdings_response(mstar_id, data, effective_date)
                    if not parsed_rows:
                        logger.debug("holdings_empty_parsed", mstar_id=mstar_id)
                        rows_failed += 1
                        continue

                    resolved_rows = await resolve_and_mark(session, parsed_rows)
                    upserted = await upsert_holdings_batch(session, resolved_rows)
                    rows_processed += upserted

                    logger.info(
                        "holdings_fund_complete",
                        mstar_id=mstar_id,
                        holdings_count=upserted,
                    )

                except RateLimitExceeded:
                    logger.error(
                        "holdings_rate_limit_exceeded",
                        processed_so_far=rows_processed,
                    )
                    break

                except Exception as exc:
                    logger.error(
                        "holdings_fund_fetch_error",
                        mstar_id=mstar_id,
                        error=str(exc),
                    )
                    rows_failed += 1

        finally:
            if use_context_manager:
                await client.__aexit__(None, None, None)

        await session.flush()

        logger.info(
            "holdings_execute_complete",
            business_date=business_date.isoformat(),
            effective_date=effective_date.isoformat(),
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
