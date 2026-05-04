"""Morningstar ETF holdings ingestion pipeline (Atlas-M0).

Sister pipeline to morningstar.holdings: fetches portfolio holdings for the
ETF universe (de_etf_master) from Morningstar Direct and upserts into
de_etf_holdings, keyed by (ticker, instrument_id, as_of_date).

Differences from de_mf_holdings ingestion:
  - Universe is de_etf_master.ticker (not de_mf_master.mstar_id)
  - Output table keyed by ticker (not mstar_id)
  - Weight stored as decimal fraction (0.0512 = 5.12 %), not percentage
  - Holdings without a resolvable ISIN -> instrument_id are SKIPPED, since
    the de_etf_holdings PK requires a non-null instrument_id; unresolved
    holdings are counted as rows_failed for reporting

Refresh cadence: monthly, on the same Morningstar job day that runs
HoldingsPipeline (de_mf_holdings).

Smoke-test caveats called out in the spec — verified on first call,
documented in readiness report:
  - Field names ('Holdings', 'Weighting', 'ExternalId', 'HoldingDate')
  - Top-N vs full holdings disclosure
  - Latest-only vs historical disclosures
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.etf import DeEtfMaster
from app.models.holdings import DeEtfHoldings
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded
from app.pipelines.morningstar.isin_resolver import resolve_isin_batch

logger = get_logger(__name__)

HOLDINGS_DATAPOINTS: list[str] = [
    "Holdings",
    "HoldingDate",
]

UPSERT_BATCH_SIZE: int = 500

# Morningstar weights are returned as percentages (e.g. 5.12 for 5.12 %).
# de_etf_holdings.weight stores the decimal fraction (0.0512), per spec.
WEIGHT_PCT_TO_FRACTION = Decimal("0.01")
WEIGHT_FRACTION_MAX = Decimal("1")


def _safe_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _normalise_weight(raw: Any) -> Optional[Decimal]:
    """Convert Morningstar weight to a 0..1 decimal fraction.

    Morningstar returns weights as percentages (5.12 = 5.12 %). If a value
    > 1 is seen we divide by 100. If <=1 we assume the response is already
    a fraction. Either way the result is clamped to [0, 1] and returned at
    Numeric(8,6) precision.
    """
    val = _safe_decimal(raw)
    if val is None:
        return None
    if val < 0:
        return None
    if val > 1:
        val = val * WEIGHT_PCT_TO_FRACTION
    if val > WEIGHT_FRACTION_MAX:
        val = WEIGHT_FRACTION_MAX
    # Numeric(8,6) -> 6 fractional digits
    return val.quantize(Decimal("0.000001"))


def parse_etf_holdings_response(
    ticker: str,
    data: dict[str, Any],
    fallback_date: date,
) -> tuple[list[dict[str, Any]], date]:
    """Parse a Morningstar holdings payload for an ETF.

    Returns (parsed_rows, effective_as_of_date).

    parsed_rows fields:
      - ticker (echoed from input)
      - isin (str|None)  -- needs resolution to instrument_id before upsert
      - holding_name (str|None) -- diagnostic only, not stored
      - weight (Decimal|None) -- 0..1 fraction
    """
    raw_holdings = data.get("Holdings") or []
    holding_date_raw = data.get("HoldingDate")

    effective_date = fallback_date
    if holding_date_raw:
        try:
            effective_date = date.fromisoformat(str(holding_date_raw)[:10])
        except (ValueError, TypeError):
            logger.warning(
                "etf_holdings_parse_date_fallback",
                ticker=ticker,
                raw=holding_date_raw,
            )

    if not isinstance(raw_holdings, list):
        logger.warning(
            "etf_holdings_response_unexpected_format",
            ticker=ticker,
            type=type(raw_holdings).__name__,
        )
        return [], effective_date

    parsed: list[dict[str, Any]] = []
    for item in raw_holdings:
        if not isinstance(item, dict):
            continue

        isin: Optional[str] = (
            item.get("ExternalId")
            or item.get("ISIN")
            or item.get("Isin")
        )
        if isin:
            isin = str(isin).strip() or None

        weight = _normalise_weight(
            item.get("Weighting") or item.get("Weight")
        )
        if weight is None:
            continue

        holding_name: Optional[str] = (
            item.get("HoldingName")
            or item.get("Name")
            or item.get("SecurityName")
        )

        parsed.append(
            {
                "ticker": ticker,
                "isin": isin,
                "holding_name": holding_name,
                "weight": weight,
            }
        )

    logger.debug(
        "etf_holdings_parse_complete",
        ticker=ticker,
        count=len(parsed),
        as_of_date=effective_date.isoformat(),
    )
    return parsed, effective_date


async def load_etf_universe(session: AsyncSession) -> list[str]:
    """Load active ETF tickers from de_etf_master.

    Filters to is_active = True. Country filter is intentionally omitted —
    the universe currently mixes Indian and US-listed ETFs; the Morningstar
    holdings call works on tickers it knows about and returns 404 (handled
    gracefully) for the rest.
    """
    result = await session.execute(
        select(DeEtfMaster.ticker).where(DeEtfMaster.is_active == True)  # noqa: E712
    )
    tickers = [row[0] for row in result.fetchall()]
    logger.info("etf_holdings_universe_loaded", count=len(tickers))
    return tickers


async def upsert_etf_holdings_batch(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert a batch of ETF holdings into de_etf_holdings.

    Each row must contain: ticker, instrument_id, as_of_date, weight,
    last_disclosed_date. Conflicts on (ticker, instrument_id, as_of_date)
    update the weight + last_disclosed_date so re-runs converge.
    """
    if not rows:
        return 0

    total = 0
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        batch = rows[i : i + UPSERT_BATCH_SIZE]
        stmt = pg_insert(DeEtfHoldings).values(batch)
        stmt = stmt.on_conflict_do_update(
            constraint="pk_de_etf_holdings",
            set_={
                "weight": stmt.excluded.weight,
                "last_disclosed_date": stmt.excluded.last_disclosed_date,
            },
        )
        await session.execute(stmt)
        total += len(batch)
    return total


class EtfHoldingsPipeline(BasePipeline):
    """Monthly pipeline: fetch ETF portfolio holdings from Morningstar.

    For each ETF in de_etf_master, fetches the holdings list and upserts
    into de_etf_holdings. Holdings without an ISIN that resolves to a
    de_instrument row are dropped (the table requires a non-null
    instrument_id).
    """

    pipeline_name = "morningstar_etf_holdings"
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
        self._report_date = report_date

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        tickers = await load_etf_universe(session)
        if not tickers:
            logger.warning("etf_holdings_empty_universe")
            return ExecutionResult(rows_processed=0, rows_failed=0)

        effective_fallback = self._report_date or business_date
        last_disclosed = business_date  # day Morningstar was queried
        rows_processed = 0
        rows_failed = 0
        etfs_with_data = 0
        etfs_no_data = 0

        client = self._client or MorningstarClient(
            max_per_second=self._max_per_second,
            max_per_day=self._max_per_day,
        )
        use_context_manager = self._client is None

        try:
            if use_context_manager:
                await client.__aenter__()

            for ticker in tickers:
                try:
                    # Morningstar accepts ticker via the same fund endpoint
                    # used for MFs; id_type "Ticker" is the documented form.
                    data = await client.fetch(
                        id_type="Ticker",
                        identifier=ticker,
                        datapoints=HOLDINGS_DATAPOINTS,
                    )

                    if not data:
                        logger.debug("etf_holdings_no_data", ticker=ticker)
                        etfs_no_data += 1
                        continue

                    parsed_rows, as_of_date = parse_etf_holdings_response(
                        ticker, data, effective_fallback
                    )
                    if not parsed_rows:
                        logger.debug("etf_holdings_empty_parsed", ticker=ticker)
                        etfs_no_data += 1
                        continue

                    isins = [r["isin"] for r in parsed_rows if r.get("isin")]
                    isin_map = await resolve_isin_batch(session, isins)

                    upsert_rows: list[dict[str, Any]] = []
                    fund_failed = 0
                    for r in parsed_rows:
                        instrument_id = isin_map.get(r["isin"]) if r.get("isin") else None
                        if instrument_id is None:
                            fund_failed += 1
                            continue
                        upsert_rows.append(
                            {
                                "ticker": ticker,
                                "instrument_id": instrument_id,
                                "as_of_date": as_of_date,
                                "weight": r["weight"],
                                "last_disclosed_date": last_disclosed,
                            }
                        )

                    upserted = await upsert_etf_holdings_batch(session, upsert_rows)
                    rows_processed += upserted
                    rows_failed += fund_failed
                    if upserted:
                        etfs_with_data += 1
                    else:
                        etfs_no_data += 1

                    logger.info(
                        "etf_holdings_etf_complete",
                        ticker=ticker,
                        as_of_date=as_of_date.isoformat(),
                        upserted=upserted,
                        unresolved_isins=fund_failed,
                    )

                except RateLimitExceeded:
                    logger.error(
                        "etf_holdings_rate_limit_exceeded",
                        processed_so_far=rows_processed,
                    )
                    break
                except Exception as exc:
                    logger.error(
                        "etf_holdings_etf_fetch_error",
                        ticker=ticker,
                        error=str(exc),
                    )
                    etfs_no_data += 1
        finally:
            if use_context_manager:
                await client.__aexit__(None, None, None)

        await session.flush()

        logger.info(
            "etf_holdings_execute_complete",
            business_date=business_date.isoformat(),
            etfs_total=len(tickers),
            etfs_with_data=etfs_with_data,
            etfs_no_data=etfs_no_data,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
