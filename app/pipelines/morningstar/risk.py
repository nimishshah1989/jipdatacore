"""Morningstar risk statistics pipeline.

Fetches risk/return datapoints (Sharpe, alpha, beta, standard deviation,
max drawdown, return periods) and upserts into de_mf_master (inline columns)
or a dedicated risk stats table if present.

Since de_mf_master does not have dedicated risk columns, this pipeline
stores the risk stats as a structured update to investment_strategy JSON
or falls back to a log-only mode until a risk stats model is added.

Datapoints fetched:
  Alpha, Beta, StandardDeviation, SharpeRatio, MaxDrawdown,
  ReturnM1, ReturnM3, ReturnM6, ReturnM12, ReturnM36, ReturnM60
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeMfMaster
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded
from app.pipelines.morningstar.fund_master import load_target_universe

logger = get_logger(__name__)

# Risk + return datapoints
RISK_DATAPOINTS: list[str] = [
    "Alpha",
    "Beta",
    "StandardDeviation",
    "SharpeRatio",
    "MaxDrawdown",
    "ReturnM1",
    "ReturnM3",
    "ReturnM6",
    "ReturnM12",
    "ReturnM36",
    "ReturnM60",
]

# Mapping from Morningstar field name to display label (for structured storage)
RISK_FIELD_MAP: dict[str, str] = {
    "Alpha": "alpha",
    "Beta": "beta",
    "StandardDeviation": "std_dev",
    "SharpeRatio": "sharpe_ratio",
    "MaxDrawdown": "max_drawdown",
    "ReturnM1": "return_1m",
    "ReturnM3": "return_3m",
    "ReturnM6": "return_6m",
    "ReturnM12": "return_12m",
    "ReturnM36": "return_36m",
    "ReturnM60": "return_60m",
}


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal safely via str(). Returns None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def parse_risk_response(
    mstar_id: str,
    data: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Parse Morningstar risk/return API response.

    Returns a dict of {canonical_key: Decimal} for all parseable values,
    or None if data is empty.

    All values are converted via Decimal(str(value)) per project convention.
    """
    if not data:
        return None

    result: dict[str, Any] = {"mstar_id": mstar_id}
    has_data = False

    for api_key, canonical_key in RISK_FIELD_MAP.items():
        raw_value = data.get(api_key)
        decimal_value = _safe_decimal(raw_value)
        result[canonical_key] = decimal_value
        if decimal_value is not None:
            has_data = True

    if not has_data:
        logger.debug("risk_parse_no_values", mstar_id=mstar_id)
        return None

    return result


def build_risk_json(parsed: dict[str, Any]) -> str:
    """Serialise parsed risk stats to a compact JSON string for storage.

    Only includes non-None values. Uses str() on Decimal for JSON safety.
    """
    import json

    risk_fields = {
        k: str(v)
        for k, v in parsed.items()
        if k != "mstar_id" and v is not None
    }
    return json.dumps(risk_fields, sort_keys=True)


async def upsert_risk_stats(
    session: AsyncSession,
    parsed: dict[str, Any],
) -> None:
    """Store risk stats by updating de_mf_master.investment_strategy with JSON.

    This is the interim storage approach. When a dedicated risk stats table
    (de_mf_risk_stats) is added in a future migration, this function will be
    updated to upsert there instead.

    Args:
        session: Active async database session.
        parsed: Output of parse_risk_response — {canonical_key: Decimal}.
    """
    mstar_id: str = parsed["mstar_id"]
    risk_json = build_risk_json(parsed)

    await session.execute(
        update(DeMfMaster)
        .where(DeMfMaster.mstar_id == mstar_id)
        .values(
            investment_strategy=risk_json,
            updated_at=datetime.now(tz=timezone.utc),
        )
    )
    logger.debug("risk_stats_stored", mstar_id=mstar_id)


class RiskPipeline(BasePipeline):
    """Pipeline: fetch risk statistics from Morningstar and store on de_mf_master.

    Fetches Alpha, Beta, Sharpe, StdDev, MaxDrawdown, and trailing returns
    for each active fund. Stores as JSON in investment_strategy column until
    a dedicated risk stats table is added.

    Does NOT require a trading day.
    """

    pipeline_name = "morningstar_risk"
    requires_trading_day = False

    def __init__(
        self,
        client: Optional[MorningstarClient] = None,
        max_per_second: int = 5,
        max_per_day: int = 10_000,
    ) -> None:
        self._client = client
        self._max_per_second = max_per_second
        self._max_per_day = max_per_day

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Fetch risk stats for all active funds and update de_mf_master.

        Returns ExecutionResult with rows_processed = funds updated,
        rows_failed = funds with fetch/parse errors.
        """
        mstar_ids = await load_target_universe(session)

        if not mstar_ids:
            logger.warning("risk_empty_universe")
            return ExecutionResult(rows_processed=0, rows_failed=0)

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
                        datapoints=RISK_DATAPOINTS,
                    )

                    if not data:
                        logger.debug("risk_no_data", mstar_id=mstar_id)
                        rows_failed += 1
                        continue

                    parsed = parse_risk_response(mstar_id, data)
                    if not parsed:
                        logger.debug("risk_empty_parsed", mstar_id=mstar_id)
                        rows_failed += 1
                        continue

                    await upsert_risk_stats(session, parsed)
                    rows_processed += 1

                except RateLimitExceeded:
                    logger.error(
                        "risk_rate_limit_exceeded",
                        processed_so_far=rows_processed,
                    )
                    break

                except Exception as exc:
                    logger.error(
                        "risk_fund_fetch_error",
                        mstar_id=mstar_id,
                        error=str(exc),
                    )
                    rows_failed += 1

        finally:
            if use_context_manager:
                await client.__aexit__(None, None, None)

        await session.flush()

        logger.info(
            "risk_execute_complete",
            business_date=business_date.isoformat(),
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
