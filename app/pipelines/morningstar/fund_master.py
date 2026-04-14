"""Morningstar fund master refresh pipeline.

Weekly pipeline that fetches fund detail datapoints for the target universe
(~450-550 equity growth regular funds) and updates de_mf_master with:
  category_name, broad_category, expense_ratio, primary_benchmark,
  fund_name (canonical Morningstar name), inception_date.

On 404: marks fund inactive if last_seen > 30 days ago.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeMfMaster
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded

logger = get_logger(__name__)

# Datapoints fetched for fund master refresh
FUND_MASTER_DATAPOINTS: list[str] = [
    "Name",
    "CategoryName",
    "BroadCategoryGroup",
    "NetExpenseRatio",
    "ManagerName",
    "TotalNetAssets",
    "InceptionDate",
    "Benchmark",
    # IND-C9: Regular (1) vs Direct (2) plan identifier. Drives the MF
    # technical-indicators eligibility filter — only purchase_mode=1 funds
    # with broad_category='Equity' and non-IDCW names are processed.
    "PurchaseMode",
]

# Number of days after which a 404 triggers is_active = False
INACTIVE_THRESHOLD_DAYS: int = 30


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal safely. Returns None on failure.

    Always converts via str() to avoid float precision issues.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _safe_date(value: Any) -> Optional[date]:
    """Parse an ISO date string (YYYY-MM-DD) to date. Returns None on failure."""
    if not value:
        return None
    try:
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    """Coerce a value to int. Returns None on failure or empty string."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_fund_master_response(
    mstar_id: str,
    data: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """Extract and normalise fund master fields from a Morningstar API response.

    Returns a dict suitable for updating de_mf_master, or None if data is empty.

    All financial values are converted via Decimal(str(value)) per convention.
    """
    if not data:
        return None

    return {
        "mstar_id": mstar_id,
        "fund_name": data.get("Name") or None,
        "category_name": data.get("CategoryName") or None,
        "broad_category": data.get("BroadCategoryGroup") or None,
        "expense_ratio": _safe_decimal(data.get("NetExpenseRatio")),
        "primary_benchmark": data.get("Benchmark") or None,
        "inception_date": _safe_date(data.get("InceptionDate")),
        "investment_strategy": data.get("ManagerName") or None,  # reuse field for manager
        "purchase_mode": _safe_int(data.get("PurchaseMode")),
    }


async def load_target_universe(session: AsyncSession) -> list[str]:
    """Load mstar_ids for the active equity fund universe from de_mf_master.

    Returns list of mstar_id strings for funds that are:
    - is_active = True
    - not is_index_fund
    - not is_etf

    This corresponds to the ~450-550 equity growth regular funds universe.
    """
    result = await session.execute(
        select(DeMfMaster.mstar_id).where(
            DeMfMaster.is_active == True,  # noqa: E712
            DeMfMaster.is_index_fund == False,  # noqa: E712
            DeMfMaster.is_etf == False,  # noqa: E712
        )
    )
    mstar_ids = [row[0] for row in result.fetchall()]
    logger.info("fund_master_universe_loaded", count=len(mstar_ids))
    return mstar_ids


async def update_fund_master_row(
    session: AsyncSession,
    fields: dict[str, Any],
) -> None:
    """Apply parsed Morningstar fields to a de_mf_master row.

    Only updates non-None fields to avoid overwriting existing data with nulls.
    """
    mstar_id: str = fields["mstar_id"]
    update_values: dict[str, Any] = {}

    for col in ("fund_name", "category_name", "broad_category",
                "expense_ratio", "primary_benchmark", "inception_date",
                "investment_strategy", "purchase_mode"):
        val = fields.get(col)
        if val is not None:
            update_values[col] = val

    if not update_values:
        logger.debug("fund_master_no_update_fields", mstar_id=mstar_id)
        return

    update_values["updated_at"] = datetime.now(tz=timezone.utc)

    await session.execute(
        update(DeMfMaster)
        .where(DeMfMaster.mstar_id == mstar_id)
        .values(**update_values)
    )
    logger.debug("fund_master_row_updated", mstar_id=mstar_id, fields=list(update_values.keys()))


async def mark_fund_inactive_if_stale(
    session: AsyncSession,
    mstar_id: str,
    threshold_days: int = INACTIVE_THRESHOLD_DAYS,
) -> bool:
    """Mark a fund inactive if its updated_at is older than threshold_days.

    Called when Morningstar returns 404 for a fund.
    Returns True if the fund was marked inactive, False otherwise.
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=threshold_days)

    result = await session.execute(
        select(DeMfMaster.updated_at, DeMfMaster.is_active).where(
            DeMfMaster.mstar_id == mstar_id
        )
    )
    row = result.one_or_none()
    if row is None:
        logger.warning("fund_master_404_unknown_mstar_id", mstar_id=mstar_id)
        return False

    updated_at, is_active = row

    if not is_active:
        logger.debug("fund_master_already_inactive", mstar_id=mstar_id)
        return False

    # updated_at may be timezone-naive from DB; normalise
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)

    if updated_at < cutoff:
        await session.execute(
            update(DeMfMaster)
            .where(DeMfMaster.mstar_id == mstar_id)
            .values(
                is_active=False,
                closure_date=datetime.now(tz=timezone.utc).date(),
                updated_at=datetime.now(tz=timezone.utc),
            )
        )
        logger.info(
            "fund_master_marked_inactive",
            mstar_id=mstar_id,
            last_seen=updated_at.isoformat(),
            threshold_days=threshold_days,
        )
        return True

    logger.info(
        "fund_master_404_recent_fund_kept_active",
        mstar_id=mstar_id,
        last_seen=updated_at.isoformat(),
    )
    return False


class FundMasterPipeline(BasePipeline):
    """Weekly pipeline: refresh fund metadata from Morningstar.

    Iterates over all active non-index/non-ETF funds and updates
    category_name, broad_category, expense_ratio, benchmark, inception_date.

    Does NOT require a trading day (runs weekly on any calendar day).
    """

    pipeline_name = "morningstar_fund_master"
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
        """Fetch fund metadata from Morningstar and update de_mf_master.

        Returns ExecutionResult with rows_processed = funds updated,
        rows_failed = funds with fetch errors.
        """
        mstar_ids = await load_target_universe(session)

        if not mstar_ids:
            logger.warning("fund_master_empty_universe")
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
                        datapoints=FUND_MASTER_DATAPOINTS,
                    )

                    if not data:
                        # 404 or stub mode
                        await mark_fund_inactive_if_stale(session, mstar_id)
                        await session.flush()
                        rows_failed += 1
                        continue

                    fields = parse_fund_master_response(mstar_id, data)
                    if fields:
                        await update_fund_master_row(session, fields)
                        rows_processed += 1
                    else:
                        rows_failed += 1

                except RateLimitExceeded:
                    logger.error(
                        "fund_master_rate_limit_exceeded",
                        processed_so_far=rows_processed,
                    )
                    # Stop iteration — daily cap exhausted
                    break

                except Exception as exc:
                    logger.error(
                        "fund_master_fund_fetch_error",
                        mstar_id=mstar_id,
                        error=str(exc),
                    )
                    rows_failed += 1

        finally:
            if use_context_manager:
                await client.__aexit__(None, None, None)

        await session.flush()

        logger.info(
            "fund_master_execute_complete",
            business_date=business_date.isoformat(),
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
        return ExecutionResult(rows_processed=rows_processed, rows_failed=rows_failed)
