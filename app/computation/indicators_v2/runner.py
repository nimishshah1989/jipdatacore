"""Daily incremental runner for indicators v2.

Called by the nightly compute pipeline. Computes a small date window
(default: last 5 business days to cover weekends/holidays and catch any
retroactive NAV corrections) for every asset class.

GAP-14: MF is now included — GAP-01 bootstrapped purchase_mode, GAP-14
relaxed min_history to 20 days so young funds still get short-window
indicators.

The full historical backfill is handled by scripts/backfill_indicators_v2.py;
this runner is for the cron path only.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.indicators_v2.assets.equity import compute_equity_indicators
from app.computation.indicators_v2.assets.etf import compute_etf_indicators
from app.computation.indicators_v2.assets.global_ import compute_global_indicators
from app.computation.indicators_v2.assets.index_ import compute_index_indicators
from app.computation.indicators_v2.assets.mf import compute_mf_indicators
from app.computation.indicators_v2.engine import CompResult
from app.logging import get_logger

logger = get_logger(__name__)

# Business-day lookback. Covers weekends, trading holidays, and any late-
# arriving data for the last few days.
DAILY_LOOKBACK_DAYS: int = 5


@dataclass
class IndicatorsV2RunReport:
    """Summary of one compute_indicators_v2 nightly run."""

    business_date: date
    asset_results: dict[str, CompResult] = field(default_factory=dict)
    total_rows_written: int = 0
    failed_assets: list[str] = field(default_factory=list)

    @property
    def overall_status(self) -> str:
        if not self.failed_assets:
            return "passed"
        # At least one asset succeeded (recorded in asset_results) AND at least
        # one failed — partial is better than total failure for dashboards.
        if self.asset_results:
            return "partial"
        return "failed"


async def run_indicators_v2_pipeline(
    session: AsyncSession,
    business_date: date,
    pipeline_run_id: int | None = None,
    lookback_days: int = DAILY_LOOKBACK_DAYS,
) -> IndicatorsV2RunReport:
    """Compute indicators v2 for every asset class for the trailing window.

    Called from scripts/cron/jip_trigger.sh via the pipeline trigger API.
    On any asset failure, logs the error and continues to the next asset
    (partial success preferred over total failure). Returns a report the
    caller can record to de_pipeline_log / de_cron_run.

    All 5 asset classes (equity/index/etf/global/mf) run.
    """
    from_date = business_date - timedelta(days=lookback_days)
    to_date = business_date

    report = IndicatorsV2RunReport(business_date=business_date)

    logger.info(
        "indicators_v2_pipeline_start",
        business_date=business_date.isoformat(),
        from_date=from_date.isoformat(),
        lookback_days=lookback_days,
        pipeline_run_id=pipeline_run_id,
    )

    asset_steps = [
        ("equity", compute_equity_indicators),
        ("index", compute_index_indicators),
        ("etf", compute_etf_indicators),
        ("global", compute_global_indicators),
        ("mf", compute_mf_indicators),
    ]

    for asset_name, compute_fn in asset_steps:
        try:
            logger.info("indicators_v2_asset_start", asset=asset_name)
            result = await compute_fn(
                session,
                from_date=from_date,
                to_date=to_date,
            )
            await session.commit()
            report.asset_results[asset_name] = result
            report.total_rows_written += result.rows_written
            logger.info(
                "indicators_v2_asset_done",
                asset=asset_name,
                processed=result.instruments_processed,
                rows=result.rows_written,
                errored=result.instruments_errored,
            )
        except Exception as exc:
            logger.exception(
                "indicators_v2_asset_failed",
                asset=asset_name,
                error=str(exc),
            )
            report.failed_assets.append(asset_name)
            # Rollback the failed asset's session state so the next asset
            # gets a clean session.
            await session.rollback()

    logger.info(
        "indicators_v2_pipeline_complete",
        business_date=business_date.isoformat(),
        overall_status=report.overall_status,
        total_rows=report.total_rows_written,
        failed_assets=report.failed_assets,
    )

    return report


async def _main_async(business_date: date, lookback_days: int) -> int:
    from app.db.session import async_session_factory
    async with async_session_factory() as session:
        report = await run_indicators_v2_pipeline(session, business_date, lookback_days=lookback_days)
    print(f"indicators_v2 complete: status={report.overall_status} rows={report.total_rows_written} failed={report.failed_assets}", flush=True)
    return 0 if not report.failed_assets else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run indicators v2 for a business date")
    parser.add_argument("--date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--lookback-days", type=int, default=DAILY_LOOKBACK_DAYS)
    args = parser.parse_args()
    sys.exit(asyncio.run(_main_async(args.date, args.lookback_days)))
