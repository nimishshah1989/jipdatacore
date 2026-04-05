"""MF EOD orchestrator pipeline — extends BasePipeline."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult
from app.pipelines.mf.amfi import (
    build_amfi_code_to_mstar_map,
    compute_checksum,
    fetch_amfi_content,
    filter_universe,
    parse_amfi_nav_content,
    upsert_nav_rows,
    validate_freshness,
)
from app.pipelines.mf.lifecycle import run_lifecycle_check
from app.pipelines.mf.returns import compute_returns_for_date
from app.pipelines.validation import AnomalyRecord, apply_data_status

logger = get_logger(__name__)

# NAV spike threshold: abs(nav_change_pct) > 15% → warning anomaly
NAV_SPIKE_THRESHOLD = Decimal("15")
MF_EOD_SLA_HOUR = 22
MF_EOD_SLA_MINUTE = 30


class MfEodPipeline(BasePipeline):
    """MF EOD orchestrator pipeline.

    Orchestration order:
    1. Fetch AMFI NAVAll.txt
    2. Parse and validate freshness
    3. Filter to target universe (equity Growth Regular)
    4. Map amfi_code → mstar_id
    5. Upsert NAV rows
    6. Run lifecycle check (merge/closure detection)
    7. Compute returns (1d through 10y)
    8. Validate (NAV spikes, zero NAV)
    9. Apply data_status (raw → validated/quarantined)

    SLA: complete by 22:30 IST.
    """

    pipeline_name = "mf_eod"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        """Run MF EOD ingestion for business_date."""
        logger.info("mf_eod_execute_start", business_date=business_date.isoformat())

        # Step 1: Fetch AMFI content
        async with httpx.AsyncClient() as client:
            raw_content = await fetch_amfi_content(client)

        content_checksum = compute_checksum(raw_content)
        content_str = raw_content.decode("utf-8", errors="replace")

        # Step 2: Parse
        all_rows = parse_amfi_nav_content(content_str)
        logger.info(
            "mf_eod_parsed",
            total_rows=len(all_rows),
            checksum=content_checksum,
        )

        # Step 3: Freshness validation
        is_valid, reason = validate_freshness(all_rows, business_date)
        if not is_valid:
            logger.warning(
                "mf_eod_freshness_check_failed",
                reason=reason,
                business_date=business_date.isoformat(),
            )
            # Do not abort — still ingest available data, mark partial
            # The validate() step will detect anomalies

        # Step 4: Filter universe
        universe_rows = filter_universe(all_rows)

        # Collect active amfi codes for lifecycle check
        active_amfi_codes = {row.amfi_code for row in all_rows}

        # Step 5: Map amfi_code → mstar_id
        amfi_to_mstar = await build_amfi_code_to_mstar_map(session)

        # Step 6: Upsert NAV rows
        rows_inserted, rows_skipped = await upsert_nav_rows(
            session, universe_rows, amfi_to_mstar, run_log.id
        )

        # Flush before lifecycle and returns
        await session.flush()

        # Step 7: Lifecycle check
        lifecycle_events = await run_lifecycle_check(session, active_amfi_codes, business_date)
        logger.info("mf_eod_lifecycle_events", count=lifecycle_events)

        # Step 8: Returns computation (incremental — only today's mstar_ids)
        inserted_mstar_ids = [
            amfi_to_mstar[row.amfi_code]
            for row in universe_rows
            if row.amfi_code in amfi_to_mstar
        ]
        returns_updated, returns_failed = await compute_returns_for_date(
            session, business_date, inserted_mstar_ids
        )
        logger.info(
            "mf_eod_returns",
            updated=returns_updated,
            failed=returns_failed,
        )

        rows_failed = rows_skipped + returns_failed

        logger.info(
            "mf_eod_execute_complete",
            business_date=business_date.isoformat(),
            rows_inserted=rows_inserted,
            rows_failed=rows_failed,
            lifecycle_events=lifecycle_events,
        )

        return ExecutionResult(
            rows_processed=rows_inserted,
            rows_failed=rows_failed,
        )

    async def validate(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> list[AnomalyRecord]:
        """Post-insert validation for MF NAV data.

        Checks:
        1. NAV spike: abs(nav_change_pct) > 15% → warning severity
        2. Zero NAV: nav <= 0 → critical severity (should not exist due to DB
           constraint, but defensive check)

        Then applies data_status transitions (raw → validated/quarantined).
        """
        from sqlalchemy import select as sa_select

        from app.models.prices import DeMfNavDaily

        anomalies: list[AnomalyRecord] = []
        anomaly_mstar_ids: set[str] = set()

        # Query today's rows inserted by this run
        result = await session.execute(
            sa_select(
                DeMfNavDaily.mstar_id,
                DeMfNavDaily.nav,
                DeMfNavDaily.nav_change_pct,
            ).where(
                DeMfNavDaily.nav_date == business_date,
                DeMfNavDaily.pipeline_run_id == run_log.id,
            )
        )
        rows = result.all()

        for row in rows:
            mstar_id = row.mstar_id
            nav = row.nav
            nav_change_pct = row.nav_change_pct

            # Zero / negative NAV (critical)
            if nav is not None and nav <= Decimal("0"):
                anomalies.append(
                    AnomalyRecord(
                        entity_type="mf",
                        anomaly_type="negative_value",
                        severity="critical",
                        mstar_id=mstar_id,
                        actual_value=str(nav),
                        expected_range="nav > 0",
                    )
                )
                anomaly_mstar_ids.add(mstar_id)

            # NAV spike (warning — severity=high)
            if nav_change_pct is not None:
                abs_change = abs(nav_change_pct)
                if abs_change > NAV_SPIKE_THRESHOLD:
                    anomalies.append(
                        AnomalyRecord(
                            entity_type="mf",
                            anomaly_type="nav_deviation",
                            severity="high",
                            mstar_id=mstar_id,
                            actual_value=str(nav_change_pct),
                            expected_range=f"abs(nav_change_pct) <= {NAV_SPIKE_THRESHOLD}",
                        )
                    )
                    anomaly_mstar_ids.add(mstar_id)

        # Apply data_status transitions
        if rows:
            await apply_data_status(
                session=session,
                table_name="de_mf_nav_daily",
                business_date=business_date,
                pipeline_run_id=run_log.id,
                anomaly_mstar_ids=anomaly_mstar_ids,
            )

        logger.info(
            "mf_eod_validate_complete",
            business_date=business_date.isoformat(),
            anomalies_detected=len(anomalies),
            quarantined_funds=len(anomaly_mstar_ids),
        )
        return anomalies
