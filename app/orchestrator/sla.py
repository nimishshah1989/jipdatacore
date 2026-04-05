"""SLA deadline enforcement for JIP Data Engine pipelines."""

from __future__ import annotations


from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DePipelineLog

logger = get_logger(__name__)

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class SLAConfig:
    """SLA configuration for a single pipeline."""

    pipeline_name: str
    # Deadline in IST (hour, minute)
    deadline_hour: int
    deadline_minute: int
    # Severity for breach notification
    severity: str = "warning"  # "warning" | "critical"
    description: str = ""


@dataclass
class SLABreachEvent:
    """Represents a detected SLA breach."""

    pipeline_name: str
    business_date: date
    deadline_ist: datetime
    current_status: str
    severity: str
    detected_at: datetime


class SLAChecker:
    """Check SLA deadlines every 15 minutes; fire alerts on breach.

    SLA check logic:
    - For each configured pipeline, check if it is past its deadline IST
    - If past deadline AND pipeline status is not complete/partial/skipped → sla_breach
    """

    DEFAULT_SLAS: list[SLAConfig] = [
        SLAConfig("nse_bhav", 8, 30, "critical", "NSE BHAV copy must be ingested by 08:30 IST"),
        SLAConfig("nse_indices", 8, 45, "critical", "NSE indices must be ingested by 08:45 IST"),
        SLAConfig("nse_corporate_actions", 9, 30, "warning", "Corporate actions by 09:30 IST"),
        SLAConfig("amfi_nav", 10, 0, "warning", "AMFI NAV by 10:00 IST"),
        SLAConfig("fii_dii_flows", 11, 0, "warning", "FII/DII flows by 11:00 IST"),
        SLAConfig("relative_strength", 20, 0, "warning", "RS computation by 20:00 IST"),
        SLAConfig("regime_detection", 21, 0, "warning", "Regime detection by 21:00 IST"),
        SLAConfig("yfinance_global", 22, 0, "warning", "Global data by 22:00 IST"),
    ]

    def __init__(self, slas: list[SLAConfig] | None = None) -> None:
        self._slas = slas or self.DEFAULT_SLAS

    def _deadline_for_date(self, sla: SLAConfig, business_date: date) -> datetime:
        """Compute the absolute deadline datetime (IST) for a given date."""
        deadline_time = time(sla.deadline_hour, sla.deadline_minute)
        return datetime.combine(business_date, deadline_time, tzinfo=IST)

    async def check(
        self,
        session: AsyncSession,
        business_date: date,
        now: datetime | None = None,
    ) -> list[SLABreachEvent]:
        """Check all SLAs for the given business date.

        Returns a list of SLABreachEvent for any breaches detected.
        Should be called every 15 minutes by a scheduler.
        """
        now_ist = (now or datetime.now(tz=IST)).astimezone(IST)
        breaches: list[SLABreachEvent] = []

        # Fetch latest pipeline statuses for today from de_pipeline_log
        result = await session.execute(
            sa.select(
                DePipelineLog.pipeline_name,
                sa.func.max(DePipelineLog.run_number).label("max_run"),
            )
            .where(DePipelineLog.business_date == business_date)
            .group_by(DePipelineLog.pipeline_name)
        )
        latest_runs = {row.pipeline_name: row.max_run for row in result.fetchall()}

        # Fetch status for the latest run of each pipeline
        pipeline_statuses: dict[str, str] = {}
        if latest_runs:
            status_result = await session.execute(
                sa.select(DePipelineLog.pipeline_name, DePipelineLog.status)
                .where(
                    DePipelineLog.business_date == business_date,
                    sa.tuple_(
                        DePipelineLog.pipeline_name, DePipelineLog.run_number
                    ).in_(
                        [(name, run) for name, run in latest_runs.items()]
                    ),
                )
            )
            for row in status_result.fetchall():
                pipeline_statuses[row.pipeline_name] = row.status

        # Check each SLA
        for sla in self._slas:
            deadline = self._deadline_for_date(sla, business_date)

            # Only check if we're past the deadline
            if now_ist <= deadline:
                continue

            current_status = pipeline_statuses.get(sla.pipeline_name, "not_started")
            ok_statuses = {"success", "partial", "skipped"}

            if current_status not in ok_statuses:
                breach = SLABreachEvent(
                    pipeline_name=sla.pipeline_name,
                    business_date=business_date,
                    deadline_ist=deadline,
                    current_status=current_status,
                    severity=sla.severity,
                    detected_at=now_ist,
                )
                breaches.append(breach)
                logger.warning(
                    "sla_breach_detected",
                    pipeline=sla.pipeline_name,
                    business_date=business_date.isoformat(),
                    deadline=deadline.isoformat(),
                    current_status=current_status,
                    severity=sla.severity,
                )

        return breaches

    def get_sla(self, pipeline_name: str) -> SLAConfig | None:
        """Get SLA config for a specific pipeline."""
        for sla in self._slas:
            if sla.pipeline_name == pipeline_name:
                return sla
        return None
