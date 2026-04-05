"""Cron schedule management for JIP Data Engine pipelines (IST timezone)."""

from __future__ import annotations


from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from app.logging import get_logger

logger = get_logger(__name__)

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class ScheduleEntry:
    """A single cron-scheduled pipeline group."""

    name: str
    # Cron expression in IST (minute hour dom month dow)
    cron_expr: str
    pipelines: list[str]
    description: str = ""
    enabled: bool = True
    # If trigger_after is set, this entry is triggered by the named entry completing
    trigger_after: str | None = None


@dataclass
class CronSchedule:
    """Registry of all JIP Data Engine cron schedules.

    All times are IST (Asia/Kolkata, UTC+5:30).
    """

    entries: list[ScheduleEntry] = field(default_factory=list)

    @classmethod
    def default(cls) -> "CronSchedule":
        """Build the canonical schedule for JIP Data Engine."""
        entries = [
            ScheduleEntry(
                name="pre_market",
                cron_expr="30 7 * * 1-5",  # 07:30 IST, weekdays
                pipelines=["nse_bhav", "nse_corporate_actions", "nse_indices"],
                description="Pre-market: NSE data load",
            ),
            ScheduleEntry(
                name="t1_delivery",
                cron_expr="0 9 * * 1-5",  # 09:00 IST, weekdays
                pipelines=["fii_dii_flows"],
                description="T+1 delivery: FII/DII flows",
            ),
            ScheduleEntry(
                name="eod",
                cron_expr="30 18 * * 1-5",  # 18:30 IST, weekdays
                pipelines=[
                    "nse_bhav",
                    "nse_corporate_actions",
                    "nse_indices",
                    "fii_dii_flows",
                    "amfi_nav",
                    "yfinance_global",
                    "fred_macro",
                ],
                description="End of day: full data refresh",
            ),
            ScheduleEntry(
                name="rs_computation",
                cron_expr="",  # Triggered after EOD completes
                pipelines=["relative_strength"],
                description="RS computation: triggered after EOD",
                trigger_after="eod",
            ),
            ScheduleEntry(
                name="regime_computation",
                cron_expr="",  # Triggered after RS completes
                pipelines=["regime_detection"],
                description="Regime detection: triggered after RS",
                trigger_after="rs_computation",
            ),
            ScheduleEntry(
                name="reconciliation",
                cron_expr="0 23 * * *",  # 23:00 IST, daily
                pipelines=["__reconciliation__"],
                description="Cross-source data reconciliation",
            ),
            ScheduleEntry(
                name="qualitative",
                cron_expr="*/30 * * * *",  # Every 30 minutes
                pipelines=["qualitative_rss"],
                description="Qualitative: RSS feed ingestion",
            ),
            ScheduleEntry(
                name="full_rs_rebuild",
                cron_expr="0 2 * * 0",  # Sunday 02:00 IST
                pipelines=["relative_strength"],
                description="Full RS historical rebuild (Sunday)",
            ),
            ScheduleEntry(
                name="morningstar_weekly",
                cron_expr="0 4 * * 0",  # Sunday 04:00 IST
                pipelines=["morningstar_nav", "morningstar_portfolio"],
                description="Morningstar weekly data refresh",
            ),
            ScheduleEntry(
                name="holdings_monthly",
                cron_expr="0 3 1 * *",  # 1st of month 03:00 IST
                pipelines=["morningstar_portfolio"],
                description="Holdings: 1st of month refresh",
            ),
        ]
        return cls(entries=entries)

    def get_entry(self, name: str) -> ScheduleEntry | None:
        """Retrieve a schedule entry by name."""
        for entry in self.entries:
            if entry.name == name:
                return entry
        return None

    def get_triggered_by(self, completed_entry_name: str) -> list[ScheduleEntry]:
        """Return entries that should be triggered when the named entry completes."""
        return [
            e for e in self.entries
            if e.trigger_after == completed_entry_name and e.enabled
        ]

    def get_cron_entries(self) -> list[ScheduleEntry]:
        """Return only entries with actual cron expressions (not triggered)."""
        return [e for e in self.entries if e.cron_expr and e.enabled]

    def next_run_after(self, entry: ScheduleEntry, after: datetime) -> datetime | None:
        """Calculate the next run time after the given datetime (IST).

        Uses simple cron parsing for the supported subset of expressions.
        Returns None for triggered (non-cron) entries.
        """
        if not entry.cron_expr:
            return None

        try:
            from croniter import croniter
            it = croniter(entry.cron_expr, after.astimezone(IST))
            nxt = it.get_next(datetime)
            return nxt.replace(tzinfo=IST)
        except ImportError:
            logger.warning("croniter_not_installed", entry=entry.name)
            return None
        except Exception as exc:
            logger.error("cron_next_run_error", entry=entry.name, error=str(exc))
            return None

    def due_entries(self, at: datetime | None = None) -> list[ScheduleEntry]:
        """Return schedule entries that are due at or before the given time.

        Requires croniter to be installed. Falls back to empty list if unavailable.
        This is a best-effort utility — the actual scheduler should use APScheduler
        or a similar library.
        """
        now = at or datetime.now(tz=IST)
        due: list[ScheduleEntry] = []
        for entry in self.get_cron_entries():
            try:
                from croniter import croniter
                it = croniter(entry.cron_expr, now.astimezone(IST))
                prev = it.get_prev(datetime)
                # Due if previous scheduled time was within the last minute
                delta_seconds = (now.astimezone(IST) - prev.replace(tzinfo=IST)).total_seconds()
                if 0 <= delta_seconds < 60:
                    due.append(entry)
            except Exception:
                pass
        return due
