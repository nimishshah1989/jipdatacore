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
            # ── Pre-market (07:30 IST, weekdays) ──
            ScheduleEntry(
                name="pre_market",
                cron_expr="30 7 * * 1-5",
                pipelines=["nse_bhav", "nse_corporate_actions", "nse_indices"],
                description="Pre-market: NSE data load",
            ),
            # ── T+1 delivery (09:00 IST, weekdays) ──
            ScheduleEntry(
                name="t1_delivery",
                cron_expr="0 9 * * 1-5",
                pipelines=["fii_dii_flows"],
                description="T+1 delivery: FII/DII flows",
            ),
            # ── EOD ingestion (18:30 IST, weekdays) ──
            ScheduleEntry(
                name="eod",
                cron_expr="30 18 * * 1-5",
                pipelines=[
                    "nse_bhav",
                    "nse_corporate_actions",
                    "nse_indices",
                    "fii_dii_flows",
                    "amfi_nav",
                    "yfinance_global",
                    "fred_macro",
                    "india_vix",
                    "etf_prices",
                    # Atlas daily additions
                    "fo_bhavcopy",
                    "fo_ban_list",
                    "participant_oi",
                    "gsec_yields",
                    "rbi_fx_rates",
                    "insider_trades",
                    "bulk_block_deals",
                ],
                description="End of day: full data refresh",
            ),
            # ── EOD weekend (18:30 IST, Sat-Sun) — global only ──
            ScheduleEntry(
                name="eod_weekend",
                cron_expr="30 18 * * 0,6",
                pipelines=["yfinance_global", "fred_macro"],
                description="Weekend EOD: global markets + macro",
            ),
            # ── Nightly compute chain (19:30 IST, weekdays) ──
            # Triggered after EOD completes; runs the full compute DAG
            ScheduleEntry(
                name="nightly_compute",
                cron_expr="",
                pipelines=[
                    "__validate_ohlcv__",
                    "equity_technicals_sql",
                    "equity_technicals_pandas",
                    "relative_strength",
                    "market_breadth",
                    "mf_derived",
                    "etf_technicals",
                    "etf_rs",
                    "global_technicals",
                    "global_rs",
                    "full_runner",
                    "__goldilocks_compute__",
                ],
                description="Nightly compute: technicals → RS → breadth → derived → goldilocks",
                trigger_after="eod",
            ),
            # ── F&O summary (20:00 IST, weekdays) ──
            ScheduleEntry(
                name="fo_summary",
                cron_expr="0 20 * * 1-5",
                pipelines=["fo_summary"],
                description="F&O summary: PCR, OI, FII positions",
            ),
            # ── RBI policy rates (daily 09:15 IST — poll for MPC changes) ──
            ScheduleEntry(
                name="macro_daily",
                cron_expr="15 9 * * *",
                pipelines=["rbi_policy_rates"],
                description="RBI policy rates: daily poll for repo/CRR/SLR changes",
            ),
            # ── Shareholding pattern filings (daily 21:00 IST) ──
            ScheduleEntry(
                name="filings_daily",
                cron_expr="0 21 * * *",
                pipelines=["shareholding_pattern"],
                description="Shareholding pattern: daily poll for quarterly filings",
            ),
            # ── Reconciliation (23:00 IST, daily) ──
            ScheduleEntry(
                name="reconciliation",
                cron_expr="0 23 * * *",
                pipelines=["__reconciliation__"],
                description="Cross-source data reconciliation",
            ),
            # ── Qualitative (every 30 min) ──
            ScheduleEntry(
                name="qualitative",
                cron_expr="*/30 * * * *",
                pipelines=["qualitative_rss"],
                description="Qualitative: RSS feed ingestion",
            ),
            # ── Full RS rebuild (Sunday 02:00 IST) ──
            ScheduleEntry(
                name="full_rs_rebuild",
                cron_expr="0 2 * * 0",
                pipelines=["relative_strength"],
                description="Full RS historical rebuild (Sunday)",
            ),
            # ── Morningstar weekly (Sunday 04:00 IST) ──
            ScheduleEntry(
                name="morningstar_weekly",
                cron_expr="0 4 * * 0",
                pipelines=["morningstar_nav", "morningstar_portfolio"],
                description="Morningstar weekly data refresh",
            ),
            # ── Holdings monthly (1st of month 03:00 IST) ──
            ScheduleEntry(
                name="holdings_monthly",
                cron_expr="0 3 1 * *",
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
