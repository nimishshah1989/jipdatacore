"""Schedules — derived from registry cron expressions.

Groups assets by (cron_expr, schedule_group) so one schedule fires all
assets that share a cadence. Replaces the bash-cron + jip_trigger.sh stack.
"""

from __future__ import annotations

from collections import defaultdict

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_app.registry import TABLE_SPECS


def build_all_schedules():
    """One schedule per distinct (cron_expr, schedule_group)."""
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for spec in TABLE_SPECS:
        if not spec.cron_expr:
            continue
        groups[(spec.cron_expr, spec.schedule_group)].append(spec.table)

    schedules = []
    for (cron_expr, group_name), tables in groups.items():
        job = define_asset_job(
            name=f"job_{group_name}",
            selection=AssetSelection.assets(*tables),
            description=f"Materialise {len(tables)} asset(s) in group {group_name}",
        )
        schedules.append(
            ScheduleDefinition(
                name=f"schedule_{group_name}",
                cron_schedule=cron_expr,
                execution_timezone="Asia/Kolkata",
                job=job,
                default_status=DefaultScheduleStatus.RUNNING,
                description=f"Schedule for {group_name} ({len(tables)} tables)",
            )
        )
    return schedules
