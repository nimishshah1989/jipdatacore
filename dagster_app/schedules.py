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


def _slug(cron_expr: str) -> str:
    """Convert '33 18 * * 1-5' → '33_18_x_x_15'."""
    return (
        cron_expr.replace(" ", "_")
        .replace("*", "x")
        .replace("/", "s")
        .replace(",", "c")
        .replace("-", "")
    )


def build_all_schedules():
    """One schedule per distinct (cron_expr, schedule_group)."""
    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for spec in TABLE_SPECS:
        if not spec.cron_expr:
            continue
        groups[(spec.cron_expr, spec.schedule_group)].append(spec.table)

    # Count cron variants per group to decide if we need a suffix
    crons_per_group: dict[str, set[str]] = defaultdict(set)
    for cron_expr, group_name in groups:
        crons_per_group[group_name].add(cron_expr)

    schedules = []
    for (cron_expr, group_name), tables in groups.items():
        # Disambiguate when same group has multiple cron expressions
        suffix = f"_{_slug(cron_expr)}" if len(crons_per_group[group_name]) > 1 else ""
        job_name = f"job_{group_name}{suffix}"
        sched_name = f"schedule_{group_name}{suffix}"
        job = define_asset_job(
            name=job_name,
            selection=AssetSelection.assets(*tables),
            description=f"Materialise {len(tables)} asset(s) in group {group_name}",
        )
        schedules.append(
            ScheduleDefinition(
                name=sched_name,
                cron_schedule=cron_expr,
                execution_timezone="Asia/Kolkata",
                job=job,
                default_status=DefaultScheduleStatus.RUNNING,
                description=f"Schedule for {group_name} ({len(tables)} tables) — cron: {cron_expr}",
            )
        )
    return schedules
