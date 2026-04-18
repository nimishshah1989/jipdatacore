"""Asset factory — generates one Dagster asset per TableSpec.

Each asset:
  - Has a FreshnessPolicy derived from cron_expr + max_lag_hours
  - Materializes by calling the existing trigger API for its pipeline
  - Carries metadata (table, date_col, criticality) shown in the UI
"""

from __future__ import annotations

from datetime import timedelta

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    FreshnessPolicy,
    MetadataValue,
    asset,
)

from dagster_app.registry import TABLE_SPECS, TableSpec
from dagster_app.resources import DataEngineApi


def _freshness_policy(spec: TableSpec) -> FreshnessPolicy | None:
    """Build a FreshnessPolicy from cron + lag.

    Skip if cron_expr is empty (purely-triggered pipelines).
    """
    if not spec.cron_expr:
        return None
    return FreshnessPolicy(
        maximum_lag_minutes=spec.max_lag_hours * 60,
        cron_schedule=spec.cron_expr,
        cron_schedule_timezone="Asia/Kolkata",
    )


def _make_asset(spec: TableSpec):
    """Create one Dagster asset for one TableSpec."""

    policy = _freshness_policy(spec)

    @asset(
        name=spec.table,
        group_name=spec.category,
        compute_kind="http_trigger",
        freshness_policy=policy,
        metadata={
            "table": spec.table,
            "pipeline": spec.pipeline,
            "schedule_group": spec.schedule_group,
            "cron_expr": spec.cron_expr or "(triggered)",
            "criticality": spec.criticality,
            "date_col": spec.date_col,
            "max_lag_hours": spec.max_lag_hours,
            "fresh_lag_hours": spec.fresh_lag_hours,
        },
        description=(
            f"Pipeline `{spec.pipeline}` produces table `{spec.table}`. "
            f"Schedule: {spec.cron_expr or 'triggered'} ({spec.schedule_group}). "
            f"SLA: fresh ≤ {spec.fresh_lag_hours}h, max ≤ {spec.max_lag_hours}h. "
            f"Criticality: {spec.criticality}."
        ),
    )
    def _impl(context: AssetExecutionContext, api: DataEngineApi):
        context.log.info(f"Triggering pipeline {spec.pipeline} for table {spec.table}")
        result = api.trigger(spec.pipeline)
        context.log.info(f"Pipeline result: {result}")
        # Surface key fields in the materialization so they show in the UI
        return None

    # Attach run-time metadata after materialization (added in next iteration)
    return _impl


def build_all_assets():
    """Return a list of asset definitions, one per TableSpec."""
    return [_make_asset(s) for s in TABLE_SPECS]
