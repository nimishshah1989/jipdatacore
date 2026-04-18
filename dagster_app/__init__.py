"""JIP Data Engine — Dagster orchestration layer.

Wraps the existing pipeline trigger API as Dagster assets so we get:
  - Per-table freshness SLAs (FreshnessPolicy)
  - Per-table data quality checks (asset_check, including row-count ±5%)
  - Auto-generated lineage graph (which pipeline produces which table)
  - Schedules + retries + alerts (sensor + Telegram webhook)
  - Single dashboard URL: data.jslwealth.in/dagster

The existing pipeline Python code is NOT changed. Each Dagster asset issues
an HTTP POST to the existing /api/v1/pipeline/trigger/single/{name} endpoint
and waits for completion. This keeps the migration fully reversible.
"""
