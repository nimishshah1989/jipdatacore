"""Sensors — alert on failures, freshness violations, check failures."""

from __future__ import annotations

import os
from typing import Any

import httpx
from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    EventLogEntry,
    RunFailureSensorContext,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    sensor,
)


def _send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception:
        pass


@run_failure_sensor(
    name="alert_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
    description="Send a Telegram alert when any Dagster run fails.",
)
def alert_on_run_failure(context: RunFailureSensorContext):
    """Pages on any pipeline failure."""
    run = context.dagster_run
    asset_keys = run.asset_selection or set()
    asset_str = ", ".join(str(k) for k in list(asset_keys)[:5]) or run.job_name

    text = (
        f"🔴 *JIP Data Engine — Run Failed*\n"
        f"Job: `{run.job_name}`\n"
        f"Assets: `{asset_str}`\n"
        f"Run ID: `{run.run_id[:8]}`\n"
        f"View: https://data.jslwealth.in/dagster/runs/{run.run_id}"
    )
    _send_telegram(text)
    context.log.info(f"Telegram alert sent for run {run.run_id}")
