"""Dagster Definitions — entry point.

Dagster discovers this module via workspace.yaml. Everything visible in the
UI (assets, checks, schedules, sensors) is registered here.
"""

from __future__ import annotations

import os

from dagster import Definitions

from dagster_app.assets import build_all_assets
from dagster_app.checks import build_all_checks
from dagster_app.resources import DataEngineApi, RdsConnection
from dagster_app.schedules import build_all_schedules
from dagster_app.sensors import alert_on_run_failure


defs = Definitions(
    assets=build_all_assets(),
    asset_checks=build_all_checks(),
    schedules=build_all_schedules(),
    sensors=[alert_on_run_failure],
    resources={
        "api": DataEngineApi(
            base_url=os.environ.get("DATA_ENGINE_BASE_URL", "http://data-engine:8010"),
            api_key=os.environ.get("PIPELINE_API_KEY", ""),
        ),
        "rds": RdsConnection(
            dsn=os.environ.get("DATABASE_URL_SYNC", ""),
        ),
    },
)
