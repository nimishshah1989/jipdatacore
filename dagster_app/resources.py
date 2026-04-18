"""Dagster resources — DB connection + HTTP client to existing trigger API."""

from __future__ import annotations

import os
from contextlib import contextmanager

import httpx
import psycopg2
from dagster import ConfigurableResource


class DataEngineApi(ConfigurableResource):
    """HTTP client to the existing FastAPI service.

    Each Dagster asset materialization calls
        POST {base_url}/api/v1/pipeline/trigger/single/{pipeline_name}
    and returns when the underlying pipeline completes.
    """

    base_url: str = "http://data-engine:8010"
    api_key: str = ""
    timeout_seconds: int = 1800  # 30 min — matches longest pipeline

    def trigger(self, pipeline_name: str, business_date: str | None = None) -> dict:
        """Fire the pipeline synchronously and return the response JSON."""
        url = f"{self.base_url.rstrip('/')}/api/v1/pipeline/trigger/single/{pipeline_name}"
        params = {}
        if business_date:
            params["business_date"] = business_date
        headers = {}
        if self.api_key:
            headers["X-Pipeline-Key"] = self.api_key

        with httpx.Client(timeout=self.timeout_seconds) as client:
            resp = client.post(url, params=params, headers=headers)
            resp.raise_for_status()
            return resp.json()


class RdsConnection(ConfigurableResource):
    """Read-only psycopg2 connection to the data_engine RDS for asset checks.

    Used by row-count / freshness checks. We deliberately use psycopg2 (sync)
    for asset checks because Dagster's check execution is sync.
    """

    dsn: str = ""

    @contextmanager
    def cursor(self):
        dsn = self.dsn or os.environ.get("DATABASE_URL_SYNC", "")
        # Strip SQLAlchemy driver prefix if present
        if dsn.startswith("postgresql+psycopg2://"):
            dsn = dsn.replace("postgresql+psycopg2://", "postgresql://", 1)
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            try:
                yield cur
            finally:
                cur.close()
        finally:
            conn.close()
