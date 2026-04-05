"""FRED macro data pipeline — US Treasury yields, FEDFUNDS, CPI, unemployment."""

from __future__ import annotations


from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.models.pipeline import DePipelineLog
from app.models.prices import DeMacroValues
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

FRED_API_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# FRED series to fetch
FRED_SERIES = [
    "DGS10",     # 10-Year Treasury Constant Maturity Rate
    "DGS2",      # 2-Year Treasury Constant Maturity Rate
    "FEDFUNDS",  # Federal Funds Effective Rate
    "T10Y2Y",    # 10-Year minus 2-Year Treasury Yield Spread
    "CPIAUCSL",  # Consumer Price Index for All Urban Consumers
    "UNRATE",    # Civilian Unemployment Rate
]


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal; return None for missing values."""
    if value is None:
        return None
    try:
        s = str(value).strip()
        if s in ("", ".", "N/A"):
            return None
        return Decimal(s)
    except Exception:
        return None


def _get_latest_observation(
    observations: list[dict[str, Any]],
    up_to_date: date,
) -> tuple[date, Decimal] | None:
    """Get the most recent observation on or before the given date.

    FRED may not have data for weekends/holidays; we take the latest available.
    Returns (observation_date, value) or None.
    """
    best: tuple[date, Decimal] | None = None

    for obs in observations:
        obs_date_str = obs.get("date", "")
        value_str = obs.get("value", "")

        try:
            from datetime import datetime as dt

            obs_date = dt.strptime(obs_date_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        if obs_date > up_to_date:
            continue

        value = _safe_decimal(value_str)
        if value is None:
            continue

        if best is None or obs_date > best[0]:
            best = (obs_date, value)

    return best


async def fetch_fred_series(
    client: httpx.AsyncClient,
    series_id: str,
    api_key: str,
    observation_start: str,
    observation_end: str,
) -> list[dict[str, Any]]:
    """Fetch observations for a single FRED series.

    Args:
        client: httpx async client
        series_id: FRED series ID (e.g. "DGS10")
        api_key: FRED API key
        observation_start: ISO date string (YYYY-MM-DD)
        observation_end: ISO date string (YYYY-MM-DD)

    Returns list of observation dicts with 'date' and 'value' keys.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "observation_end": observation_end,
        "sort_order": "asc",
    }
    response = await client.get(FRED_API_BASE_URL, params=params, timeout=20.0)
    response.raise_for_status()
    data = response.json()
    return data.get("observations", [])


async def upsert_macro_values(
    session: AsyncSession,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert macro value rows into de_macro_values.

    Returns (rows_processed, rows_failed).
    """
    if not rows:
        return 0, 0

    stmt = pg_insert(DeMacroValues).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["date", "ticker"],
        set_={"value": stmt.excluded.value},
    )
    await session.execute(stmt)
    return len(rows), 0


class FredPipeline(BasePipeline):
    """Fetches US macro data from FRED API.

    Series fetched: DGS10, DGS2, FEDFUNDS, T10Y2Y, CPIAUCSL, UNRATE.
    Stores the latest available observation for each series on or before
    the business_date.

    FRED_API_KEY must be set in environment / .env file.
    Trigger: Daily (not restricted to trading days).
    SLA: 09:00 IST.
    """

    pipeline_name = "fred_macro"
    requires_trading_day = False  # FRED publishes on US business days
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        settings = get_settings()
        api_key = settings.fred_api_key

        if not api_key:
            raise ValueError(
                "FRED_API_KEY not configured. Set fred_api_key in .env"
            )

        logger.info(
            "fred_execute_start",
            series_count=len(FRED_SERIES),
            business_date=business_date.isoformat(),
        )

        # Fetch last 30 days to handle weekends / FRED lag
        from datetime import timedelta

        obs_start = (business_date - timedelta(days=30)).isoformat()
        obs_end = business_date.isoformat()

        rows: list[dict[str, Any]] = []
        rows_failed = 0

        async with httpx.AsyncClient() as client:
            for series_id in FRED_SERIES:
                try:
                    observations = await fetch_fred_series(
                        client,
                        series_id,
                        api_key,
                        observation_start=obs_start,
                        observation_end=obs_end,
                    )

                    latest = _get_latest_observation(observations, business_date)

                    if latest is None:
                        logger.warning(
                            "fred_series_no_data",
                            series_id=series_id,
                            business_date=business_date.isoformat(),
                        )
                        continue

                    obs_date, value = latest

                    rows.append(
                        {
                            "date": obs_date,
                            "ticker": series_id,
                            "value": value,
                        }
                    )

                    logger.info(
                        "fred_series_fetched",
                        series_id=series_id,
                        obs_date=obs_date.isoformat(),
                        value=str(value),
                    )

                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "fred_series_http_error",
                        series_id=series_id,
                        status_code=exc.response.status_code,
                        business_date=business_date.isoformat(),
                    )
                    rows_failed += 1
                    # Continue with other series — isolated failure
                    continue

        rows_processed, upsert_failed = await upsert_macro_values(session, rows)

        logger.info(
            "fred_upserted",
            rows_processed=rows_processed,
            rows_failed=rows_failed + upsert_failed,
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=rows_processed,
            rows_failed=rows_failed,
        )
