"""FRED macro data pipeline — US Treasury yields, FEDFUNDS, CPI, unemployment, global yields."""

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

# ---------------------------------------------------------------------------
# FRED series metadata: maps ticker -> (human_readable_name, unit, frequency)
# unit: 'percent' | 'index' | 'thousands' | 'billions'
# frequency: 'daily' | 'weekly' | 'monthly' | 'quarterly'
# ---------------------------------------------------------------------------
FRED_SERIES_METADATA: dict[str, tuple[str, str, str]] = {
    # --- US Treasury Yields (daily) ---
    "DGS1MO":  ("US 1-Month Treasury Yield",          "percent", "daily"),
    "DGS3MO":  ("US 3-Month Treasury Yield",          "percent", "daily"),
    "DGS6MO":  ("US 6-Month Treasury Yield",          "percent", "daily"),
    "DGS1":    ("US 1-Year Treasury Yield",           "percent", "daily"),
    "DGS2":    ("US 2-Year Treasury Yield",           "percent", "daily"),
    "DGS3":    ("US 3-Year Treasury Yield",           "percent", "daily"),
    "DGS5":    ("US 5-Year Treasury Yield",           "percent", "daily"),
    "DGS7":    ("US 7-Year Treasury Yield",           "percent", "daily"),
    "DGS10":   ("US 10-Year Treasury Yield",          "percent", "daily"),
    "DGS20":   ("US 20-Year Treasury Yield",          "percent", "daily"),
    "DGS30":   ("US 30-Year Treasury Yield",          "percent", "daily"),

    # --- US Macro (monthly) ---
    "CPIAUCSL": ("US CPI All Urban Consumers",        "index",   "monthly"),
    "CPILFESL": ("US Core CPI (ex Food & Energy)",    "index",   "monthly"),
    "PCEPI":    ("US PCE Price Index",                "index",   "monthly"),
    "PCEPILFE": ("US Core PCE Price Index",           "index",   "monthly"),
    "UNRATE":   ("US Unemployment Rate",              "percent", "monthly"),
    "PAYEMS":   ("US Nonfarm Payrolls",               "thousands","monthly"),
    "ICSA":     ("US Initial Jobless Claims",         "thousands","weekly"),
    "INDPRO":   ("US Industrial Production Index",   "index",   "monthly"),
    "RSAFS":    ("US Retail Sales",                   "millions","monthly"),
    "HOUST":    ("US Housing Starts",                 "thousands","monthly"),
    "UMCSENT":  ("US Consumer Sentiment (UMich)",     "index",   "monthly"),
    "DGORDER":  ("US Durable Goods Orders",           "millions","monthly"),
    "JTSJOL":   ("US Job Openings (JOLTS)",           "thousands","monthly"),
    "PPIFIS":   ("US PPI Final Demand",               "index",   "monthly"),

    # --- US Financial (daily / monthly) ---
    "FEDFUNDS":       ("US Federal Funds Rate",              "percent", "monthly"),
    "T10Y2Y":         ("US 10Y-2Y Treasury Spread",          "percent", "daily"),
    "T10Y3M":         ("US 10Y-3M Treasury Spread",          "percent", "daily"),
    "BAMLH0A0HYM2":   ("US HY OAS Credit Spread",            "percent", "daily"),
    "VIXCLS":         ("CBOE VIX Volatility Index",          "index",   "daily"),

    # --- Global Bond Yields — OECD long-term rates via FRED (monthly) ---
    "IRLTLT01DEM156N": ("Germany 10-Year Government Bond Yield",    "percent", "monthly"),
    "IRLTLT01JPM156N": ("Japan 10-Year Government Bond Yield",      "percent", "monthly"),
    "IRLTLT01GBM156N": ("UK 10-Year Government Bond Yield",         "percent", "monthly"),
    "IRLTLT01FRM156N": ("France 10-Year Government Bond Yield",     "percent", "monthly"),
    "IRLTLT01ITM156N": ("Italy 10-Year Government Bond Yield",      "percent", "monthly"),
    "IRLTLT01CAM156N": ("Canada 10-Year Government Bond Yield",     "percent", "monthly"),
    "IRLTLT01AUM156N": ("Australia 10-Year Government Bond Yield",  "percent", "monthly"),
    "IRLTLT01KRM156N": ("South Korea 10-Year Government Bond Yield","percent", "monthly"),
    "IRLTLT01BRM156N": ("Brazil 10-Year Government Bond Yield",     "percent", "monthly"),
    "IRLTLT01INM156N": ("India 10-Year Government Bond Yield",      "percent", "monthly"),

    # --- Global Macro (quarterly / monthly) ---
    "NYGDPPCAPKDUSA": ("US GDP per Capita (constant 2015 USD)",     "index",   "annual"),
    "NYGDPPCAPKDCHN": ("China GDP per Capita (constant 2015 USD)",  "index",   "annual"),
    "NYGDPPCAPKDJPN": ("Japan GDP per Capita (constant 2015 USD)",  "index",   "annual"),
    "NYGDPPCAPKDDEU": ("Germany GDP per Capita (constant 2015 USD)","index",   "annual"),
    "NYGDPPCAPKDGBR": ("UK GDP per Capita (constant 2015 USD)",     "index",   "annual"),
    "CPALTT01USM657N": ("US CPI Total (YoY growth)",                "percent", "monthly"),
    "CPALTT01DEU657N": ("Germany CPI Total (YoY growth)",           "percent", "monthly"),
    "CPALTT01GBR657N": ("UK CPI Total (YoY growth)",                "percent", "monthly"),
    "CPALTT01JPN657N": ("Japan CPI Total (YoY growth)",             "percent", "monthly"),
    "CPALTT01CNM657N": ("China CPI Total (YoY growth)",             "percent", "monthly"),
    "LRUNTTTTUSM156S": ("US Unemployment Rate (OECD harmonised)",   "percent", "monthly"),
    "LRUNTTTTDEUM156S":("Germany Unemployment Rate (OECD)",         "percent", "monthly"),
    "LRUNTTTTGBM156S": ("UK Unemployment Rate (OECD)",              "percent", "monthly"),
    "LRUNTTTTJPM156S": ("Japan Unemployment Rate (OECD)",           "percent", "monthly"),
}

# FRED series to fetch on each pipeline run (all keys from metadata)
FRED_SERIES: list[str] = list(FRED_SERIES_METADATA.keys())


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

    # Retry up to 3 times with exponential backoff for transient failures
    import asyncio as _aio

    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            response = await client.get(FRED_API_BASE_URL, params=params, timeout=20.0)
            response.raise_for_status()
            data = response.json()
            return data.get("observations", [])
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_exc = exc
            if attempt < 2:
                await _aio.sleep(2 ** attempt)
                continue
            raise
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                last_exc = exc
                await _aio.sleep(2 ** attempt)
                continue
            raise

    raise last_exc  # type: ignore[misc]


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
    """Fetches US and global macro data from FRED API.

    Series fetched (~80 series):
    - US Treasury yield curve (1M to 30Y, daily)
    - US macro indicators: CPI, Core CPI, PCE, Unemployment, Payrolls,
      Initial Claims, Industrial Production, Retail Sales, Housing Starts,
      Consumer Sentiment, Durable Goods, JOLTS, PPI (monthly)
    - US financial: FEDFUNDS, yield spreads (10Y-2Y, 10Y-3M), HY credit
      spread, VIX (daily/monthly)
    - Global 10Y government bond yields for 10 countries via OECD/FRED (monthly)
    - Global GDP per capita and CPI growth for major economies (annual/monthly)
    - Global unemployment rates (monthly)

    Each series returns the latest available observation on or before
    business_date. Missing or empty series are logged as warnings and skipped
    (isolated failure — other series continue).

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
                    continue
                except (httpx.TimeoutException, httpx.ConnectError) as exc:
                    logger.error(
                        "fred_series_network_error",
                        series_id=series_id,
                        error=str(exc),
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
