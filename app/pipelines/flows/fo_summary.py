"""F&O summary pipeline — fetches NSE option chain and computes PCR, OI, max pain."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.computed import DeFoSummary
from app.models.pipeline import DePipelineLog
from app.pipelines.framework import BasePipeline, ExecutionResult

logger = get_logger(__name__)

NSE_OPTION_CHAIN_URL = "https://www.nseindia.com/api/option-chain-indices"
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/option-chain",
}

FO_SYMBOLS = ["NIFTY", "BANKNIFTY"]


def _safe_decimal(value: Any) -> Decimal | None:
    """Convert a value to Decimal safely."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def compute_pcr(
    total_put_oi: int,
    total_call_oi: int,
    total_put_volume: int,
    total_call_volume: int,
) -> tuple[Decimal | None, Decimal | None]:
    """Compute PCR by OI and PCR by volume.

    pcr_oi = total_put_oi / total_call_oi
    pcr_volume = total_put_volume / total_call_volume

    Returns (pcr_oi, pcr_volume). Returns None for each if denominator is zero.
    """
    pcr_oi: Decimal | None = None
    pcr_volume: Decimal | None = None

    if total_call_oi > 0:
        pcr_oi = Decimal(str(total_put_oi)) / Decimal(str(total_call_oi))

    if total_call_volume > 0:
        pcr_volume = Decimal(str(total_put_volume)) / Decimal(str(total_call_volume))

    return pcr_oi, pcr_volume


def compute_max_pain(strike_oi: dict[Decimal, dict[str, int]]) -> Decimal | None:
    """Compute max pain strike price.

    Max pain = strike where total monetary loss for option buyers
    (equivalently, profit for option sellers) is maximised.

    For each strike S_candidate:
      pain(S_candidate) = sum over all strikes S of:
        call_oi(S) * max(0, S - S_candidate)  [call writers lose when price > strike]
        + put_oi(S) * max(0, S_candidate - S)  [put writers lose when price < strike]

    The max pain point is argmin(pain).

    Args:
        strike_oi: dict mapping strike price -> {"call_oi": int, "put_oi": int}

    Returns max pain strike or None if no strikes available.
    """
    if not strike_oi:
        return None

    strikes = sorted(strike_oi.keys())
    min_pain: Decimal | None = None
    max_pain_strike: Decimal | None = None

    for candidate in strikes:
        pain = Decimal("0")
        for strike, oi_data in strike_oi.items():
            call_oi = oi_data.get("call_oi", 0)
            put_oi = oi_data.get("put_oi", 0)
            pain += Decimal(str(call_oi)) * max(Decimal("0"), strike - candidate)
            pain += Decimal(str(put_oi)) * max(Decimal("0"), candidate - strike)

        if min_pain is None or pain < min_pain:
            min_pain = pain
            max_pain_strike = candidate

    return max_pain_strike


def parse_option_chain(
    data: dict[str, Any],
) -> dict[str, Any]:
    """Parse NSE option chain response.

    Returns a dict with:
        total_call_oi, total_put_oi, total_call_volume, total_put_volume,
        oi_change, pcr_oi, pcr_volume, max_pain, strike_oi
    """
    records: list[dict[str, Any]] = []

    filtered = data.get("filtered", {})
    if filtered:
        records = filtered.get("data", [])
    else:
        records = data.get("records", {}).get("data", [])

    total_call_oi = 0
    total_put_oi = 0
    total_call_volume = 0
    total_put_volume = 0
    total_call_oi_change = 0
    total_put_oi_change = 0

    strike_oi: dict[Decimal, dict[str, int]] = {}

    for record in records:
        strike_raw = record.get("strikePrice")
        if strike_raw is None:
            continue
        strike = _safe_decimal(strike_raw)
        if strike is None:
            continue

        call_data = record.get("CE", {}) or {}
        put_data = record.get("PE", {}) or {}

        call_oi = int(call_data.get("openInterest", 0) or 0)
        put_oi = int(put_data.get("openInterest", 0) or 0)
        call_vol = int(call_data.get("totalTradedVolume", 0) or 0)
        put_vol = int(put_data.get("totalTradedVolume", 0) or 0)
        call_oi_chg = int(call_data.get("changeinOpenInterest", 0) or 0)
        put_oi_chg = int(put_data.get("changeinOpenInterest", 0) or 0)

        total_call_oi += call_oi
        total_put_oi += put_oi
        total_call_volume += call_vol
        total_put_volume += put_vol
        total_call_oi_change += call_oi_chg
        total_put_oi_change += put_oi_chg

        strike_oi[strike] = {"call_oi": call_oi, "put_oi": put_oi}

    pcr_oi, pcr_volume = compute_pcr(
        total_put_oi, total_call_oi, total_put_volume, total_call_volume
    )
    max_pain_strike = compute_max_pain(strike_oi)
    total_oi = total_call_oi + total_put_oi
    oi_change = total_call_oi_change + total_put_oi_change

    return {
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_call_volume": total_call_volume,
        "total_put_volume": total_put_volume,
        "total_oi": total_oi,
        "oi_change": oi_change,
        "pcr_oi": pcr_oi,
        "pcr_volume": pcr_volume,
        "max_pain": max_pain_strike,
    }


async def _fetch_option_chain(
    client: httpx.AsyncClient,
    symbol: str,
) -> dict[str, Any]:
    """Fetch option chain data for a given symbol from NSE."""
    await client.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=15.0)
    url = f"{NSE_OPTION_CHAIN_URL}?symbol={symbol}"
    response = await client.get(url, headers=NSE_HEADERS, timeout=20.0)
    response.raise_for_status()
    return response.json()


async def upsert_fo_summary(
    session: AsyncSession,
    business_date: date,
    summary: dict[str, Any],
) -> None:
    """Upsert F&O summary into de_fo_summary for the given date."""
    stmt = pg_insert(DeFoSummary).values(
        [
            {
                "date": business_date,
                "pcr_oi": summary.get("pcr_oi"),
                "pcr_volume": summary.get("pcr_volume"),
                "total_oi": summary.get("total_oi"),
                "oi_change": summary.get("oi_change"),
                "max_pain": summary.get("max_pain"),
            }
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["date"],
        set_={
            "pcr_oi": stmt.excluded.pcr_oi,
            "pcr_volume": stmt.excluded.pcr_volume,
            "total_oi": stmt.excluded.total_oi,
            "oi_change": stmt.excluded.oi_change,
            "max_pain": stmt.excluded.max_pain,
        },
    )
    await session.execute(stmt)


class FoSummaryPipeline(BasePipeline):
    """Fetches NSE option chain and computes PCR, OI, and max pain.

    Aggregates NIFTY + BANKNIFTY option chains.
    Trigger: End of day.
    SLA: 17:30 IST.
    """

    pipeline_name = "fo_summary"
    requires_trading_day = True
    exchange = "NSE"

    async def execute(
        self,
        business_date: date,
        session: AsyncSession,
        run_log: DePipelineLog,
    ) -> ExecutionResult:
        logger.info(
            "fo_summary_execute_start",
            business_date=business_date.isoformat(),
        )

        # Aggregate metrics across all symbols
        agg_total_oi = 0
        agg_oi_change = 0
        agg_total_call_oi = 0
        agg_total_put_oi = 0
        agg_total_call_volume = 0
        agg_total_put_volume = 0
        max_pain_values: list[Decimal] = []

        symbols_processed = 0
        rows_failed = 0

        async with httpx.AsyncClient() as client:
            for symbol in FO_SYMBOLS:
                try:
                    raw_data = await _fetch_option_chain(client, symbol)
                    parsed = parse_option_chain(raw_data)

                    agg_total_oi += parsed["total_oi"]
                    agg_oi_change += parsed["oi_change"]
                    agg_total_call_oi += parsed["total_call_oi"]
                    agg_total_put_oi += parsed["total_put_oi"]
                    agg_total_call_volume += parsed["total_call_volume"]
                    agg_total_put_volume += parsed["total_put_volume"]

                    if parsed.get("max_pain") is not None:
                        max_pain_values.append(parsed["max_pain"])

                    symbols_processed += 1

                    logger.info(
                        "fo_summary_symbol_parsed",
                        symbol=symbol,
                        pcr_oi=str(parsed.get("pcr_oi")),
                        total_oi=parsed["total_oi"],
                        business_date=business_date.isoformat(),
                    )

                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "fo_summary_symbol_http_error",
                        symbol=symbol,
                        status_code=exc.response.status_code,
                        business_date=business_date.isoformat(),
                    )
                    rows_failed += 1
                    # Continue with other symbols — isolated failure
                    continue

        if symbols_processed == 0:
            return ExecutionResult(rows_processed=0, rows_failed=rows_failed)

        # Compute aggregate PCR
        agg_pcr_oi, agg_pcr_volume = compute_pcr(
            agg_total_put_oi,
            agg_total_call_oi,
            agg_total_put_volume,
            agg_total_call_volume,
        )

        # Use average max pain across symbols if available
        agg_max_pain: Decimal | None = None
        if max_pain_values:
            agg_max_pain = sum(max_pain_values) / Decimal(str(len(max_pain_values)))

        summary = {
            "pcr_oi": agg_pcr_oi,
            "pcr_volume": agg_pcr_volume,
            "total_oi": agg_total_oi,
            "oi_change": agg_oi_change,
            "max_pain": agg_max_pain,
        }

        await upsert_fo_summary(session, business_date, summary)

        logger.info(
            "fo_summary_upserted",
            pcr_oi=str(agg_pcr_oi),
            pcr_volume=str(agg_pcr_volume),
            total_oi=agg_total_oi,
            max_pain=str(agg_max_pain),
            business_date=business_date.isoformat(),
        )

        return ExecutionResult(
            rows_processed=symbols_processed,
            rows_failed=rows_failed,
        )
