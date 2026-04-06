"""Pre-computation QA — 10 data quality checks run BEFORE any computation.

Critical failures (marked is_critical=True) gate computation: if any critical
check fails, the returned QAReport has overall_status="failed" and computation
MUST NOT proceed.

Usage:
    report = await run_pre_computation_qa(session, business_date)
    if report.overall_status == "failed":
        raise RuntimeError("Pre-QA failed — computation halted")
"""

from __future__ import annotations

from datetime import date

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.qa_types import QAReport, StepResult
from app.logging import get_logger

logger = get_logger(__name__)

# Thresholds
OHLCV_CRITICAL = 200
OHLCV_WARNING = 500
MF_NAV_CRITICAL = 200
MF_NAV_WARNING = 300
PRICE_SPIKE_PCT_THRESHOLD = 0.25   # 25% single-day move
PRICE_SPIKE_UNIVERSE_PCT = 0.05    # >5% of universe spiking = fail
ZERO_VOLUME_UNIVERSE_PCT = 0.10    # >10% zero/null volume = warning
HOLDINGS_STALE_DAYS = 90           # holdings older than 90 days = warning
FLOW_STALE_DAYS = 5                # flows older than 5 trading days = warning

BENCHMARKS = ["NIFTY 50", "NIFTY 500", "NIFTY MIDCAP 100"]


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


async def check_ohlcv_coverage(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Count validated OHLCV rows. <1000 = failed (critical), 1000-1500 = warning."""
    step = StepResult(step_name="check_ohlcv_coverage", status="running")
    result = await session.execute(
        sa.text(
            "SELECT COUNT(*) AS cnt FROM de_equity_price_daily"
            " WHERE date = :bdate AND data_status = 'validated'"
        ),
        {"bdate": business_date},
    )
    count = result.scalar_one()
    step.rows_affected = count
    step.details = {"validated_rows": count}
    if count < OHLCV_CRITICAL:
        step.mark_complete("failed")
        step.errors.append(
            f"Only {count} validated OHLCV rows (critical threshold: {OHLCV_CRITICAL})"
        )
    elif count < OHLCV_WARNING:
        step.mark_complete("warning")
        step.errors.append(
            f"{count} validated OHLCV rows — below warning threshold {OHLCV_WARNING}"
        )
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.ohlcv_coverage", count=count, status=step.status, date=str(business_date))
    return step


async def check_no_negative_prices(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check no close/open/high/low < 0. Any negative = failed (critical)."""
    step = StepResult(step_name="check_no_negative_prices", status="running")
    result = await session.execute(
        sa.text(
            "SELECT COUNT(*) AS cnt FROM de_equity_price_daily"
            " WHERE date = :bdate"
            "   AND (close < 0 OR open < 0 OR high < 0 OR low < 0)"
        ),
        {"bdate": business_date},
    )
    count = result.scalar_one()
    step.rows_affected = count
    step.details = {"negative_price_rows": count}
    if count > 0:
        step.mark_complete("failed")
        step.errors.append(
            f"{count} rows have negative price values (close/open/high/low)"
        )
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.no_negative_prices", count=count, status=step.status, date=str(business_date))
    return step


async def check_high_low_consistency(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check high >= low for all rows. Any violation = failed (critical)."""
    step = StepResult(step_name="check_high_low_consistency", status="running")
    result = await session.execute(
        sa.text(
            "SELECT i.symbol FROM de_equity_price_daily p"
            " JOIN de_instrument i ON i.id = p.instrument_id"
            " WHERE p.date = :bdate AND p.high < p.low"
            " LIMIT 50"
        ),
        {"bdate": business_date},
    )
    violating_symbols = [row[0] for row in result.fetchall()]
    count = len(violating_symbols)
    step.rows_affected = count
    step.details = {"violation_count": count, "sample_symbols": violating_symbols[:10]}
    if count > 0:
        step.mark_complete("failed")
        step.errors.append(
            f"{count} rows have high < low (sample: {violating_symbols[:5]})"
        )
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.high_low_consistency", violations=count, status=step.status, date=str(business_date))
    return step


async def check_zero_volume_pct(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Count rows where volume = 0 or NULL. >10% of total = warning."""
    step = StepResult(step_name="check_zero_volume_pct", status="running")
    result = await session.execute(
        sa.text(
            "SELECT"
            "  COUNT(*) FILTER (WHERE volume = 0 OR volume IS NULL) AS zero_vol,"
            "  COUNT(*) AS total"
            " FROM de_equity_price_daily"
            " WHERE date = :bdate"
        ),
        {"bdate": business_date},
    )
    row = result.fetchone()
    zero_vol = row[0] if row else 0
    total = row[1] if row else 0
    pct = (zero_vol / total * 100.0) if total > 0 else 0.0
    step.rows_affected = zero_vol
    step.details = {"zero_volume_count": zero_vol, "total_rows": total, "zero_volume_pct": round(pct, 2)}
    if total > 0 and (zero_vol / total) > ZERO_VOLUME_UNIVERSE_PCT:
        step.mark_complete("warning")
        step.errors.append(
            f"{zero_vol} of {total} rows have zero/null volume ({pct:.1f}%)"
            f" — threshold {ZERO_VOLUME_UNIVERSE_PCT*100:.0f}%"
        )
    else:
        step.mark_complete("passed")
    logger.info(
        "pre_qa.zero_volume_pct",
        zero_vol=zero_vol, total=total, pct=round(pct, 2),
        status=step.status, date=str(business_date),
    )
    return step


async def check_price_spikes(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Flag abs((close - prev_close)/prev_close) > 25%. >5% of universe = failed."""
    step = StepResult(step_name="check_price_spikes", status="running")
    result = await session.execute(
        sa.text(
            "WITH prev AS ("
            "  SELECT DISTINCT ON (instrument_id)"
            "    instrument_id, close AS prev_close"
            "  FROM de_equity_price_daily"
            "  WHERE date < :bdate AND close IS NOT NULL AND close > 0"
            "  ORDER BY instrument_id, date DESC"
            "),"
            "today AS ("
            "  SELECT instrument_id, close"
            "  FROM de_equity_price_daily"
            "  WHERE date = :bdate AND close IS NOT NULL AND close > 0"
            "),"
            "joined AS ("
            "  SELECT"
            "    ABS((t.close - p.prev_close) / p.prev_close) AS chg"
            "  FROM today t"
            "  JOIN prev p ON p.instrument_id = t.instrument_id"
            "),"
            "counts AS ("
            "  SELECT"
            "    COUNT(*) FILTER (WHERE chg > :spike_threshold) AS spike_count,"
            "    COUNT(*) AS total"
            "  FROM joined"
            ")"
            "SELECT spike_count, total FROM counts"
        ),
        {"bdate": business_date, "spike_threshold": PRICE_SPIKE_PCT_THRESHOLD},
    )
    row = result.fetchone()
    spike_count = row[0] if row else 0
    total = row[1] if row else 0
    spike_pct = (spike_count / total * 100.0) if total > 0 else 0.0
    step.rows_affected = spike_count
    step.details = {
        "spike_count": spike_count,
        "universe_size": total,
        "spike_pct": round(spike_pct, 2),
        "spike_threshold_pct": PRICE_SPIKE_PCT_THRESHOLD * 100,
    }
    if total > 0 and (spike_count / total) > PRICE_SPIKE_UNIVERSE_PCT:
        step.mark_complete("failed")
        step.errors.append(
            f"{spike_count} of {total} instruments spiked >25%"
            f" ({spike_pct:.1f}%) — threshold {PRICE_SPIKE_UNIVERSE_PCT*100:.0f}%"
        )
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.price_spikes", spikes=spike_count, total=total, status=step.status, date=str(business_date))
    return step


async def check_benchmark_availability(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check NIFTY 50, NIFTY 500, NIFTY MIDCAP 100 exist in de_index_price_daily."""
    step = StepResult(step_name="check_benchmark_availability", status="running")
    result = await session.execute(
        sa.text(
            "SELECT DISTINCT index_code FROM de_index_prices"
            " WHERE index_code = ANY(:symbols) AND date = :bdate"
        ),
        {"symbols": BENCHMARKS, "bdate": business_date},
    )
    found = {row[0] for row in result.fetchall()}
    missing = [b for b in BENCHMARKS if b not in found]
    step.rows_affected = len(found)
    step.details = {"found": list(found), "missing": missing}
    if missing:
        step.mark_complete("failed")
        step.errors.append(f"Missing benchmark index data for: {missing}")
    else:
        step.mark_complete("passed")
    logger.info(
        "pre_qa.benchmark_availability",
        found=len(found), missing=missing,
        status=step.status, date=str(business_date),
    )
    return step


async def check_mf_nav_coverage(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Count distinct mstar_id in de_mf_nav_daily. <200 = failed, 200-300 = warning."""
    step = StepResult(step_name="check_mf_nav_coverage", status="running")
    result = await session.execute(
        sa.text(
            "SELECT COUNT(DISTINCT mstar_id) AS cnt FROM de_mf_nav_daily"
            " WHERE nav_date = :bdate"
        ),
        {"bdate": business_date},
    )
    count = result.scalar_one()
    step.rows_affected = count
    step.details = {"distinct_funds": count}
    if count < MF_NAV_CRITICAL:
        step.mark_complete("failed")
        step.errors.append(
            f"Only {count} MF NAV records (critical threshold: {MF_NAV_CRITICAL})"
        )
    elif count < MF_NAV_WARNING:
        step.mark_complete("warning")
        step.errors.append(
            f"{count} MF NAV records — below warning threshold {MF_NAV_WARNING}"
        )
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.mf_nav_coverage", count=count, status=step.status, date=str(business_date))
    return step


async def check_mf_nav_non_negative(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check no NAV <= 0 in de_mf_nav_daily. Any = failed (critical)."""
    step = StepResult(step_name="check_mf_nav_non_negative", status="running")
    result = await session.execute(
        sa.text(
            "SELECT COUNT(*) AS cnt FROM de_mf_nav_daily"
            " WHERE nav_date = :bdate AND nav <= 0"
        ),
        {"bdate": business_date},
    )
    count = result.scalar_one()
    step.rows_affected = count
    step.details = {"non_positive_nav_count": count}
    if count > 0:
        step.mark_complete("failed")
        step.errors.append(f"{count} MF NAV records have nav <= 0")
    else:
        step.mark_complete("passed")
    logger.info("pre_qa.mf_nav_non_negative", bad_count=count, status=step.status, date=str(business_date))
    return step


async def check_holdings_freshness(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check MAX(as_of_date) in de_mf_holdings. >90 days before business_date = warning."""
    step = StepResult(step_name="check_holdings_freshness", status="running")
    result = await session.execute(
        sa.text("SELECT MAX(as_of_date) AS latest FROM de_mf_holdings")
    )
    latest = result.scalar_one()
    step.details = {"latest_holdings_date": str(latest) if latest else None}
    if latest is None:
        step.mark_complete("warning")
        step.errors.append("No holdings data found in de_mf_holdings")
    else:
        days_old = (business_date - latest).days
        step.details["days_since_last_holdings"] = days_old
        if days_old > HOLDINGS_STALE_DAYS:
            step.mark_complete("warning")
            step.errors.append(
                f"Holdings data is {days_old} days old (latest: {latest})"
                f" — threshold {HOLDINGS_STALE_DAYS} days"
            )
        else:
            step.mark_complete("passed")
    logger.info("pre_qa.holdings_freshness", latest=str(latest), status=step.status, date=str(business_date))
    return step


async def check_flow_data_availability(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check MAX(date) in de_institutional_flows. >5 trading days before business_date = warning."""
    step = StepResult(step_name="check_flow_data_availability", status="running")
    result = await session.execute(
        sa.text("SELECT MAX(date) AS latest FROM de_institutional_flows")
    )
    latest = result.scalar_one()
    step.details = {"latest_flow_date": str(latest) if latest else None}
    if latest is None:
        step.mark_complete("warning")
        step.errors.append("No flow data found in de_institutional_flows")
    else:
        days_old = (business_date - latest).days
        step.details["days_since_last_flow"] = days_old
        if days_old > FLOW_STALE_DAYS:
            step.mark_complete("warning")
            step.errors.append(
                f"Flow data is {days_old} calendar days old (latest: {latest})"
                f" — threshold {FLOW_STALE_DAYS} days"
            )
        else:
            step.mark_complete("passed")
    logger.info("pre_qa.flow_data_availability", latest=str(latest), status=step.status, date=str(business_date))
    return step


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

CRITICAL_CHECKS = {
    "check_ohlcv_coverage",
    "check_no_negative_prices",
    "check_high_low_consistency",
    "check_benchmark_availability",
    "check_mf_nav_non_negative",
}


async def run_pre_computation_qa(
    session: AsyncSession, business_date: date
) -> QAReport:
    """Run all 10 pre-computation QA checks sequentially.

    Returns a QAReport with phase="pre_qa". If any critical check fails,
    overall_status is "failed" and computation must be halted.

    Args:
        session: SQLAlchemy async session.
        business_date: The trading date being validated.

    Returns:
        QAReport with results of all 10 checks.
    """
    report = QAReport(phase="pre_qa", business_date=business_date)
    logger.info("pre_qa.start", date=str(business_date))

    checks = [
        check_ohlcv_coverage,
        check_no_negative_prices,
        check_high_low_consistency,
        check_zero_volume_pct,
        check_price_spikes,
        check_benchmark_availability,
        check_mf_nav_coverage,
        check_mf_nav_non_negative,
        check_holdings_freshness,
        check_flow_data_availability,
    ]

    for check_fn in checks:
        try:
            step = await check_fn(session, business_date)
        except Exception as exc:
            step = StepResult(step_name=check_fn.__name__, status="running")
            step.mark_complete("failed")
            step.errors.append(f"Unexpected error: {exc}")
            logger.error("pre_qa.check_error", check=check_fn.__name__, error=str(exc))
        report.add_step(step)

    report.mark_complete()
    logger.info(
        "pre_qa.complete",
        date=str(business_date),
        overall_status=report.overall_status,
        passed=report.passed_count,
        warnings=report.warning_count,
        failed=report.failed_count,
        duration_ms=report.duration_ms,
    )
    return report
