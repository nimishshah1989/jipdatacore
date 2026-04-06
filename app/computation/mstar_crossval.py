"""Morningstar cross-validation for computed MF risk metrics.

Fetches risk statistics from Morningstar API for a sample of active equity
funds and compares them against our computed values stored in
de_mf_derived_daily, reporting deviations against defined tolerance bands.

Comparison metrics:
  sharpe_1y     vs Morningstar SharpeRatio   (15% tolerance)
  beta_vs_nifty vs Morningstar Beta          (10% tolerance)
  max_drawdown_1y vs Morningstar MaxDrawdown (20% tolerance)
  volatility_1y vs Morningstar StandardDev   (15% tolerance)
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.qa_types import QAReport, StepResult
from app.logging import get_logger
from app.pipelines.morningstar.client import MorningstarClient
from app.pipelines.morningstar.risk import RISK_DATAPOINTS, parse_risk_response

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Metric comparison configuration
# ---------------------------------------------------------------------------

METRIC_COMPARISONS: list[dict[str, Any]] = [
    {
        "our_col": "sharpe_1y",
        "mstar_key": "sharpe_ratio",
        "tolerance_pct": Decimal("15"),
    },
    {
        "our_col": "beta_vs_nifty",
        "mstar_key": "beta",
        "tolerance_pct": Decimal("10"),
    },
    {
        "our_col": "max_drawdown_1y",
        "mstar_key": "max_drawdown",
        "tolerance_pct": Decimal("20"),
    },
    {
        "our_col": "volatility_1y",
        "mstar_key": "std_dev",
        "tolerance_pct": Decimal("15"),
    },
]

_HUNDRED = Decimal("100")
_FIVE = Decimal("5")
_ZERO = Decimal("0")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


async def get_top_funds(session: AsyncSession, n: int = 10) -> list[dict[str, str]]:
    """Return up to *n* active equity (non-index, non-ETF) funds from de_mf_master.

    Ordered by fund_name to give a deterministic, representative sample.
    The primary goal is to have *some* real funds to compare — exact ranking
    by AUM is not required for cross-validation purposes.

    Args:
        session: Active async DB session.
        n: Maximum number of funds to return.

    Returns:
        List of dicts with keys "mstar_id" and "fund_name".
    """
    query = sa.text(
        """
        SELECT mstar_id, fund_name
        FROM de_mf_master
        WHERE is_active = TRUE
          AND broad_category = 'Equity'
          AND is_index_fund = FALSE
          AND is_etf = FALSE
        ORDER BY fund_name
        LIMIT :n
        """
    )
    result = await session.execute(query, {"n": n})
    rows = result.fetchall()
    return [{"mstar_id": row.mstar_id, "fund_name": row.fund_name} for row in rows]


async def fetch_our_metrics(
    session: AsyncSession,
    mstar_id: str,
    business_date: date,
) -> Optional[dict[str, Any]]:
    """Fetch computed risk metrics from de_mf_derived_daily for one fund/date.

    Args:
        session: Active async DB session.
        mstar_id: Morningstar fund identifier.
        business_date: The nav_date to query.

    Returns:
        Dict of column name → value (Decimal or None), or None if no row found.
    """
    query = sa.text(
        """
        SELECT
            sharpe_1y,
            beta_vs_nifty,
            max_drawdown_1y,
            volatility_1y
        FROM de_mf_derived_daily
        WHERE mstar_id = :mstar_id
          AND nav_date = :nav_date
        LIMIT 1
        """
    )
    result = await session.execute(
        query, {"mstar_id": mstar_id, "nav_date": business_date}
    )
    row = result.fetchone()
    if row is None:
        return None
    return {
        "sharpe_1y": row.sharpe_1y,
        "beta_vs_nifty": row.beta_vs_nifty,
        "max_drawdown_1y": row.max_drawdown_1y,
        "volatility_1y": row.volatility_1y,
    }


# ---------------------------------------------------------------------------
# Morningstar fetch helper
# ---------------------------------------------------------------------------


async def fetch_mstar_metrics(
    client: MorningstarClient,
    mstar_id: str,
) -> Optional[dict[str, Any]]:
    """Fetch and parse risk metrics from Morningstar for one fund.

    Args:
        client: Initialised MorningstarClient (already in async context).
        mstar_id: Morningstar fund identifier.

    Returns:
        Dict with canonical keys (sharpe_ratio, beta, max_drawdown, std_dev, …),
        or None if the API returned no data or parsing failed.
    """
    data = await client.fetch(
        id_type="FundId",
        identifier=mstar_id,
        datapoints=RISK_DATAPOINTS,
    )
    if not data:
        logger.debug("mstar_crossval_no_data", mstar_id=mstar_id)
        return None

    parsed = parse_risk_response(mstar_id, data)
    if parsed is None:
        logger.debug("mstar_crossval_parse_failed", mstar_id=mstar_id)
    return parsed


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------


def _safe_decimal(value: Any) -> Optional[Decimal]:
    """Convert *value* to Decimal via str() without raising."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def compare_metrics(
    our_data: dict[str, Any],
    mstar_data: dict[str, Any],
    fund_name: str,
    mstar_id: str,
) -> list[dict[str, Any]]:
    """Compare our computed metrics against Morningstar values for one fund.

    For each metric in METRIC_COMPARISONS:
      - "match"           if abs deviation < 5%
      - "within_tolerance" if 5% <= deviation <= tolerance_pct
      - "breach"          if deviation > tolerance_pct
      - "missing_ours"    if our value is absent
      - "missing_mstar"   if Morningstar value is absent
      - "both_missing"    if neither value is present

    Division-by-zero (zero Morningstar denominator) is handled gracefully:
    those comparisons are marked "mstar_zero".

    Args:
        our_data: Dict from fetch_our_metrics (column → value).
        mstar_data: Dict from fetch_mstar_metrics (canonical key → Decimal).
        fund_name: Display name (for logging).
        mstar_id: Morningstar fund identifier.

    Returns:
        List of comparison dicts, one per metric in METRIC_COMPARISONS.
    """
    comparisons: list[dict[str, Any]] = []

    for metric in METRIC_COMPARISONS:
        our_col: str = metric["our_col"]
        mstar_key: str = metric["mstar_key"]
        tolerance: Decimal = metric["tolerance_pct"]

        our_value = _safe_decimal(our_data.get(our_col))
        mstar_value = _safe_decimal(mstar_data.get(mstar_key))

        record: dict[str, Any] = {
            "mstar_id": mstar_id,
            "fund_name": fund_name,
            "metric": our_col,
            "our_value": str(our_value) if our_value is not None else None,
            "mstar_value": str(mstar_value) if mstar_value is not None else None,
            "tolerance_pct": str(tolerance),
            "deviation_pct": None,
            "status": "unknown",
        }

        if our_value is None and mstar_value is None:
            record["status"] = "both_missing"
        elif our_value is None:
            record["status"] = "missing_ours"
        elif mstar_value is None:
            record["status"] = "missing_mstar"
        else:
            # Both values present — compute relative deviation
            abs_mstar = abs(mstar_value)
            if abs_mstar == _ZERO:
                record["status"] = "mstar_zero"
            else:
                deviation = abs(our_value - mstar_value) / abs_mstar * _HUNDRED
                record["deviation_pct"] = str(deviation.quantize(Decimal("0.0001")))

                if deviation < _FIVE:
                    record["status"] = "match"
                elif deviation <= tolerance:
                    record["status"] = "within_tolerance"
                else:
                    record["status"] = "breach"
                    logger.warning(
                        "mstar_crossval_breach",
                        mstar_id=mstar_id,
                        fund_name=fund_name,
                        metric=our_col,
                        our_value=str(our_value),
                        mstar_value=str(mstar_value),
                        deviation_pct=str(deviation.quantize(Decimal("0.01"))),
                        tolerance_pct=str(tolerance),
                    )

        comparisons.append(record)

    return comparisons


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_mstar_crossvalidation(
    session: AsyncSession,
    business_date: date,
) -> QAReport:
    """Run Morningstar cross-validation for the given business date.

    Fetches risk metrics from Morningstar for up to 10 active equity funds and
    compares them against our computed values in de_mf_derived_daily. Results
    are packaged into a QAReport with one StepResult per fund plus a rollup
    StepResult with all comparisons in details["comparisons"].

    If Morningstar credentials are not configured the entire report is marked
    "skipped" with a clear message — this is not treated as a failure.

    Args:
        session: Active async DB session.
        business_date: The date to query our derived metrics for.

    Returns:
        QAReport with phase="mstar_xval".
    """
    report = QAReport(phase="mstar_xval", business_date=business_date)

    # 1. Retrieve fund sample
    funds = await get_top_funds(session, n=10)
    if not funds:
        report.add_step(
            StepResult(
                name="get_top_funds",
                status="warn",
                message="No active equity funds found in de_mf_master",
            )
        )
        return report

    logger.info(
        "mstar_crossval_start",
        business_date=business_date.isoformat(),
        fund_count=len(funds),
    )

    # 2. Run comparisons inside the Morningstar client context
    all_comparisons: list[dict[str, Any]] = []
    total_metrics = 0
    total_breaches = 0

    try:
        async with MorningstarClient() as client:
            # Quick probe: if credentials absent, the first fund returns {}
            # but we detect it via the empty return before any real comparison.
            # We probe explicitly to short-circuit cleanly.
            probe_fund = funds[0]
            probe_data = await client.fetch(
                id_type="FundId",
                identifier=probe_fund["mstar_id"],
                datapoints=RISK_DATAPOINTS[:1],
            )
            if not probe_data:
                # Stub / no credentials — mark skipped and return early
                report.add_step(
                    StepResult(
                        name="mstar_credentials_check",
                        status="skipped",
                        message="Morningstar credentials not configured — crossvalidation skipped",
                    )
                )
                report.overall_status = "skipped"
                return report

            # 3. Per-fund comparisons
            for fund in funds:
                mstar_id = fund["mstar_id"]
                fund_name = fund["fund_name"]

                try:
                    our_metrics = await fetch_our_metrics(session, mstar_id, business_date)
                    mstar_metrics = await fetch_mstar_metrics(client, mstar_id)

                    if our_metrics is None:
                        report.add_step(
                            StepResult(
                                name=f"fund_{mstar_id}",
                                status="skipped",
                                message=f"{fund_name}: no row in de_mf_derived_daily for {business_date}",
                            )
                        )
                        logger.debug(
                            "mstar_crossval_no_derived",
                            mstar_id=mstar_id,
                            business_date=business_date.isoformat(),
                        )
                        continue

                    if mstar_metrics is None:
                        report.add_step(
                            StepResult(
                                name=f"fund_{mstar_id}",
                                status="skipped",
                                message=f"{fund_name}: no risk data from Morningstar",
                            )
                        )
                        continue

                    comparisons = compare_metrics(
                        our_metrics, mstar_metrics, fund_name, mstar_id
                    )
                    fund_breaches = sum(
                        1 for c in comparisons if c["status"] == "breach"
                    )
                    fund_metrics = len(comparisons)

                    all_comparisons.extend(comparisons)
                    total_metrics += fund_metrics
                    total_breaches += fund_breaches

                    fund_status = "fail" if fund_breaches > 0 else "pass"
                    report.add_step(
                        StepResult(
                            name=f"fund_{mstar_id}",
                            status=fund_status,
                            message=(
                                f"{fund_name}: {fund_breaches}/{fund_metrics} metrics breached tolerance"
                                if fund_breaches
                                else f"{fund_name}: all {fund_metrics} metrics within tolerance"
                            ),
                            details={"comparisons": comparisons},
                            metric_count=fund_metrics,
                            breach_count=fund_breaches,
                        )
                    )

                except Exception as exc:
                    logger.error(
                        "mstar_crossval_fund_error",
                        mstar_id=mstar_id,
                        fund_name=fund_name,
                        error=str(exc),
                    )
                    report.add_step(
                        StepResult(
                            name=f"fund_{mstar_id}",
                            status="error",
                            message=f"{fund_name}: unexpected error — {exc}",
                        )
                    )

    except Exception as exc:
        logger.error("mstar_crossval_client_error", error=str(exc))
        report.add_step(
            StepResult(
                name="mstar_client",
                status="error",
                message=f"Morningstar client failed: {exc}",
            )
        )
        return report

    # 4. Rollup StepResult with all comparisons
    rollup_status = "fail" if total_breaches > 0 else "pass"
    if total_metrics == 0:
        rollup_status = "skipped"

    report.add_step(
        StepResult(
            name="mstar_crossval_rollup",
            status=rollup_status,
            message=(
                f"Cross-validation complete: {total_breaches}/{total_metrics} metric comparisons breached tolerance"
                if total_metrics
                else "No metrics were comparable (all funds missing data)"
            ),
            details={"comparisons": all_comparisons},
            metric_count=total_metrics,
            breach_count=total_breaches,
        )
    )

    logger.info(
        "mstar_crossval_complete",
        business_date=business_date.isoformat(),
        funds_evaluated=len(funds),
        total_metrics=total_metrics,
        total_breaches=total_breaches,
        overall_status=report.overall_status,
    )

    return report
