"""Post-computation QA — 8 output sanity checks after computations complete.

These checks are reporting-only (no gating). Each returns a StepResult.
The main entry point run_post_computation_qa() returns a QAReport.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.qa_types import QAReport, StepResult
from app.logging import get_logger

logger = get_logger(__name__)

# Valid regime values
VALID_REGIMES = {"BULL", "BEAR", "SIDEWAYS", "RECOVERY"}

# Confidence component weights (must match regime.py)
CONFIDENCE_WEIGHTS = {
    "breadth_score": 0.30,
    "momentum_score": 0.25,
    "volume_score": 0.15,
    "global_score": 0.15,
    "fii_score": 0.15,
}


async def check_technicals_populated(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check coverage of de_equity_technical_daily vs de_equity_ohlcv.

    <80% coverage = warning, 0 rows = failed.
    """
    name = "technicals_populated"
    try:
        tech_count_row = await session.execute(
            sa.text(
                "SELECT COUNT(*) FROM de_equity_technical_daily WHERE date = :d"
            ),
            {"d": business_date},
        )
        tech_count: int = tech_count_row.scalar() or 0

        price_count_row = await session.execute(
            sa.text(
                "SELECT COUNT(*) FROM de_equity_ohlcv WHERE date = :d"
            ),
            {"d": business_date},
        )
        price_count: int = price_count_row.scalar() or 0

        details: dict[str, Any] = {
            "technical_rows": tech_count,
            "price_rows": price_count,
        }

        if tech_count == 0:
            return StepResult(
                name=name,
                status="failed",
                message="No rows in de_equity_technical_daily for business_date",
                details=details,
            )

        if price_count == 0:
            coverage_pct = 100.0
        else:
            coverage_pct = (tech_count / price_count) * 100.0

        details["coverage_pct"] = round(coverage_pct, 2)

        if coverage_pct < 80.0:
            return StepResult(
                name=name,
                status="warning",
                message=f"Technical coverage {coverage_pct:.1f}% is below 80% threshold",
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"Technical coverage {coverage_pct:.1f}% ({tech_count}/{price_count} instruments)",
            details=details,
        )

    except Exception as exc:
        logger.error("check_technicals_populated failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_technicals_range(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check SMA50/SMA200 are within 50% deviation of close_adj.

    >5% outliers = warning.
    """
    name = "technicals_range"
    try:
        result = await session.execute(
            sa.text(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(
                        CASE
                            WHEN close_adj > 0
                                 AND (
                                     (sma_50 IS NOT NULL AND ABS(sma_50 - close_adj) / close_adj > 0.50)
                                     OR
                                     (sma_200 IS NOT NULL AND ABS(sma_200 - close_adj) / close_adj > 0.50)
                                 )
                            THEN 1
                            ELSE 0
                        END
                    ) AS outliers
                FROM de_equity_technical_daily
                WHERE date = :d
                """
            ),
            {"d": business_date},
        )
        row = result.fetchone()
        total: int = row.total if row else 0
        outliers: int = row.outliers if row else 0

        details: dict[str, Any] = {
            "total_rows": total,
            "outlier_count": outliers,
        }

        if total == 0:
            return StepResult(
                name=name,
                status="passed",
                message="No rows to validate for technicals range",
                details=details,
            )

        outlier_pct = (outliers / total) * 100.0
        details["outlier_pct"] = round(outlier_pct, 2)

        if outlier_pct > 5.0:
            return StepResult(
                name=name,
                status="warning",
                message=f"{outliers} rows ({outlier_pct:.1f}%) have SMA deviation >50% from close_adj",
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"SMA range check passed — {outliers} outliers out of {total} rows",
            details=details,
        )

    except Exception as exc:
        logger.error("check_technicals_range failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_rs_scores_populated(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check RS scores exist for equity entity_type and benchmark coverage.

    0 rows = failed. Checks rs_composite in [-20, 20].
    """
    name = "rs_scores_populated"
    try:
        count_row = await session.execute(
            sa.text(
                """
                SELECT COUNT(*) FROM de_rs_scores
                WHERE date = :d AND entity_type = 'equity'
                """
            ),
            {"d": business_date},
        )
        total: int = count_row.scalar() or 0

        if total == 0:
            return StepResult(
                name=name,
                status="failed",
                message="No RS score rows for entity_type='equity' on business_date",
                details={"row_count": 0},
            )

        # Check benchmark coverage
        benchmarks_row = await session.execute(
            sa.text(
                """
                SELECT COUNT(DISTINCT vs_benchmark) AS benchmark_count
                FROM de_rs_scores
                WHERE date = :d AND entity_type = 'equity'
                """
            ),
            {"d": business_date},
        )
        benchmark_count: int = benchmarks_row.scalar() or 0

        # Check rs_composite range [-20, 20]
        out_of_range_row = await session.execute(
            sa.text(
                """
                SELECT COUNT(*) FROM de_rs_scores
                WHERE date = :d AND entity_type = 'equity'
                  AND rs_composite IS NOT NULL
                  AND (rs_composite < -20 OR rs_composite > 20)
                """
            ),
            {"d": business_date},
        )
        out_of_range: int = out_of_range_row.scalar() or 0

        details: dict[str, Any] = {
            "row_count": total,
            "benchmark_count": benchmark_count,
            "rs_composite_out_of_range": out_of_range,
        }

        messages = []
        status = "passed"

        if benchmark_count < 3:
            messages.append(f"Only {benchmark_count}/3 expected benchmarks found")
            status = "warning"

        if out_of_range > 0:
            messages.append(f"{out_of_range} rows have rs_composite outside [-20, 20]")
            status = "warning"

        if not messages:
            messages.append(
                f"RS scores populated: {total} rows across {benchmark_count} benchmarks"
            )

        return StepResult(
            name=name,
            status=status,
            message="; ".join(messages),
            details=details,
        )

    except Exception as exc:
        logger.error("check_rs_scores_populated failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_rs_distribution(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check RS composite distribution statistics for NIFTY 50 benchmark.

    Mean should be near 0 (flag if abs > 2.0).
    Stddev should be 1-3 (flag if > 5.0 or < 0.1).
    """
    name = "rs_distribution"
    try:
        result = await session.execute(
            sa.text(
                """
                SELECT
                    AVG(rs_composite)    AS mean_rs,
                    STDDEV(rs_composite) AS stddev_rs,
                    COUNT(*)             AS row_count
                FROM de_rs_scores
                WHERE date = :d
                  AND entity_type = 'equity'
                  AND vs_benchmark = 'NIFTY 50'
                  AND rs_composite IS NOT NULL
                """
            ),
            {"d": business_date},
        )
        row = result.fetchone()
        row_count: int = row.row_count if row else 0

        details: dict[str, Any] = {"row_count": row_count}

        if row_count == 0:
            return StepResult(
                name=name,
                status="warning",
                message="No RS composite data found for NIFTY 50 benchmark on business_date",
                details=details,
            )

        mean_rs: float = float(row.mean_rs) if row.mean_rs is not None else 0.0
        stddev_rs: float = float(row.stddev_rs) if row.stddev_rs is not None else 0.0

        details["mean_rs_composite"] = round(mean_rs, 4)
        details["stddev_rs_composite"] = round(stddev_rs, 4)

        issues = []

        if abs(mean_rs) > 2.0:
            issues.append(f"Mean rs_composite {mean_rs:.4f} deviates from 0 by more than 2.0")

        if stddev_rs > 5.0:
            issues.append(f"Stddev rs_composite {stddev_rs:.4f} is above 5.0 (unusually high)")
        elif stddev_rs < 0.1:
            issues.append(f"Stddev rs_composite {stddev_rs:.4f} is below 0.1 (unusually low)")

        if issues:
            return StepResult(
                name=name,
                status="warning",
                message="; ".join(issues),
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"RS distribution healthy — mean={mean_rs:.4f}, stddev={stddev_rs:.4f}",
            details=details,
        )

    except Exception as exc:
        logger.error("check_rs_distribution failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_breadth_consistency(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check breadth daily data internal consistency.

    advance + decline + unchanged ~= total_stocks (5% tolerance).
    pct_above_200dma in [0, 100]. ad_ratio > 0.
    """
    name = "breadth_consistency"
    try:
        result = await session.execute(
            sa.text(
                """
                SELECT
                    advance,
                    decline,
                    unchanged,
                    total_stocks,
                    ad_ratio,
                    pct_above_200dma,
                    pct_above_50dma
                FROM de_breadth_daily
                WHERE date = :d
                LIMIT 1
                """
            ),
            {"d": business_date},
        )
        row = result.fetchone()

        if row is None:
            return StepResult(
                name=name,
                status="warning",
                message="No breadth data found for business_date",
                details={"date": str(business_date)},
            )

        advance = int(row.advance or 0)
        decline = int(row.decline or 0)
        unchanged = int(row.unchanged or 0)
        total_stocks = int(row.total_stocks or 0)
        ad_ratio = float(row.ad_ratio or 0.0)
        pct_above_200dma = float(row.pct_above_200dma) if row.pct_above_200dma is not None else None
        pct_above_50dma = float(row.pct_above_50dma) if row.pct_above_50dma is not None else None

        details: dict[str, Any] = {
            "advance": advance,
            "decline": decline,
            "unchanged": unchanged,
            "total_stocks": total_stocks,
            "ad_ratio": ad_ratio,
            "pct_above_200dma": pct_above_200dma,
            "pct_above_50dma": pct_above_50dma,
        }

        issues = []

        # Check advance + decline + unchanged ~= total_stocks (5% tolerance)
        computed_total = advance + decline + unchanged
        if total_stocks > 0:
            discrepancy_pct = abs(computed_total - total_stocks) / total_stocks
            if discrepancy_pct > 0.05:
                issues.append(
                    f"advance+decline+unchanged={computed_total} differs from "
                    f"total_stocks={total_stocks} by {discrepancy_pct*100:.1f}%"
                )

        # Check pct_above_200dma in [0, 100]
        if pct_above_200dma is not None and not (0.0 <= pct_above_200dma <= 100.0):
            issues.append(f"pct_above_200dma={pct_above_200dma} is outside [0, 100]")

        # Check pct_above_50dma in [0, 100]
        if pct_above_50dma is not None and not (0.0 <= pct_above_50dma <= 100.0):
            issues.append(f"pct_above_50dma={pct_above_50dma} is outside [0, 100]")

        # Check ad_ratio > 0
        if ad_ratio <= 0:
            issues.append(f"ad_ratio={ad_ratio} is not positive")

        if issues:
            return StepResult(
                name=name,
                status="warning",
                message="; ".join(issues),
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message="Breadth data is internally consistent",
            details=details,
        )

    except Exception as exc:
        logger.error("check_breadth_consistency failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_regime_validity(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check market regime row validity for business_date.

    No row = warning (not failed, computation may have skipped).
    """
    name = "regime_validity"
    try:
        result = await session.execute(
            sa.text(
                """
                SELECT
                    regime,
                    confidence,
                    breadth_score,
                    momentum_score,
                    volume_score,
                    global_score,
                    fii_score
                FROM de_market_regime
                WHERE date = :d
                ORDER BY computed_at DESC
                LIMIT 1
                """
            ),
            {"d": business_date},
        )
        row = result.fetchone()

        if row is None:
            return StepResult(
                name=name,
                status="warning",
                message="No market regime row found for business_date (computation may have skipped)",
                details={"date": str(business_date)},
            )

        regime: str = str(row.regime or "")
        confidence: float = float(row.confidence or 0.0)
        breadth_score: float = float(row.breadth_score or 0.0)
        momentum_score: float = float(row.momentum_score or 0.0)
        volume_score: float = float(row.volume_score or 0.0)
        global_score: float = float(row.global_score or 0.0)
        fii_score: float = float(row.fii_score or 0.0)

        details: dict[str, Any] = {
            "regime": regime,
            "confidence": confidence,
            "breadth_score": breadth_score,
            "momentum_score": momentum_score,
            "volume_score": volume_score,
            "global_score": global_score,
            "fii_score": fii_score,
        }

        issues = []

        # Check regime is valid
        if regime not in VALID_REGIMES:
            issues.append(f"Invalid regime value: '{regime}'. Expected one of {sorted(VALID_REGIMES)}")

        # Check confidence in [0, 100]
        if not (0.0 <= confidence <= 100.0):
            issues.append(f"Confidence {confidence} is outside [0, 100]")

        # Check confidence approximately equals weighted sum of components
        expected_confidence = (
            breadth_score * CONFIDENCE_WEIGHTS["breadth_score"]
            + momentum_score * CONFIDENCE_WEIGHTS["momentum_score"]
            + volume_score * CONFIDENCE_WEIGHTS["volume_score"]
            + global_score * CONFIDENCE_WEIGHTS["global_score"]
            + fii_score * CONFIDENCE_WEIGHTS["fii_score"]
        )
        details["expected_confidence"] = round(expected_confidence, 4)

        if abs(confidence - expected_confidence) > 1.0:
            issues.append(
                f"Confidence {confidence:.4f} differs from expected weighted sum "
                f"{expected_confidence:.4f} by more than 1.0"
            )

        if issues:
            return StepResult(
                name=name,
                status="warning",
                message="; ".join(issues),
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"Regime '{regime}' is valid with confidence={confidence:.2f}",
            details=details,
        )

    except Exception as exc:
        logger.error("check_regime_validity failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_fund_derived_coverage(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check de_mf_derived_daily coverage and value ranges.

    0 rows = warning. Checks sharpe_1y, beta_vs_nifty, max_drawdown_1y,
    volatility_1y, coverage_pct ranges.
    """
    name = "fund_derived_coverage"
    try:
        count_row = await session.execute(
            sa.text(
                "SELECT COUNT(*) FROM de_mf_derived_daily WHERE nav_date = :d"
            ),
            {"d": business_date},
        )
        total: int = count_row.scalar() or 0

        if total == 0:
            return StepResult(
                name=name,
                status="warning",
                message="No fund derived rows found for business_date",
                details={"nav_date": str(business_date), "row_count": 0},
            )

        violations_row = await session.execute(
            sa.text(
                """
                SELECT
                    SUM(CASE WHEN sharpe_1y IS NOT NULL
                             AND (sharpe_1y < -5 OR sharpe_1y > 5)
                             THEN 1 ELSE 0 END) AS sharpe_violations,
                    SUM(CASE WHEN beta_vs_nifty IS NOT NULL
                             AND (beta_vs_nifty < -1 OR beta_vs_nifty > 5)
                             THEN 1 ELSE 0 END) AS beta_violations,
                    SUM(CASE WHEN max_drawdown_1y IS NOT NULL
                             AND max_drawdown_1y > 0
                             THEN 1 ELSE 0 END) AS drawdown_violations,
                    SUM(CASE WHEN volatility_1y IS NOT NULL
                             AND volatility_1y < 0
                             THEN 1 ELSE 0 END) AS volatility_violations,
                    SUM(CASE WHEN coverage_pct IS NOT NULL
                             AND (coverage_pct < 0 OR coverage_pct > 100)
                             THEN 1 ELSE 0 END) AS coverage_violations
                FROM de_mf_derived_daily
                WHERE nav_date = :d
                """
            ),
            {"d": business_date},
        )
        vrow = violations_row.fetchone()

        sharpe_v = int(vrow.sharpe_violations or 0) if vrow else 0
        beta_v = int(vrow.beta_violations or 0) if vrow else 0
        drawdown_v = int(vrow.drawdown_violations or 0) if vrow else 0
        volatility_v = int(vrow.volatility_violations or 0) if vrow else 0
        coverage_v = int(vrow.coverage_violations or 0) if vrow else 0
        total_violations = sharpe_v + beta_v + drawdown_v + volatility_v + coverage_v

        details: dict[str, Any] = {
            "row_count": total,
            "sharpe_1y_violations": sharpe_v,
            "beta_vs_nifty_violations": beta_v,
            "max_drawdown_1y_violations": drawdown_v,
            "volatility_1y_violations": volatility_v,
            "coverage_pct_violations": coverage_v,
            "total_violations": total_violations,
        }

        if total_violations > 0:
            return StepResult(
                name=name,
                status="warning",
                message=f"{total_violations} range violations found in de_mf_derived_daily",
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"Fund derived data: {total} rows, all range checks passed",
            details=details,
        )

    except Exception as exc:
        logger.error("check_fund_derived_coverage failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def check_cross_table_consistency(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Check entity_id values in de_rs_scores exist in de_instrument.

    Any orphans = warning with count.
    """
    name = "cross_table_consistency"
    try:
        orphan_row = await session.execute(
            sa.text(
                """
                SELECT COUNT(DISTINCT rs.entity_id) AS orphan_count
                FROM de_rs_scores rs
                WHERE rs.date = :d
                  AND rs.entity_type = 'equity'
                  AND NOT EXISTS (
                      SELECT 1 FROM de_instrument inst
                      WHERE inst.id::text = rs.entity_id
                  )
                """
            ),
            {"d": business_date},
        )
        orphan_count: int = orphan_row.scalar() or 0

        total_row = await session.execute(
            sa.text(
                """
                SELECT COUNT(DISTINCT entity_id) FROM de_rs_scores
                WHERE date = :d AND entity_type = 'equity'
                """
            ),
            {"d": business_date},
        )
        total_entities: int = total_row.scalar() or 0

        details: dict[str, Any] = {
            "total_equity_entities": total_entities,
            "orphan_entity_count": orphan_count,
        }

        if orphan_count > 0:
            return StepResult(
                name=name,
                status="warning",
                message=f"{orphan_count} entity_id values in de_rs_scores have no matching de_instrument row",
                details=details,
            )

        return StepResult(
            name=name,
            status="passed",
            message=f"All {total_entities} entity_ids in de_rs_scores matched to de_instrument",
            details=details,
        )

    except Exception as exc:
        logger.error("check_cross_table_consistency failed", error=str(exc), date=str(business_date))
        return StepResult(
            name=name,
            status="failed",
            message=f"Check raised exception: {exc}",
            details={},
        )


async def run_post_computation_qa(
    session: AsyncSession, business_date: date
) -> QAReport:
    """Run all 8 post-computation QA checks sequentially.

    Returns a QAReport with phase='post_qa'. No gating — purely informational.

    Args:
        session: Async SQLAlchemy session.
        business_date: The business date to validate.

    Returns:
        QAReport containing results of all 8 checks.
    """
    report = QAReport(phase="post_qa", business_date=business_date)

    logger.info(
        "Starting post-computation QA",
        phase="post_qa",
        business_date=str(business_date),
    )

    checks = [
        check_technicals_populated,
        check_technicals_range,
        check_rs_scores_populated,
        check_rs_distribution,
        check_breadth_consistency,
        check_regime_validity,
        check_fund_derived_coverage,
        check_cross_table_consistency,
    ]

    for check_fn in checks:
        step = await check_fn(session, business_date)
        report.steps.append(step)
        logger.info(
            "Post-QA step complete",
            check=step.name,
            status=step.status,
            message=step.message,
        )

    logger.info(
        "Post-computation QA complete",
        phase="post_qa",
        business_date=str(business_date),
        overall_status=report.overall_status,
        passed=report.passed,
        warnings=report.warnings,
        failed=report.failed,
    )

    return report
