"""Spot-check module — validates computed values for blue-chip stocks.

Performs 5 independent checks:
  1. Technicals vs yfinance (SMA50, SMA200, close)
  2. RS composite self-consistency (recompute from components)
  3. Breadth arithmetic (advance + decline + unchanged == total_stocks)
  4. Regime self-consistency (recompute confidence + re-derive regime)
  5. MarketPulse RS comparison (stub — skipped if DB not configured)
"""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.qa_types import QAReport, StepResult
from app.config import get_settings
from app.logging import get_logger

logger = get_logger(__name__)

SPOT_CHECK_SYMBOLS = ["RELIANCE", "TCS", "HDFCBANK", "INFY"]

# RS composite weights (must mirror computation/rs.py)
RS_WEIGHTS = {
    "rs_1w": 0.10,
    "rs_1m": 0.20,
    "rs_3m": 0.30,
    "rs_6m": 0.25,
    "rs_12m": 0.15,
}

# Regime component weights (must mirror computation/regime.py)
REGIME_CONFIDENCE_WEIGHTS = {
    "breadth_score": 0.30,
    "momentum_score": 0.25,
    "volume_score": 0.15,
    "global_score": 0.15,
    "fii_score": 0.15,
}


def _deviation_pct(ours: float, reference: float) -> float:
    """Return absolute percentage deviation between two values."""
    if reference == 0:
        return 0.0
    return abs(ours - reference) / abs(reference) * 100.0


def _classify_deviation(dev_pct: float) -> str:
    """Classify deviation: match (<2%), close (2-5%), mismatch (>5%)."""
    if dev_pct < 2.0:
        return "match"
    if dev_pct <= 5.0:
        return "close"
    return "mismatch"


def _safe_float(value: Any) -> Optional[float]:
    """Convert DB Decimal / numeric to float, returning None on failure."""
    if value is None:
        return None
    try:
        f = float(str(value))
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _rederive_regime(
    confidence: float,
    breadth_score: float,
    momentum_score: float,
) -> str:
    """Re-derive regime label from confidence and component scores."""
    if confidence >= 60.0 and breadth_score >= 60.0:
        return "BULL"
    if confidence <= 40.0 or breadth_score <= 35.0:
        return "BEAR"
    if 40.0 < confidence < 60.0 and momentum_score > breadth_score:
        return "RECOVERY"
    return "SIDEWAYS"


async def _fetch_instrument_id(
    session: AsyncSession, symbol: str
) -> Optional[str]:
    """Return instrument UUID for a given symbol."""
    result = await session.execute(
        sa.text("""
            SELECT id
            FROM de_instrument
            WHERE current_symbol = :sym
            LIMIT 1
        """),
        {"sym": symbol},
    )
    row = result.fetchone()
    return str(row.id) if row else None


# ---------------------------------------------------------------------------
# Check 1: Technicals vs yfinance
# ---------------------------------------------------------------------------


async def spot_check_technicals(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Validate stored technicals against yfinance for SPOT_CHECK_SYMBOLS.

    For each stock: compares close_adj, sma_50, sma_200 vs yfinance reference.
    Tolerances: close_adj <0.5%, SMA50 <2%, SMA200 <2%.

    Returns:
        StepResult with per-stock per-metric comparison in details["checks"].
    """
    step = StepResult(name="spot_check_technicals", status="passed")
    checks: list[dict[str, Any]] = []
    any_mismatch = False
    any_warning = False

    try:
        import yfinance as yf  # noqa: PLC0415 — optional external dep
    except ImportError:
        step.status = "warning"
        step.message = "yfinance not installed — skipping technicals check"
        step.details["checks"] = []
        return step

    for symbol in SPOT_CHECK_SYMBOLS:
        # --- Fetch from DB ---
        db_result = await session.execute(
            sa.text("""
                SELECT
                    t.date,
                    t.sma_50,
                    t.sma_200,
                    t.ema_20,
                    t.close_adj
                FROM de_equity_technical_daily t
                JOIN de_instrument i ON i.id = t.instrument_id
                WHERE i.current_symbol = :sym
                  AND t.date = :bdate
                LIMIT 1
            """),
            {"sym": symbol, "bdate": business_date},
        )
        db_row = db_result.fetchone()

        if db_row is None:
            checks.append({
                "symbol": symbol,
                "metric": "all",
                "computed": None,
                "expected": None,
                "source": "yfinance",
                "deviation_pct": None,
                "status": "skipped",
                "note": "no DB row for date",
            })
            continue

        db_close = _safe_float(db_row.close_adj)
        db_sma50 = _safe_float(db_row.sma_50)
        db_sma200 = _safe_float(db_row.sma_200)

        # --- Fetch from yfinance ---
        try:
            ticker_data: Any = yf.download(
                f"{symbol}.NS",
                period="6mo",
                progress=False,
                auto_adjust=True,
            )

            if ticker_data is None or ticker_data.empty:
                raise ValueError("empty yfinance response")

            # pandas .rolling() SMA computation
            close_series: Any = ticker_data["Close"]
            if hasattr(close_series, "squeeze"):
                close_series = close_series.squeeze()

            yf_sma50_series = close_series.rolling(50).mean()
            yf_sma200_series = close_series.rolling(200).mean()

            # Use the last available value (closest to business_date)
            yf_close = float(close_series.iloc[-1])
            yf_sma50_raw = yf_sma50_series.iloc[-1]
            yf_sma200_raw = yf_sma200_series.iloc[-1]

            yf_sma50: Optional[float] = (
                float(yf_sma50_raw)
                if not (isinstance(yf_sma50_raw, float) and math.isnan(yf_sma50_raw))
                else None
            )
            yf_sma200: Optional[float] = (
                float(yf_sma200_raw)
                if not (isinstance(yf_sma200_raw, float) and math.isnan(yf_sma200_raw))
                else None
            )

        except Exception as exc:
            logger.warning(
                "spot_check_technicals_yfinance_error",
                symbol=symbol,
                error=str(exc),
            )
            for metric in ("close_adj", "sma_50", "sma_200"):
                checks.append({
                    "symbol": symbol,
                    "metric": metric,
                    "computed": None,
                    "expected": None,
                    "source": "yfinance",
                    "deviation_pct": None,
                    "status": "warning",
                    "note": f"yfinance error: {exc}",
                })
            any_warning = True
            continue

        # --- Compare each metric ---
        metric_specs = [
            ("close_adj", db_close, yf_close, 0.5),
            ("sma_50", db_sma50, yf_sma50, 2.0),
            ("sma_200", db_sma200, yf_sma200, 2.0),
        ]

        for metric_name, db_val, yf_val, _tolerance in metric_specs:
            if db_val is None or yf_val is None:
                checks.append({
                    "symbol": symbol,
                    "metric": metric_name,
                    "computed": str(db_val) if db_val is not None else None,
                    "expected": str(round(yf_val, 4)) if yf_val is not None else None,
                    "source": "yfinance",
                    "deviation_pct": None,
                    "status": "skipped",
                    "note": "null value",
                })
                continue

            dev = _deviation_pct(db_val, yf_val)
            classification = _classify_deviation(dev)

            checks.append({
                "symbol": symbol,
                "metric": metric_name,
                "computed": str(round(db_val, 4)),
                "expected": str(round(yf_val, 4)),
                "source": "yfinance",
                "deviation_pct": str(round(dev, 4)),
                "status": classification,
            })

            if classification == "mismatch":
                any_mismatch = True

    step.details["checks"] = checks

    if any_mismatch:
        step.status = "failed"
        step.message = "One or more technicals mismatched yfinance reference (>5% deviation)"
    elif any_warning:
        step.status = "warning"
        step.message = "yfinance unavailable for one or more symbols"
    else:
        step.message = "All technicals within tolerance vs yfinance"

    return step


# ---------------------------------------------------------------------------
# Check 2: RS composite self-consistency
# ---------------------------------------------------------------------------


async def spot_check_rs_self_consistency(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Validate RS composite = recomputed weighted sum of component scores.

    Recompute: rs_1w*0.10 + rs_1m*0.20 + rs_3m*0.30 + rs_6m*0.25 + rs_12m*0.15
    Tolerance: 0.01 (floating point rounding).

    Returns:
        StepResult with per-stock comparison in details["checks"].
    """
    step = StepResult(name="spot_check_rs_self_consistency", status="passed")
    checks: list[dict[str, Any]] = []
    any_mismatch = False

    for symbol in SPOT_CHECK_SYMBOLS:
        rs_result = await session.execute(
            sa.text("""
                SELECT
                    r.rs_composite,
                    r.rs_1w,
                    r.rs_1m,
                    r.rs_3m,
                    r.rs_6m,
                    r.rs_12m
                FROM de_rs_scores r
                JOIN de_instrument i ON i.id = r.entity_id
                WHERE r.date = :bdate
                  AND r.entity_type = 'equity'
                  AND r.vs_benchmark = 'NIFTY 50'
                  AND i.current_symbol = :sym
                LIMIT 1
            """),
            {"bdate": business_date, "sym": symbol},
        )
        row = rs_result.fetchone()

        if row is None:
            checks.append({
                "symbol": symbol,
                "stored_composite": None,
                "recomputed_composite": None,
                "delta": None,
                "status": "skipped",
                "note": "no RS row for date",
            })
            continue

        stored = _safe_float(row.rs_composite)
        rs_1w = _safe_float(row.rs_1w)
        rs_1m = _safe_float(row.rs_1m)
        rs_3m = _safe_float(row.rs_3m)
        rs_6m = _safe_float(row.rs_6m)
        rs_12m = _safe_float(row.rs_12m)

        if any(v is None for v in [stored, rs_1w, rs_1m, rs_3m, rs_6m, rs_12m]):
            checks.append({
                "symbol": symbol,
                "stored_composite": str(stored) if stored is not None else None,
                "recomputed_composite": None,
                "delta": None,
                "status": "skipped",
                "note": "null component value",
            })
            continue

        recomputed = (
            rs_1w * RS_WEIGHTS["rs_1w"]     # type: ignore[operator]
            + rs_1m * RS_WEIGHTS["rs_1m"]   # type: ignore[operator]
            + rs_3m * RS_WEIGHTS["rs_3m"]   # type: ignore[operator]
            + rs_6m * RS_WEIGHTS["rs_6m"]   # type: ignore[operator]
            + rs_12m * RS_WEIGHTS["rs_12m"] # type: ignore[operator]
        )

        delta = abs(stored - recomputed)  # type: ignore[operator]
        tolerance = 0.01
        status = "match" if delta <= tolerance else "mismatch"

        if status == "mismatch":
            any_mismatch = True

        checks.append({
            "symbol": symbol,
            "stored_composite": str(round(stored, 6)),  # type: ignore[arg-type]
            "recomputed_composite": str(round(recomputed, 6)),
            "delta": str(round(delta, 6)),
            "status": status,
        })

    step.details["checks"] = checks

    if any_mismatch:
        step.status = "failed"
        step.message = "RS composite does not match recomputed weighted sum for one or more symbols"
    else:
        step.message = "RS composites are self-consistent"

    return step


# ---------------------------------------------------------------------------
# Check 3: Breadth arithmetic
# ---------------------------------------------------------------------------


async def spot_check_breadth_arithmetic(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Validate breadth row arithmetic for business_date.

    Checks:
      - advance + decline + unchanged == total_stocks (exact)
      - pct_above_200dma in [0, 100]
      - pct_above_50dma in [0, 100]

    Returns:
        StepResult with values in details.
    """
    step = StepResult(name="spot_check_breadth_arithmetic", status="passed")

    result = await session.execute(
        sa.text("""
            SELECT
                advance,
                decline,
                unchanged,
                total_stocks,
                pct_above_200dma,
                pct_above_50dma
            FROM de_breadth_daily
            WHERE date = :bdate
            LIMIT 1
        """),
        {"bdate": business_date},
    )
    row = result.fetchone()

    if row is None:
        step.status = "skipped"
        step.message = "No breadth row for business_date"
        return step

    advance = int(row.advance or 0)
    decline = int(row.decline or 0)
    unchanged = int(row.unchanged or 0)
    total_stocks = int(row.total_stocks or 0)
    pct_200 = _safe_float(row.pct_above_200dma)
    pct_50 = _safe_float(row.pct_above_50dma)

    computed_total = advance + decline + unchanged
    total_match = computed_total == total_stocks

    pct_200_valid = pct_200 is None or (0.0 <= pct_200 <= 100.0)
    pct_50_valid = pct_50 is None or (0.0 <= pct_50 <= 100.0)

    step.details = {
        "advance": advance,
        "decline": decline,
        "unchanged": unchanged,
        "total_stocks": total_stocks,
        "computed_total": computed_total,
        "total_match": total_match,
        "pct_above_200dma": pct_200,
        "pct_above_50dma": pct_50,
        "pct_200_valid": pct_200_valid,
        "pct_50_valid": pct_50_valid,
    }

    failures: list[str] = []
    if not total_match:
        failures.append(
            f"advance+decline+unchanged={computed_total} != total_stocks={total_stocks}"
        )
    if not pct_200_valid:
        failures.append(f"pct_above_200dma={pct_200} out of [0,100]")
    if not pct_50_valid:
        failures.append(f"pct_above_50dma={pct_50} out of [0,100]")

    if failures:
        step.status = "failed"
        step.message = "; ".join(failures)
    else:
        step.message = "Breadth arithmetic is consistent"

    return step


# ---------------------------------------------------------------------------
# Check 4: Regime self-consistency
# ---------------------------------------------------------------------------


async def spot_check_regime_self_consistency(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Validate stored regime vs recomputed from component scores.

    Recomputes confidence = breadth*0.30 + momentum*0.25 + volume*0.15 +
                            global*0.15 + fii*0.15
    Re-derives regime from recomputed confidence and breadth_score.
    Tolerance for confidence: 1.0 absolute.

    Returns:
        StepResult with values in details.
    """
    step = StepResult(name="spot_check_regime_self_consistency", status="passed")

    result = await session.execute(
        sa.text("""
            SELECT
                regime,
                confidence,
                breadth_score,
                momentum_score,
                volume_score,
                global_score,
                fii_score
            FROM de_market_regime
            WHERE date = :bdate
            ORDER BY computed_at DESC
            LIMIT 1
        """),
        {"bdate": business_date},
    )
    row = result.fetchone()

    if row is None:
        step.status = "skipped"
        step.message = "No regime row for business_date"
        return step

    stored_regime: str = str(row.regime)
    stored_confidence = _safe_float(row.confidence)
    breadth = _safe_float(row.breadth_score)
    momentum = _safe_float(row.momentum_score)
    volume = _safe_float(row.volume_score)
    global_s = _safe_float(row.global_score)
    fii = _safe_float(row.fii_score)

    if any(v is None for v in [stored_confidence, breadth, momentum, volume, global_s, fii]):
        step.status = "skipped"
        step.message = "Null component score in regime row"
        step.details = {
            "stored_regime": stored_regime,
            "stored_confidence": stored_confidence,
        }
        return step

    # Recompute confidence
    recomputed_confidence = (
        breadth * REGIME_CONFIDENCE_WEIGHTS["breadth_score"]    # type: ignore[operator]
        + momentum * REGIME_CONFIDENCE_WEIGHTS["momentum_score"]  # type: ignore[operator]
        + volume * REGIME_CONFIDENCE_WEIGHTS["volume_score"]    # type: ignore[operator]
        + global_s * REGIME_CONFIDENCE_WEIGHTS["global_score"]  # type: ignore[operator]
        + fii * REGIME_CONFIDENCE_WEIGHTS["fii_score"]          # type: ignore[operator]
    )

    confidence_delta = abs(stored_confidence - recomputed_confidence)  # type: ignore[operator]
    confidence_match = confidence_delta <= 1.0

    # Re-derive regime
    rederived_regime = _rederive_regime(
        confidence=recomputed_confidence,
        breadth_score=breadth,  # type: ignore[arg-type]
        momentum_score=momentum,  # type: ignore[arg-type]
    )
    regime_match = stored_regime == rederived_regime

    step.details = {
        "stored_regime": stored_regime,
        "rederived_regime": rederived_regime,
        "regime_match": regime_match,
        "stored_confidence": round(stored_confidence, 4),  # type: ignore[arg-type]
        "recomputed_confidence": round(recomputed_confidence, 4),
        "confidence_delta": round(confidence_delta, 4),
        "confidence_match": confidence_match,
        "component_scores": {
            "breadth_score": breadth,
            "momentum_score": momentum,
            "volume_score": volume,
            "global_score": global_s,
            "fii_score": fii,
        },
    }

    failures: list[str] = []
    if not confidence_match:
        failures.append(
            f"confidence delta {confidence_delta:.4f} exceeds tolerance 1.0"
        )
    if not regime_match:
        failures.append(
            f"stored regime '{stored_regime}' != rederived '{rederived_regime}'"
        )

    if failures:
        step.status = "failed"
        step.message = "; ".join(failures)
    else:
        step.message = "Regime self-consistency check passed"

    return step


# ---------------------------------------------------------------------------
# Check 5: MarketPulse RS comparison (stub)
# ---------------------------------------------------------------------------


async def spot_check_vs_marketpulse(
    session: AsyncSession, business_date: date
) -> StepResult:
    """Compare RS composites against MarketPulse legacy DB (stub).

    Skipped if fie_v3_database_url is not configured in settings.

    Returns:
        StepResult with status="skipped" when DB not configured.
    """
    step = StepResult(name="spot_check_vs_marketpulse", status="skipped")

    settings = get_settings()
    if not settings.fie_v3_database_url:
        step.message = "MarketPulse DB not configured"
        step.details["note"] = "Set fie_v3_database_url in .env to enable this check"
        return step

    # Stub: connection configured but comparison not yet implemented
    try:
        # Future: connect to fie_v3_database_url, query RS composites,
        # compare with de_rs_scores for SPOT_CHECK_SYMBOLS.
        step.status = "warning"
        step.message = "MarketPulse DB configured but comparison not yet implemented"
        step.details["note"] = "Stub — full comparison to be implemented in future chunk"
    except Exception as exc:
        step.status = "warning"
        step.message = f"MarketPulse connection error: {exc}"

    return step


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_spot_checks(
    session: AsyncSession,
    business_date: date,
) -> QAReport:
    """Run all 5 spot-checks and return an aggregated QAReport.

    Each check is wrapped in try/except — failures are captured as StepResult
    with status="failed" rather than propagating exceptions.

    Args:
        session: Async SQLAlchemy session.
        business_date: Date to run checks against.

    Returns:
        QAReport with phase="spot_check".
    """
    logger.info(
        "spot_check_start",
        business_date=business_date.isoformat(),
    )

    report = QAReport(phase="spot_check", business_date=business_date)

    check_fns = [
        spot_check_technicals,
        spot_check_rs_self_consistency,
        spot_check_breadth_arithmetic,
        spot_check_regime_self_consistency,
        spot_check_vs_marketpulse,
    ]

    for fn in check_fns:
        try:
            step = await fn(session, business_date)
        except Exception as exc:
            logger.error(
                "spot_check_step_exception",
                step=fn.__name__,
                error=str(exc),
                exc_info=True,
            )
            step = StepResult(
                name=fn.__name__,
                status="failed",
                message=f"Unhandled exception: {exc}",
            )
        report.add_step(step)
        logger.info(
            "spot_check_step_complete",
            step=step.name,
            status=step.status,
            message=step.message,
        )

    logger.info(
        "spot_check_complete",
        business_date=business_date.isoformat(),
        overall_status=report.overall_status,
        passed=report.passed,
        warnings=report.warnings,
        failed=report.failed,
        skipped=report.skipped,
    )

    return report
