"""Computation QA API — serves live data for computation_tracker.html.

Reads directly from computed tables (de_equity_technical_daily, de_rs_scores,
de_breadth_daily, de_market_regime, de_mf_derived_daily) rather than relying
on de_pipeline_log, so the dashboard reflects actual computation state.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import sqlalchemy as sa
import structlog
from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/computation", tags=["computation"])

IST = timezone(timedelta(hours=5, minutes=30))

_get_session: Any = None


def set_session_factory(factory: Any) -> None:
    global _get_session
    _get_session = factory


def _fmt_ist(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).isoformat()


# ---------------------------------------------------------------------------
# Direct table queries
# ---------------------------------------------------------------------------


async def _pipeline_status(session: Any) -> tuple[list[dict], Optional[str]]:
    """Query actual computed tables for row counts and date ranges."""
    steps = []

    # 1. Technicals
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, MAX(date) AS latest, MIN(date) AS earliest, "
        "COUNT(DISTINCT date) AS days FROM de_equity_technical_daily"
    ))
    row = r.fetchone()
    tech_rows = row.cnt if row else 0
    tech_latest = row.latest if row else None
    tech_days = row.days if row else 0
    steps.append({
        "step": "technicals",
        "status": "passed" if tech_rows > 0 else "pending",
        "rows": tech_rows,
        "days_computed": tech_days,
        "latest_date": str(tech_latest) if tech_latest else None,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # 2. RS Scores
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, COUNT(DISTINCT date) AS days, MAX(date) AS latest "
        "FROM de_rs_scores WHERE entity_type = 'equity'"
    ))
    row = r.fetchone()
    rs_rows = row.cnt if row else 0
    rs_days = row.days if row else 0
    rs_latest = row.latest if row else None
    steps.append({
        "step": "rs",
        "status": "passed" if rs_rows > 0 else "pending",
        "rows": rs_rows,
        "days_computed": rs_days,
        "latest_date": str(rs_latest) if rs_latest else None,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # 3. Breadth
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, MAX(date) AS latest FROM de_breadth_daily"
    ))
    row = r.fetchone()
    breadth_rows = row.cnt if row else 0
    breadth_latest = row.latest if row else None
    steps.append({
        "step": "breadth",
        "status": "passed" if breadth_rows > 0 else "pending",
        "rows": breadth_rows,
        "days_computed": breadth_rows,
        "latest_date": str(breadth_latest) if breadth_latest else None,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # 4. Regime
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, MAX(date) AS latest FROM de_market_regime"
    ))
    row = r.fetchone()
    regime_rows = row.cnt if row else 0
    regime_latest = row.latest if row else None
    # Get latest regime detail
    regime_detail = None
    if regime_rows > 0:
        r2 = await session.execute(sa.text(
            "SELECT regime, confidence, breadth_score, momentum_score, volume_score, "
            "global_score, fii_score FROM de_market_regime ORDER BY date DESC LIMIT 1"
        ))
        rr = r2.fetchone()
        if rr:
            regime_detail = {
                "regime": rr.regime,
                "confidence": float(rr.confidence) if rr.confidence else None,
                "breadth_score": float(rr.breadth_score) if rr.breadth_score else None,
                "momentum_score": float(rr.momentum_score) if rr.momentum_score else None,
                "volume_score": float(rr.volume_score) if rr.volume_score else None,
                "global_score": float(rr.global_score) if rr.global_score else None,
                "fii_score": float(rr.fii_score) if rr.fii_score else None,
            }
    steps.append({
        "step": "regime",
        "status": "passed" if regime_rows > 0 else "pending",
        "rows": regime_rows,
        "days_computed": regime_rows,
        "latest_date": str(regime_latest) if regime_latest else None,
        "regime_detail": regime_detail,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # 5. Sectors
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, COUNT(DISTINCT date) AS days, MAX(date) AS latest "
        "FROM de_rs_scores WHERE entity_type = 'sector'"
    ))
    row = r.fetchone()
    sector_rows = row.cnt if row else 0
    sector_days = row.days if row else 0
    steps.append({
        "step": "sectors",
        "status": "passed" if sector_rows > 0 else "pending",
        "rows": sector_rows,
        "days_computed": sector_days,
        "latest_date": str(row.latest) if row and row.latest else None,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # 6. Fund Derived
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, COUNT(DISTINCT nav_date) AS days, MAX(nav_date) AS latest "
        "FROM de_mf_derived_daily"
    ))
    row = r.fetchone()
    fund_rows = row.cnt if row else 0
    fund_days = row.days if row else 0
    fund_latest = row.latest if row else None
    steps.append({
        "step": "fund_derived",
        "status": "passed" if fund_rows > 0 else "pending",
        "rows": fund_rows,
        "days_computed": fund_days,
        "latest_date": str(fund_latest) if fund_latest else None,
        "duration_s": None,
        "started_at": None,
        "completed_at": None,
        "errors": [],
    })

    # Business date = latest computed date
    latest_date = str(tech_latest) if tech_latest else None

    return steps, latest_date


async def _breadth_history(session: Any) -> list[dict]:
    """Last 10 breadth entries for the dashboard."""
    r = await session.execute(sa.text(
        "SELECT date, advance, decline, unchanged, total_stocks, "
        "CAST(ad_ratio AS FLOAT) AS ad_ratio, "
        "CAST(pct_above_200dma AS FLOAT) AS pct_200, "
        "CAST(pct_above_50dma AS FLOAT) AS pct_50, "
        "new_52w_highs, new_52w_lows "
        "FROM de_breadth_daily ORDER BY date DESC LIMIT 10"
    ))
    rows = r.fetchall()
    return [
        {
            "date": str(row.date),
            "advance": row.advance,
            "decline": row.decline,
            "unchanged": row.unchanged,
            "total": row.total_stocks,
            "ad_ratio": round(row.ad_ratio, 2) if row.ad_ratio else None,
            "pct_above_200dma": round(row.pct_200, 1) if row.pct_200 else None,
            "pct_above_50dma": round(row.pct_50, 1) if row.pct_50 else None,
            "new_52w_highs": row.new_52w_highs,
            "new_52w_lows": row.new_52w_lows,
        }
        for row in rows
    ]


async def _regime_history(session: Any) -> list[dict]:
    """Last 20 regime entries."""
    r = await session.execute(sa.text(
        "SELECT date, regime, CAST(confidence AS FLOAT) AS confidence "
        "FROM de_market_regime ORDER BY date DESC LIMIT 20"
    ))
    return [
        {"date": str(row.date), "regime": row.regime, "confidence": round(row.confidence, 1) if row.confidence else None}
        for row in r.fetchall()
    ]


async def _fund_derived_summary(session: Any) -> dict:
    """Summary stats from de_mf_derived_daily for latest date."""
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, "
        "AVG(CAST(sharpe_1y AS FLOAT)) AS avg_sharpe, "
        "AVG(CAST(beta_vs_nifty AS FLOAT)) AS avg_beta, "
        "AVG(CAST(volatility_1y AS FLOAT)) AS avg_vol, "
        "AVG(CAST(max_drawdown_1y AS FLOAT)) AS avg_dd, "
        "AVG(CAST(coverage_pct AS FLOAT)) AS avg_coverage "
        "FROM de_mf_derived_daily "
        "WHERE nav_date = (SELECT MAX(nav_date) FROM de_mf_derived_daily)"
    ))
    row = r.fetchone()
    if not row or row.cnt == 0:
        return {}
    return {
        "fund_count": row.cnt,
        "avg_sharpe_1y": round(row.avg_sharpe, 4) if row.avg_sharpe else None,
        "avg_beta": round(row.avg_beta, 4) if row.avg_beta else None,
        "avg_volatility_1y": round(row.avg_vol, 4) if row.avg_vol else None,
        "avg_max_drawdown_1y": round(row.avg_dd, 4) if row.avg_dd else None,
        "avg_coverage_pct": round(row.avg_coverage, 1) if row.avg_coverage else None,
    }


async def _rs_distribution(session: Any) -> dict:
    """RS score distribution stats for latest date."""
    r = await session.execute(sa.text(
        "SELECT COUNT(*) AS cnt, "
        "AVG(CAST(rs_composite AS FLOAT)) AS mean_rs, "
        "STDDEV(CAST(rs_composite AS FLOAT)) AS std_rs, "
        "MIN(CAST(rs_composite AS FLOAT)) AS min_rs, "
        "MAX(CAST(rs_composite AS FLOAT)) AS max_rs "
        "FROM de_rs_scores "
        "WHERE entity_type = 'equity' AND vs_benchmark = 'NIFTY 50' "
        "AND date = (SELECT MAX(date) FROM de_rs_scores WHERE entity_type = 'equity')"
    ))
    row = r.fetchone()
    if not row or row.cnt == 0:
        return {}
    return {
        "stock_count": row.cnt,
        "mean": round(row.mean_rs, 4) if row.mean_rs else None,
        "stddev": round(row.std_rs, 4) if row.std_rs else None,
        "min": round(row.min_rs, 4) if row.min_rs else None,
        "max": round(row.max_rs, 4) if row.max_rs else None,
    }


async def _computation_progress(session: Any) -> dict:
    """How many dates have been computed vs total available."""
    r = await session.execute(sa.text(
        "SELECT COUNT(DISTINCT date) FROM de_equity_ohlcv "
        "WHERE data_status = 'validated' AND date >= '2024-01-01'"
    ))
    total_dates = r.scalar_one() or 0

    r2 = await session.execute(sa.text(
        "SELECT COUNT(DISTINCT date) FROM de_equity_technical_daily"
    ))
    computed_dates = r2.scalar_one() or 0

    return {
        "total_trading_days": total_dates,
        "days_computed": computed_dates,
        "pct_complete": round(computed_dates / total_dates * 100, 1) if total_dates > 0 else 0,
        "remaining": total_dates - computed_dates,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/tracker", include_in_schema=False)
async def computation_tracker_page() -> FileResponse:
    html_path = Path(__file__).parent / "computation_tracker.html"
    if not html_path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="computation_tracker.html not found")
    return FileResponse(str(html_path), media_type="text/html")


@router.get("/status")
async def get_computation_status() -> JSONResponse:
    """Live computation status from actual computed tables."""
    if _get_session is None:
        return JSONResponse(status_code=503, content={"error": "Database not initialised"})

    server_time_ist = datetime.now(tz=IST).isoformat()

    try:
        async with _get_session() as session:
            pipeline_status, business_date = await _pipeline_status(session)
            progress = await _computation_progress(session)
            breadth = await _breadth_history(session)
            regimes = await _regime_history(session)
            fund_summary = await _fund_derived_summary(session)
            rs_dist = await _rs_distribution(session)

        return JSONResponse(content={
            "business_date": business_date,
            "server_time_ist": server_time_ist,
            "progress": progress,
            "pipeline_status": pipeline_status,
            "breadth_history": breadth,
            "regime_history": regimes,
            "fund_derived_summary": fund_summary,
            "rs_distribution": rs_dist,
            "pre_qa": [],
            "post_qa": [],
            "spot_checks": [],
            "mstar_crossval": [],
            "bad_data": [],
        })

    except Exception as exc:
        logger.error("computation_status_error", error=str(exc))
        return JSONResponse(
            status_code=500,
            content={"error": str(exc), "server_time_ist": server_time_ist},
        )
