"""Goldilocks Intelligence API endpoints."""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

router = APIRouter(prefix="/goldilocks", tags=["goldilocks"])


def _dec(v) -> Optional[str]:
    """Serialize Decimal to string for JSON, None pass-through."""
    if v is None:
        return None
    return str(v)


@router.get("/market-view")
async def get_market_view(
    dt: Optional[date] = Query(None, alias="date", description="Report date (default: latest)"),
    session: AsyncSession = Depends(get_db),
):
    """Latest Goldilocks market view — S/R levels, trend, sector table."""
    if dt:
        where = "WHERE mv.report_date = :dt"
        params = {"dt": dt}
    else:
        where = "WHERE mv.report_date = (SELECT MAX(report_date) FROM de_goldilocks_market_view)"
        params = {}

    result = await session.execute(text(f"""
        SELECT mv.* FROM de_goldilocks_market_view mv {where}
    """), params)
    row = result.mappings().fetchone()
    if not row:
        raise HTTPException(404, "No market view found for date")

    # Get sector views for same date
    sectors = await session.execute(text("""
        SELECT sector, trend, outlook, rank, top_picks
        FROM de_goldilocks_sector_view
        WHERE report_date = :dt
        ORDER BY rank NULLS LAST
    """), {"dt": row["report_date"]})

    return {
        "report_date": str(row["report_date"]),
        "nifty": {
            "close": _dec(row["nifty_close"]),
            "support": [_dec(row["nifty_support_1"]), _dec(row["nifty_support_2"])],
            "resistance": [_dec(row["nifty_resistance_1"]), _dec(row["nifty_resistance_2"])],
        },
        "bank_nifty": {
            "close": _dec(row["bank_nifty_close"]),
            "support": [_dec(row["bank_nifty_support_1"]), _dec(row["bank_nifty_support_2"])],
            "resistance": [_dec(row["bank_nifty_resistance_1"]), _dec(row["bank_nifty_resistance_2"])],
        },
        "trend": {
            "direction": row["trend_direction"],
            "strength": row["trend_strength"],
        },
        "global_impact": row["global_impact"],
        "headline": row["headline"],
        "overall_view": row["overall_view"],
        "sectors": [
            {
                "sector": s["sector"],
                "trend": s["trend"],
                "outlook": s["outlook"],
                "rank": s["rank"],
                "top_picks": s["top_picks"],
            }
            for s in sectors.mappings().all()
        ],
    }


@router.get("/sector-views")
async def get_sector_views(
    dt: Optional[date] = Query(None, alias="date"),
    sector: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db),
):
    """Sector rankings with outlook and top picks."""
    conditions = []
    params = {}

    if dt:
        conditions.append("report_date = :dt")
        params["dt"] = dt
    else:
        conditions.append("report_date = (SELECT MAX(report_date) FROM de_goldilocks_sector_view)")

    if sector:
        conditions.append("sector ILIKE :sector")
        params["sector"] = f"%{sector}%"

    where = " AND ".join(conditions)
    result = await session.execute(text(f"""
        SELECT report_date, sector, trend, outlook, rank, top_picks
        FROM de_goldilocks_sector_view
        WHERE {where}
        ORDER BY rank NULLS LAST
    """), params)

    return [dict(r) for r in result.mappings().all()]


@router.get("/stock-ideas")
async def get_stock_ideas(
    status: Optional[str] = Query("active"),
    idea_type: Optional[str] = Query(None),
    session: AsyncSession = Depends(get_db),
):
    """Stock ideas with current P&L."""
    conditions = ["1=1"]
    params = {}

    if status:
        conditions.append("i.status = :status")
        params["status"] = status

    if idea_type:
        conditions.append("i.idea_type = :itype")
        params["itype"] = idea_type

    where = " AND ".join(conditions)
    result = await session.execute(text(f"""
        SELECT i.*,
            (SELECT close FROM de_equity_ohlcv e
             JOIN de_instrument inst ON inst.id = e.instrument_id
             WHERE inst.current_symbol = i.symbol
             ORDER BY e.date DESC LIMIT 1) AS current_price
        FROM de_goldilocks_stock_ideas i
        WHERE {where}
        ORDER BY i.published_date DESC NULLS LAST
    """), params)

    ideas = []
    for row in result.mappings().all():
        entry = row["entry_price"] or row["entry_zone_high"]
        current = row["current_price"]
        pnl = None
        if entry and current:
            pnl = round((current - entry) / entry * 100, 2)

        ideas.append({
            "id": str(row["id"]),
            "symbol": row["symbol"],
            "company_name": row["company_name"],
            "idea_type": row["idea_type"],
            "published_date": str(row["published_date"]) if row["published_date"] else None,
            "entry_price": _dec(row["entry_price"]),
            "entry_zone": [_dec(row["entry_zone_low"]), _dec(row["entry_zone_high"])],
            "target_1": _dec(row["target_1"]),
            "target_2": _dec(row["target_2"]),
            "stop_loss": _dec(row["stop_loss"]),
            "timeframe": row["timeframe"],
            "status": row["status"],
            "current_price": _dec(current),
            "unrealized_pnl_pct": _dec(pnl) if pnl is not None else None,
            "rationale": row["rationale"],
        })

    return ideas


@router.get("/scorecard")
async def get_scorecard(session: AsyncSession = Depends(get_db)):
    """Goldilocks stock idea accuracy scorecard."""
    from app.computation.outcome_tracker import get_goldilocks_scorecard
    return await get_goldilocks_scorecard(session)


@router.get("/divergences")
async def get_divergences(
    timeframe: Optional[str] = Query("weekly"),
    min_strength: Optional[int] = Query(1),
    limit: Optional[int] = Query(50),
    session: AsyncSession = Depends(get_db),
):
    """Recent divergence signals."""
    result = await session.execute(text("""
        SELECT d.date, d.timeframe, d.divergence_type, d.indicator,
               d.price_direction, d.indicator_direction, d.strength,
               i.current_symbol AS symbol
        FROM de_divergence_signals d
        JOIN de_instrument i ON i.id = d.instrument_id
        WHERE d.timeframe = :tf AND d.strength >= :ms
        ORDER BY d.date DESC, d.strength DESC
        LIMIT :lim
    """), {"tf": timeframe, "ms": min_strength, "lim": limit})

    return [dict(r) for r in result.mappings().all()]
