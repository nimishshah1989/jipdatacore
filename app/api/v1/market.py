"""Market Intelligence APIs — Regime, Breadth, and Macros."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.symbol_resolver import format_envelope
from app.models.computed import DeMarketRegime, DeBreadthDaily

router = APIRouter(prefix="/api/v1/market", tags=["Market Core"])


@router.get("/regime/current")
async def get_current_regime(session: AsyncSession = Depends(get_db)):
    """Fetch the latest quantitative market regime."""
    
    # We order by computed_at descending and grab the first row
    stmt = select(DeMarketRegime).order_by(DeMarketRegime.computed_at.desc()).limit(1)
    result = await session.execute(stmt)
    regime = result.scalar_one_or_none()
    
    if not regime:
        return format_envelope({}, meta={"msg": "No regime data available."})
        
    data = {
        "date": regime.computed_at.strftime("%Y-%m-%d"),
        "classification": regime.classification,
        "confidence": float(regime.confidence) if regime.confidence else 0.0,
        "indicator_detail": regime.indicator_detail
    }
    
    return format_envelope(data, meta={"computation_version": regime.computation_version})


@router.get("/breadth/latest")
async def get_latest_breadth(session: AsyncSession = Depends(get_db)):
    """Fetch the latest global market breadth metrics."""
    
    stmt = select(DeBreadthDaily).order_by(DeBreadthDaily.date.desc()).limit(1)
    result = await session.execute(stmt)
    breadth = result.scalar_one_or_none()
    
    if not breadth:
        return format_envelope({})
        
    data = {
        "date": breadth.date.strftime("%Y-%m-%d"),
        "ad_ratio": float(breadth.ad_ratio) if breadth.ad_ratio else 0.0,
        "advance_count": breadth.advance_count,
        "decline_count": breadth.decline_count,
        "pct_above_50dma": float(breadth.pct_above_50dma) if breadth.pct_above_50dma else 0.0,
        "pct_above_200dma": float(breadth.pct_above_200dma) if breadth.pct_above_200dma else 0.0,
        "new_52w_highs": breadth.new_52w_highs,
        "new_52w_lows": breadth.new_52w_lows
    }
    
    return format_envelope(data)
