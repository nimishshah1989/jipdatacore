"""Equity API Endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.symbol_resolver import resolve_symbol_to_id, format_envelope
from app.models.prices import DeEquityOhlcv
from app.models.instruments import DeInstrument

router = APIRouter(prefix="/api/v1/equity", tags=["Equity"])


@router.get("/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: AsyncSession = Depends(get_db)
):
    """Fetch OHLCV history properly adjusting for splits and bonuses."""
    
    # 1. MUST resolve symbol to UUID first! (v1.7 constraint)
    instrument_id = await resolve_symbol_to_id(symbol, session)
    
    # 2. Query prices where status is validated
    stmt = select(
        DeEquityOhlcv.date,
        DeEquityOhlcv.open_adj,
        DeEquityOhlcv.high_adj,
        DeEquityOhlcv.low_adj,
        DeEquityOhlcv.close_adj,
        DeEquityOhlcv.volume_adj,
        DeEquityOhlcv.delivery_pct
    ).where(
        DeEquityOhlcv.instrument_id == instrument_id,
        DeEquityOhlcv.data_status == 'validated'
    ).order_by(DeEquityOhlcv.date.desc())
    
    if from_date:
        stmt = stmt.where(DeEquityOhlcv.date >= from_date)
    if to_date:
        stmt = stmt.where(DeEquityOhlcv.date <= to_date)
        
    # Standard limitation to protect DB if dates not provided
    if not from_date and not to_date:
        stmt = stmt.limit(252) # 1 trading year
        
    result = await session.execute(stmt)
    records = []
    
    for row in result:
        records.append({
            "date": row.date.strftime("%Y-%m-%d"),
            "open": float(row.open_adj) if row.open_adj else None,
            "high": float(row.high_adj) if row.high_adj else None,
            "low": float(row.low_adj) if row.low_adj else None,
            "close": float(row.close_adj) if row.close_adj else None,
            "volume": int(row.volume_adj) if row.volume_adj else 0,
            "delivery_pct": float(row.delivery_pct) if row.delivery_pct else 0.0
        })
        
    return format_envelope(records)


@router.get("/universe")
async def get_active_universe(
    active: bool = True,
    sector: Optional[str] = None,
    cap_category: Optional[str] = None,
    session: AsyncSession = Depends(get_db)
):
    """Fetch all tradable equities matching filters."""
    
    stmt = select(
        DeInstrument.current_symbol,
        DeInstrument.company_name,
        DeInstrument.sector,
        DeInstrument.nifty_500
    ).where(DeInstrument.exchange == 'NSE')
    
    if active:
        stmt = stmt.where(DeInstrument.is_active == True, DeInstrument.is_tradeable == True)
        
    if sector:
        stmt = stmt.where(DeInstrument.sector == sector)
        
    result = await session.execute(stmt)
    records = [{"symbol": row.current_symbol, "name": row.company_name, "sector": row.sector, "in_500": row.nifty_500} for row in result]
    
    return format_envelope(records, pagination={"total": len(records)})
