"""Symbol resolution service — converts generic UI symbols to precise instrument UUIDs."""

from typing import Optional
from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from app.models.instruments import DeInstrument


async def resolve_symbol_to_id(symbol: str, session: AsyncSession) -> UUID:
    """Resolve a raw frontend symbol to internal DB UUID.
    Critical path (v1.7): Never query OHLCV on symbol string, always resolve UUID first 
    for partition pruning efficiency.
    """
    query = select(DeInstrument.id).where(DeInstrument.current_symbol == symbol.upper())
    result = await session.execute(query)
    instrument_id = result.scalar_one_or_none()
    
    if not instrument_id:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found in active instruments.")
        
    return instrument_id


def format_envelope(data: Any, meta: dict = None, pagination: dict = None) -> dict:
    """Standard API response envelope v1.9."""
    from datetime import datetime
    import pytz
    
    # IST timestamp format required via spec
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist).isoformat()
    
    response_meta = {
        "timestamp": now_ist,
        "computation_version": 1,
        "data_freshness": "fresh",
        "system_status": "normal"
    }
    
    if meta:
        response_meta.update(meta)
        
    envelope = {
        "data": data,
        "meta": response_meta
    }
    
    if pagination:
        envelope["pagination"] = pagination
        
    return envelope
