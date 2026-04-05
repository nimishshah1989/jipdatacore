"""
Symbol resolver service.

Resolves NSE/BSE symbol strings → instrument_id (UUID) from de_instrument.
Caches resolved mappings in Redis for 24 h to avoid repeated DB lookups.
"""

import uuid
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument
from app.services.redis_service import RedisService

logger = get_logger(__name__)

_CACHE_TTL = 86_400  # 24 hours


async def resolve_symbol(
    symbol: str,
    db: AsyncSession,
    redis: Optional[RedisService] = None,
) -> Optional[uuid.UUID]:
    """
    Resolve a symbol string to its instrument_id UUID.

    Lookup order:
      1. Redis cache key ``sym:{symbol}``
      2. de_instrument.current_symbol (exact, case-insensitive)
      3. de_symbol_history.old_symbol → current instrument

    Returns None if symbol is not found.
    """
    upper = symbol.upper()
    cache_key = f"sym:{upper}"

    # 1. Redis cache
    if redis is not None:
        cached = await redis.get(cache_key)
        if cached:
            try:
                return uuid.UUID(cached)
            except ValueError:
                logger.warning("symbol_cache_invalid_uuid", symbol=upper, cached=cached)

    # 2. Current symbol lookup
    result = await db.execute(
        sa.select(DeInstrument.id).where(
            sa.func.upper(DeInstrument.current_symbol) == upper,
            DeInstrument.is_active.is_(True),
        )
    )
    row = result.scalar_one_or_none()

    # 3. Fallback: historical symbol table
    if row is None:
        from app.models.instruments import DeSymbolHistory

        hist_result = await db.execute(
            sa.select(DeSymbolHistory.instrument_id).where(
                sa.func.upper(DeSymbolHistory.old_symbol) == upper,
            )
            .order_by(DeSymbolHistory.effective_date.desc())
            .limit(1)
        )
        row = hist_result.scalar_one_or_none()

    if row is None:
        logger.info("symbol_not_found", symbol=upper)
        return None

    instrument_id: uuid.UUID = row

    # Cache the result
    if redis is not None:
        await redis.set(cache_key, str(instrument_id), ttl_seconds=_CACHE_TTL)

    return instrument_id


async def resolve_symbol_or_404(
    symbol: str,
    db: AsyncSession,
    redis: Optional[RedisService] = None,
) -> uuid.UUID:
    """
    Same as resolve_symbol but raises HTTPException 404 if not found.
    Import lazily to avoid circular deps at module level.
    """
    from fastapi import HTTPException, status

    instrument_id = await resolve_symbol(symbol, db, redis)
    if instrument_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Symbol '{symbol}' not found",
        )
    return instrument_id
