"""Instrument deepdive endpoint — everything Atlas needs in one call."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db, get_redis
from app.services.instrument_deepdive_service import get_instrument_deepdive
from app.services.redis_service import RedisService
from app.services.symbol_resolver import resolve_symbol

router = APIRouter(prefix="/api/v1", tags=["instrument"])


@router.get(
    "/instrument/{symbol}",
    summary="Full deepdive for a stock by symbol",
    status_code=status.HTTP_200_OK,
)
async def instrument_deepdive_by_symbol(
    symbol: str,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
):
    upper = symbol.upper()
    instrument_id = await resolve_symbol(upper, db, redis)
    if instrument_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="symbol not found",
        )
    return await get_instrument_deepdive(instrument_id, db)


@router.get(
    "/instrument/id/{instrument_id}",
    summary="Full deepdive for a stock by instrument UUID",
    status_code=status.HTTP_200_OK,
)
async def instrument_deepdive_by_id(
    instrument_id: uuid.UUID,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    return await get_instrument_deepdive(instrument_id, db)
