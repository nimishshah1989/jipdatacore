"""
Sector breadth API endpoints.

GET /api/v1/sectors/breadth          — Latest sector breadth (all sectors)
GET /api/v1/sectors/breadth/history  — Time-series for a specific sector
"""

import json
from datetime import date, timedelta
from typing import Annotated, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PaginationParams, get_current_user, get_db, get_redis
from app.logging import get_logger
from app.middleware.response import (
    DataFreshness,
    EnvelopeResponse,
    PaginationMeta,
    ResponseMeta,
    build_envelope,
    envelope_headers,
)
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/sectors", tags=["sectors"])

_CACHE_TTL_LIVE = 3_600
_CACHE_TTL_HIST = 86_400

_WINDOW_MAP = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "2y": 730,
    "5y": 1825,
}


def _row_to_dict(row) -> dict:
    return {
        "date": str(row.date),
        "sector": row.sector,
        "stocks_total": row.stocks_total,
        "stocks_above_50dma": row.stocks_above_50dma,
        "stocks_above_200dma": row.stocks_above_200dma,
        "stocks_above_20ema": row.stocks_above_20ema,
        "pct_above_50dma": row.pct_above_50dma,
        "pct_above_200dma": row.pct_above_200dma,
        "pct_above_20ema": row.pct_above_20ema,
        "stocks_rsi_overbought": row.stocks_rsi_overbought,
        "stocks_rsi_oversold": row.stocks_rsi_oversold,
        "stocks_macd_bullish": row.stocks_macd_bullish,
        "breadth_regime": row.breadth_regime,
    }


@router.get(
    "/breadth",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Sector breadth snapshot for a given date",
)
async def get_sector_breadth(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    date_param: Optional[date] = Query(
        default=None, alias="date", description="Date (ISO format, defaults to latest)"
    ),
) -> EnvelopeResponse:
    if date_param is None:
        max_date_result = await db.execute(
            sa.text("SELECT max(date) FROM de_sector_breadth_daily")
        )
        date_param = max_date_result.scalar_one_or_none()
        if date_param is None:
            meta = ResponseMeta(data_freshness=DataFreshness.STALE)
            envelope = build_envelope(data=[], meta=meta)
            response.headers.update(envelope_headers(meta))
            return envelope

    cache_key = f"sectors:breadth:{date_param}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    result = await db.execute(
        sa.text(
            "SELECT * FROM de_sector_breadth_daily "
            "WHERE date = :d ORDER BY sector"
        ),
        {"d": date_param},
    )
    rows = result.mappings().all()

    data = [
        {
            "date": str(r["date"]),
            "sector": r["sector"],
            "stocks_total": r["stocks_total"],
            "stocks_above_50dma": r["stocks_above_50dma"],
            "stocks_above_200dma": r["stocks_above_200dma"],
            "stocks_above_20ema": r["stocks_above_20ema"],
            "pct_above_50dma": r["pct_above_50dma"],
            "pct_above_200dma": r["pct_above_200dma"],
            "pct_above_20ema": r["pct_above_20ema"],
            "stocks_rsi_overbought": r["stocks_rsi_overbought"],
            "stocks_rsi_oversold": r["stocks_rsi_oversold"],
            "stocks_macd_bullish": r["stocks_macd_bullish"],
            "breadth_regime": r["breadth_regime"],
        }
        for r in rows
    ]

    meta = ResponseMeta()
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/breadth/history",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Sector breadth time-series for a sector",
)
async def get_sector_breadth_history(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    sector: str = Query(description="Sector name"),
    window: str = Query(default="1y", description="Lookback window: 1m,3m,6m,1y,2y,5y"),
) -> EnvelopeResponse:
    days = _WINDOW_MAP.get(window)
    if days is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"window must be one of: {', '.join(sorted(_WINDOW_MAP))}",
        )

    from_date = date.today() - timedelta(days=days)

    cache_key = (
        f"sectors:breadth:hist:{sector}:{window}:"
        f"{pagination.page}:{pagination.page_size}"
    )
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    count_result = await db.execute(
        sa.text(
            "SELECT count(*) FROM de_sector_breadth_daily "
            "WHERE sector = :sector AND date >= :from_date"
        ),
        {"sector": sector, "from_date": from_date},
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.text(
            "SELECT * FROM de_sector_breadth_daily "
            "WHERE sector = :sector AND date >= :from_date "
            "ORDER BY date DESC "
            "LIMIT :limit OFFSET :offset"
        ),
        {
            "sector": sector,
            "from_date": from_date,
            "limit": pagination.page_size,
            "offset": pagination.offset,
        },
    )
    rows = rows_result.mappings().all()

    data = [
        {
            "date": str(r["date"]),
            "sector": r["sector"],
            "stocks_total": r["stocks_total"],
            "stocks_above_50dma": r["stocks_above_50dma"],
            "stocks_above_200dma": r["stocks_above_200dma"],
            "stocks_above_20ema": r["stocks_above_20ema"],
            "pct_above_50dma": r["pct_above_50dma"],
            "pct_above_200dma": r["pct_above_200dma"],
            "pct_above_20ema": r["pct_above_20ema"],
            "stocks_rsi_overbought": r["stocks_rsi_overbought"],
            "stocks_rsi_oversold": r["stocks_rsi_oversold"],
            "stocks_macd_bullish": r["stocks_macd_bullish"],
            "breadth_regime": r["breadth_regime"],
        }
        for r in rows
    ]

    meta = ResponseMeta()
    pag = PaginationMeta(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=total,
        has_next=(pagination.offset + pagination.page_size) < total,
    )
    envelope = build_envelope(data=data, meta=meta, pagination=pag)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_HIST)
    response.headers.update(envelope_headers(meta))
    return envelope
