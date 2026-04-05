"""
Market pulse API endpoints.

GET /api/v1/regime/current       — Latest market regime
GET /api/v1/regime/history       — Regime history
GET /api/v1/breadth/latest       — Latest breadth snapshot
GET /api/v1/breadth/history      — Breadth history
GET /api/v1/indices/list         — Index master list
GET /api/v1/indices/{code}/history — Index OHLCV history
GET /api/v1/global/indices       — Global indices latest prices
GET /api/v1/global/macro         — Global macro values
"""

import json
from datetime import date
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
from app.models.computed import DeBreadthDaily, DeMarketRegime
from app.models.instruments import DeIndexMaster
from app.models.prices import DeGlobalPrices, DeIndexPrices, DeMacroValues
from app.services.data_freshness import check_breadth_freshness, check_regime_freshness
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["market"])

_CACHE_TTL_LIVE = 3_600   # 1 h
_CACHE_TTL_HIST = 86_400  # 24 h


# ---- Endpoints ----


@router.get(
    "/regime/current",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Latest market regime classification",
)
async def get_regime_current(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
) -> EnvelopeResponse:
    """Return the most recent market regime classification."""
    cache_key = "regime:current"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    result = await db.execute(
        sa.select(DeMarketRegime)
        .order_by(DeMarketRegime.computed_at.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        meta = ResponseMeta(data_freshness=DataFreshness.STALE)
        envelope = build_envelope(data=None, meta=meta)
        response.headers.update(envelope_headers(meta))
        return envelope

    data = {
        "date": str(row.date),
        "computed_at": row.computed_at.isoformat(),
        "regime": row.regime,
        "confidence": row.confidence,
        "breadth_score": row.breadth_score,
        "momentum_score": row.momentum_score,
        "volume_score": row.volume_score,
        "global_score": row.global_score,
        "fii_score": row.fii_score,
        "indicator_detail": row.indicator_detail,
        "computation_version": row.computation_version,
    }

    freshness = await check_regime_freshness(db)
    meta = ResponseMeta(
        data_freshness=freshness,
        computation_version=row.computation_version,
    )
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/regime/history",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Market regime history",
)
async def get_regime_history(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
) -> EnvelopeResponse:
    """Return historical market regime classifications."""
    cache_key = f"regime:history:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if from_date:
        filters.append(DeMarketRegime.date >= from_date)
    if to_date:
        filters.append(DeMarketRegime.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeMarketRegime).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeMarketRegime)
        .where(*filters)
        .order_by(DeMarketRegime.computed_at.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "date": str(r.date),
            "computed_at": r.computed_at.isoformat(),
            "regime": r.regime,
            "confidence": r.confidence,
            "computation_version": r.computation_version,
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


@router.get(
    "/breadth/latest",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Latest market breadth snapshot",
)
async def get_breadth_latest(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
) -> EnvelopeResponse:
    """Return the most recent breadth statistics."""
    cache_key = "breadth:latest"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    result = await db.execute(
        sa.select(DeBreadthDaily)
        .order_by(DeBreadthDaily.date.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        meta = ResponseMeta(data_freshness=DataFreshness.STALE)
        envelope = build_envelope(data=None, meta=meta)
        response.headers.update(envelope_headers(meta))
        return envelope

    data = {
        "date": str(row.date),
        "advance": row.advance,
        "decline": row.decline,
        "unchanged": row.unchanged,
        "total_stocks": row.total_stocks,
        "ad_ratio": row.ad_ratio,
        "pct_above_200dma": row.pct_above_200dma,
        "pct_above_50dma": row.pct_above_50dma,
        "new_52w_highs": row.new_52w_highs,
        "new_52w_lows": row.new_52w_lows,
    }

    freshness = await check_breadth_freshness(db)
    meta = ResponseMeta(data_freshness=freshness)
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/breadth/history",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Market breadth history",
)
async def get_breadth_history(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
) -> EnvelopeResponse:
    """Return historical breadth statistics."""
    cache_key = f"breadth:history:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if from_date:
        filters.append(DeBreadthDaily.date >= from_date)
    if to_date:
        filters.append(DeBreadthDaily.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeBreadthDaily).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeBreadthDaily)
        .where(*filters)
        .order_by(DeBreadthDaily.date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "date": str(r.date),
            "advance": r.advance,
            "decline": r.decline,
            "unchanged": r.unchanged,
            "total_stocks": r.total_stocks,
            "ad_ratio": r.ad_ratio,
            "pct_above_200dma": r.pct_above_200dma,
            "pct_above_50dma": r.pct_above_50dma,
            "new_52w_highs": r.new_52w_highs,
            "new_52w_lows": r.new_52w_lows,
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


@router.get(
    "/indices/list",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="List all available indices",
)
async def get_indices_list(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    category: Optional[str] = Query(
        default=None, description="Filter by category: broad, sectoral, thematic, strategy"
    ),
) -> EnvelopeResponse:
    """Return index master list."""
    cache_key = f"indices:list:{category}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if category:
        valid_cats = {"broad", "sectoral", "thematic", "strategy"}
        if category not in valid_cats:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"category must be one of: {', '.join(sorted(valid_cats))}",
            )
        filters.append(DeIndexMaster.category == category)

    rows_result = await db.execute(
        sa.select(DeIndexMaster).where(*filters).order_by(DeIndexMaster.index_code.asc())
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "index_code": r.index_code,
            "index_name": r.index_name,
            "category": r.category,
        }
        for r in rows
    ]

    meta = ResponseMeta()
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_HIST)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/indices/{code}/history",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="OHLCV history for an index",
)
async def get_index_history(
    code: str,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
) -> EnvelopeResponse:
    """Return OHLCV + valuation history for an index."""
    upper_code = code.upper()

    # Verify index exists
    idx_result = await db.execute(
        sa.select(DeIndexMaster).where(DeIndexMaster.index_code == upper_code)
    )
    if idx_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index '{upper_code}' not found",
        )

    cache_key = f"indices:hist:{upper_code}:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters = [DeIndexPrices.index_code == upper_code]
    if from_date:
        filters.append(DeIndexPrices.date >= from_date)
    if to_date:
        filters.append(DeIndexPrices.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeIndexPrices).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeIndexPrices)
        .where(*filters)
        .order_by(DeIndexPrices.date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "date": str(r.date),
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
            "pe_ratio": r.pe_ratio,
            "pb_ratio": r.pb_ratio,
            "div_yield": r.div_yield,
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

    ttl = _CACHE_TTL_LIVE if not to_date or to_date >= date.today() else _CACHE_TTL_HIST
    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=ttl)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/global/indices",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Latest global index prices",
)
async def get_global_indices(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    as_of_date: Optional[date] = Query(default=None, description="Date (defaults to latest)"),
) -> EnvelopeResponse:
    """Return latest global instrument prices."""
    if as_of_date is None:
        date_result = await db.execute(sa.select(sa.func.max(DeGlobalPrices.date)))
        as_of_date = date_result.scalar_one_or_none()
        if as_of_date is None:
            meta = ResponseMeta(data_freshness=DataFreshness.STALE)
            envelope = build_envelope(data=[], meta=meta)
            response.headers.update(envelope_headers(meta))
            return envelope

    cache_key = f"global:indices:{as_of_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeGlobalPrices).where(
            DeGlobalPrices.date == as_of_date
        )
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeGlobalPrices)
        .where(DeGlobalPrices.date == as_of_date)
        .order_by(DeGlobalPrices.ticker.asc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "ticker": r.ticker,
            "date": str(r.date),
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
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

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/global/macro",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Macro indicator values",
)
async def get_global_macro(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    ticker: Optional[str] = Query(default=None, description="Filter by macro ticker"),
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
) -> EnvelopeResponse:
    """Return macro indicator values."""
    cache_key = f"global:macro:{ticker}:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if ticker:
        filters.append(DeMacroValues.ticker == ticker.upper())
    if from_date:
        filters.append(DeMacroValues.date >= from_date)
    if to_date:
        filters.append(DeMacroValues.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeMacroValues).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeMacroValues)
        .where(*filters)
        .order_by(DeMacroValues.ticker.asc(), DeMacroValues.date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "ticker": r.ticker,
            "date": str(r.date),
            "value": r.value,
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
