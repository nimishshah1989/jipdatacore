"""
Equity API endpoints.

GET /api/v1/equity/ohlcv/{symbol}      — OHLCV history for a symbol
GET /api/v1/equity/universe            — Equity universe (instrument master)
GET /api/v1/rs/stocks                  — RS scores for all stocks
GET /api/v1/rs/sectors                 — RS scores aggregated by sector
GET /api/v1/rs/stock/{symbol}          — RS score for a single symbol
"""

import json
from datetime import date
from typing import Annotated, Any, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel
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
from app.models.computed import DeRsDailySummary, DeRsScores
from app.models.instruments import DeInstrument
from app.models.prices import DeEquityOhlcv
from app.services.data_freshness import check_equity_freshness
from app.services.redis_service import RedisService
from app.services.symbol_resolver import resolve_symbol_or_404

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["equity"])

_CACHE_TTL_LIVE = 3_600   # 1 h
_CACHE_TTL_HIST = 86_400  # 24 h


# ---- Response schemas ----


class OhlcvRow(BaseModel):
    date: date
    open: Optional[Any] = None
    high: Optional[Any] = None
    low: Optional[Any] = None
    close: Optional[Any] = None
    close_adj: Optional[Any] = None
    volume: Optional[int] = None
    delivery_pct: Optional[Any] = None


class InstrumentRow(BaseModel):
    symbol: str
    isin: Optional[str] = None
    company_name: Optional[str] = None
    exchange: Optional[str] = None
    sector: Optional[str] = None
    nifty_50: bool
    nifty_200: bool
    nifty_500: bool
    is_active: bool


class RsRow(BaseModel):
    symbol: Optional[str] = None
    sector: Optional[str] = None
    rs_composite: Optional[Any] = None
    rs_1m: Optional[Any] = None
    rs_3m: Optional[Any] = None
    vs_benchmark: str
    date: date


# ---- Endpoints ----


@router.get(
    "/equity/ohlcv/{symbol}",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="OHLCV price history for a symbol",
)
async def get_equity_ohlcv(
    symbol: str,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date (inclusive) YYYY-MM-DD"),
    to_date: Optional[date] = Query(default=None, description="End date (inclusive) YYYY-MM-DD"),
    adjusted: Optional[bool] = Query(default=True, description="Return adjusted prices"),
) -> EnvelopeResponse:
    """Return OHLCV history for a symbol. Only validated data is returned."""
    upper = symbol.upper()
    cache_key = f"ohlcv:{upper}:{from_date}:{to_date}:{adjusted}:{pagination.page}:{pagination.page_size}"

    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    instrument_id = await resolve_symbol_or_404(upper, db, redis)

    filters = [
        DeEquityOhlcv.instrument_id == instrument_id,
        DeEquityOhlcv.data_status == "validated",
    ]
    if from_date:
        filters.append(DeEquityOhlcv.date >= from_date)
    if to_date:
        filters.append(DeEquityOhlcv.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeEquityOhlcv).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeEquityOhlcv)
        .where(*filters)
        .order_by(DeEquityOhlcv.date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        OhlcvRow(
            date=r.date,
            open=r.open_adj if adjusted else r.open,
            high=r.high_adj if adjusted else r.high,
            low=r.low_adj if adjusted else r.low,
            close=r.close_adj if adjusted else r.close,
            close_adj=r.close_adj,
            volume=r.volume,
            delivery_pct=r.delivery_pct,
        )
        for r in rows
    ]

    freshness = await check_equity_freshness(db)
    meta = ResponseMeta(data_freshness=freshness)
    pag = PaginationMeta(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=total,
        has_next=(pagination.offset + pagination.page_size) < total,
    )
    envelope = build_envelope(data=[r.model_dump() for r in data], meta=meta, pagination=pag)

    ttl = _CACHE_TTL_LIVE if not to_date or to_date >= date.today() else _CACHE_TTL_HIST
    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=ttl)

    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/equity/universe",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Equity instrument universe",
)
async def get_equity_universe(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    active_only: Optional[bool] = Query(default=True, description="Filter to active instruments only"),
    index_filter: Optional[str] = Query(default=None, description="Filter by index: nifty_50, nifty_200, nifty_500"),
    sector: Optional[str] = Query(default=None, description="Filter by sector"),
) -> EnvelopeResponse:
    """Return the equity universe from de_instrument."""
    cache_key = f"universe:{active_only}:{index_filter}:{sector}:{pagination.page}:{pagination.page_size}"

    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if active_only:
        filters.append(DeInstrument.is_active.is_(True))
    if index_filter == "nifty_50":
        filters.append(DeInstrument.nifty_50.is_(True))
    elif index_filter == "nifty_200":
        filters.append(DeInstrument.nifty_200.is_(True))
    elif index_filter == "nifty_500":
        filters.append(DeInstrument.nifty_500.is_(True))
    elif index_filter is not None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="index_filter must be one of: nifty_50, nifty_200, nifty_500",
        )
    if sector:
        filters.append(sa.func.lower(DeInstrument.sector) == sector.lower())

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeInstrument).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeInstrument)
        .where(*filters)
        .order_by(DeInstrument.current_symbol.asc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        InstrumentRow(
            symbol=r.current_symbol,
            isin=r.isin,
            company_name=r.company_name,
            exchange=r.exchange,
            sector=r.sector,
            nifty_50=r.nifty_50,
            nifty_200=r.nifty_200,
            nifty_500=r.nifty_500,
            is_active=r.is_active,
        )
        for r in rows
    ]

    meta = ResponseMeta()
    pag = PaginationMeta(
        page=pagination.page,
        page_size=pagination.page_size,
        total_count=total,
        has_next=(pagination.offset + pagination.page_size) < total,
    )
    envelope = build_envelope(data=[r.model_dump() for r in data], meta=meta, pagination=pag)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_HIST)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/rs/stocks",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="RS scores for all stocks",
)
async def get_rs_stocks(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    benchmark: Optional[str] = Query(default="NIFTY_50", description="Benchmark for RS comparison"),
    as_of_date: Optional[date] = Query(default=None, description="RS as of this date (defaults to latest)"),
) -> EnvelopeResponse:
    """Return RS scores for all stocks, sorted by composite RS descending."""
    bm = (benchmark or "NIFTY_50").upper()

    # Resolve date
    if as_of_date is None:
        date_result = await db.execute(
            sa.select(sa.func.max(DeRsDailySummary.date)).where(
                DeRsDailySummary.vs_benchmark == bm
            )
        )
        as_of_date = date_result.scalar_one_or_none()
        if as_of_date is None:
            meta = ResponseMeta(data_freshness=DataFreshness.STALE)
            envelope = build_envelope(data=[], meta=meta, pagination=PaginationMeta())
            response.headers.update(envelope_headers(meta))
            return envelope

    cache_key = f"rs:stocks:{bm}:{as_of_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters = [
        DeRsDailySummary.date == as_of_date,
        DeRsDailySummary.vs_benchmark == bm,
    ]

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeRsDailySummary).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeRsDailySummary)
        .where(*filters)
        .order_by(DeRsDailySummary.rs_composite.desc().nulls_last())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        RsRow(
            symbol=r.symbol,
            sector=r.sector,
            rs_composite=r.rs_composite,
            rs_1m=r.rs_1m,
            rs_3m=r.rs_3m,
            vs_benchmark=r.vs_benchmark,
            date=r.date,
        ).model_dump()
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
    "/rs/sectors",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="RS scores aggregated by sector",
)
async def get_rs_sectors(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    benchmark: Optional[str] = Query(default="NIFTY_50", description="Benchmark"),
    as_of_date: Optional[date] = Query(default=None, description="Date for RS (defaults to latest)"),
) -> EnvelopeResponse:
    """Return average RS scores grouped by sector."""
    bm = (benchmark or "NIFTY_50").upper()

    if as_of_date is None:
        date_result = await db.execute(
            sa.select(sa.func.max(DeRsDailySummary.date)).where(
                DeRsDailySummary.vs_benchmark == bm
            )
        )
        as_of_date = date_result.scalar_one_or_none()
        if as_of_date is None:
            meta = ResponseMeta(data_freshness=DataFreshness.STALE)
            envelope = build_envelope(data=[], meta=meta)
            response.headers.update(envelope_headers(meta))
            return envelope

    cache_key = f"rs:sectors:{bm}:{as_of_date}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    rows_result = await db.execute(
        sa.select(
            DeRsDailySummary.sector,
            sa.func.avg(DeRsDailySummary.rs_composite).label("avg_rs_composite"),
            sa.func.avg(DeRsDailySummary.rs_1m).label("avg_rs_1m"),
            sa.func.avg(DeRsDailySummary.rs_3m).label("avg_rs_3m"),
            sa.func.count().label("stock_count"),
        )
        .where(
            DeRsDailySummary.date == as_of_date,
            DeRsDailySummary.vs_benchmark == bm,
            DeRsDailySummary.sector.isnot(None),
        )
        .group_by(DeRsDailySummary.sector)
        .order_by(sa.desc("avg_rs_composite").nulls_last())
    )
    rows = rows_result.all()

    data = [
        {
            "sector": r.sector,
            "avg_rs_composite": float(r.avg_rs_composite) if r.avg_rs_composite else None,
            "avg_rs_1m": float(r.avg_rs_1m) if r.avg_rs_1m else None,
            "avg_rs_3m": float(r.avg_rs_3m) if r.avg_rs_3m else None,
            "stock_count": r.stock_count,
            "date": str(as_of_date),
            "benchmark": bm,
        }
        for r in rows
    ]

    meta = ResponseMeta()
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/rs/stock/{symbol}",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="RS score for a single symbol",
)
async def get_rs_stock(
    symbol: str,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    benchmark: Optional[str] = Query(default="NIFTY_50", description="Benchmark"),
    as_of_date: Optional[date] = Query(default=None, description="Date (defaults to latest)"),
) -> EnvelopeResponse:
    """Return RS score for a specific symbol."""
    upper = symbol.upper()
    bm = (benchmark or "NIFTY_50").upper()

    instrument_id = await resolve_symbol_or_404(upper, db, redis)

    if as_of_date is None:
        date_result = await db.execute(
            sa.select(sa.func.max(DeRsScores.date)).where(
                DeRsScores.entity_id == str(instrument_id),
                DeRsScores.vs_benchmark == bm,
            )
        )
        as_of_date = date_result.scalar_one_or_none()
        if as_of_date is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No RS data found for symbol '{upper}'",
            )

    result = await db.execute(
        sa.select(DeRsScores).where(
            DeRsScores.entity_id == str(instrument_id),
            DeRsScores.vs_benchmark == bm,
            DeRsScores.date == as_of_date,
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"RS data not found for '{upper}' on {as_of_date}",
        )

    data = {
        "symbol": upper,
        "date": str(row.date),
        "vs_benchmark": row.vs_benchmark,
        "rs_1w": row.rs_1w,
        "rs_1m": row.rs_1m,
        "rs_3m": row.rs_3m,
        "rs_6m": row.rs_6m,
        "rs_12m": row.rs_12m,
        "rs_composite": row.rs_composite,
        "computation_version": row.computation_version,
    }

    meta = ResponseMeta(computation_version=row.computation_version)
    envelope = build_envelope(data=data, meta=meta)
    response.headers.update(envelope_headers(meta))
    return envelope
