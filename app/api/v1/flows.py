"""
Flows API endpoints.

GET /api/v1/flows/fii-dii     — FII/DII institutional flows
GET /api/v1/flows/fo-summary  — F&O summary (PCR, OI, FII positions)
"""

import json
from datetime import date
from typing import Annotated, Optional

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PaginationParams, get_current_user, get_db, get_redis
from app.logging import get_logger
from app.middleware.response import (
    EnvelopeResponse,
    PaginationMeta,
    ResponseMeta,
    build_envelope,
    envelope_headers,
)
from app.models.computed import DeFoSummary
from app.models.flows import DeInstitutionalFlows
from app.services.data_freshness import check_flows_freshness
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/flows", tags=["flows"])

_CACHE_TTL_LIVE = 3_600   # 1 h
_CACHE_TTL_HIST = 86_400  # 24 h


@router.get(
    "/fii-dii",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="FII/DII institutional flows",
)
async def get_fii_dii_flows(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
    category: Optional[str] = Query(
        default=None,
        description="Filter by category: FII, DII, MF, Insurance, Banks, Corporates, Retail, Other",
    ),
    market_type: Optional[str] = Query(
        default=None,
        description="Filter by market type: equity, debt, hybrid, derivatives",
    ),
) -> EnvelopeResponse:
    """Return institutional flow data with optional filters."""
    cache_key = (
        f"flows:fiidii:{from_date}:{to_date}:{category}:{market_type}"
        f":{pagination.page}:{pagination.page_size}"
    )
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if from_date:
        filters.append(DeInstitutionalFlows.date >= from_date)
    if to_date:
        filters.append(DeInstitutionalFlows.date <= to_date)
    if category:
        filters.append(DeInstitutionalFlows.category == category.upper())
    if market_type:
        filters.append(DeInstitutionalFlows.market_type == market_type.lower())

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeInstitutionalFlows).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeInstitutionalFlows)
        .where(*filters)
        .order_by(DeInstitutionalFlows.date.desc(), DeInstitutionalFlows.category.asc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "date": str(r.date),
            "category": r.category,
            "market_type": r.market_type,
            "gross_buy": r.gross_buy,
            "gross_sell": r.gross_sell,
            "net_flow": r.net_flow,
            "source": r.source,
        }
        for r in rows
    ]

    freshness = await check_flows_freshness(db)
    meta = ResponseMeta(data_freshness=freshness)
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
    "/fo-summary",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="F&O market summary (PCR, OI, FII positions)",
)
async def get_fo_summary(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date"),
    to_date: Optional[date] = Query(default=None, description="End date"),
) -> EnvelopeResponse:
    """Return F&O summary statistics."""
    cache_key = f"flows:fo:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if from_date:
        filters.append(DeFoSummary.date >= from_date)
    if to_date:
        filters.append(DeFoSummary.date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeFoSummary).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeFoSummary)
        .where(*filters)
        .order_by(DeFoSummary.date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        {
            "date": str(r.date),
            "pcr_oi": r.pcr_oi,
            "pcr_volume": r.pcr_volume,
            "total_oi": r.total_oi,
            "oi_change": r.oi_change,
            "fii_index_long": r.fii_index_long,
            "fii_index_short": r.fii_index_short,
            "fii_net_futures": r.fii_net_futures,
            "fii_net_options": r.fii_net_options,
            "max_pain": r.max_pain,
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
