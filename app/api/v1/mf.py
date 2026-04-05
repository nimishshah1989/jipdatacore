"""
Mutual Fund API endpoints.

GET /api/v1/mf/nav/{mstar_id}         — NAV history for a Morningstar fund
GET /api/v1/mf/universe               — MF universe (fund master)
GET /api/v1/mf/category-flows         — Monthly MF category flows
GET /api/v1/mf/derived/{mstar_id}     — Derived stats (trailing returns, 52wk range)
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
    EnvelopeResponse,
    PaginationMeta,
    ResponseMeta,
    build_envelope,
    envelope_headers,
)
from app.models.flows import DeMfCategoryFlows
from app.models.instruments import DeMfMaster
from app.models.prices import DeMfNavDaily
from app.services.data_freshness import check_mf_freshness
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/mf", tags=["mf"])

_CACHE_TTL_LIVE = 3_600   # 1 h
_CACHE_TTL_HIST = 86_400  # 24 h


# ---- Response schemas ----


class NavRow(BaseModel):
    nav_date: date
    nav: Any
    nav_adj: Optional[Any] = None
    nav_change: Optional[Any] = None
    nav_change_pct: Optional[Any] = None


class MfUniverseRow(BaseModel):
    mstar_id: str
    amfi_code: Optional[str] = None
    fund_name: str
    amc_name: Optional[str] = None
    category_name: Optional[str] = None
    broad_category: Optional[str] = None
    is_index_fund: bool
    is_etf: bool
    is_active: bool
    expense_ratio: Optional[Any] = None


class CategoryFlowRow(BaseModel):
    month_date: date
    category: str
    net_flow_cr: Optional[Any] = None
    gross_inflow_cr: Optional[Any] = None
    gross_outflow_cr: Optional[Any] = None
    aum_cr: Optional[Any] = None
    sip_flow_cr: Optional[Any] = None


# ---- Helpers ----


async def _assert_mf_exists(mstar_id: str, db: AsyncSession) -> DeMfMaster:
    result = await db.execute(
        sa.select(DeMfMaster).where(DeMfMaster.mstar_id == mstar_id)
    )
    fund = result.scalar_one_or_none()
    if fund is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Fund '{mstar_id}' not found",
        )
    return fund


# ---- Endpoints ----


@router.get(
    "/nav/{mstar_id}",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="NAV history for a mutual fund",
)
async def get_mf_nav(
    mstar_id: str,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start date YYYY-MM-DD"),
    to_date: Optional[date] = Query(default=None, description="End date YYYY-MM-DD"),
) -> EnvelopeResponse:
    """Return NAV history for a fund. Only validated data is returned."""
    await _assert_mf_exists(mstar_id, db)

    cache_key = f"mf:nav:{mstar_id}:{from_date}:{to_date}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters = [
        DeMfNavDaily.mstar_id == mstar_id,
        DeMfNavDaily.data_status == "validated",
    ]
    if from_date:
        filters.append(DeMfNavDaily.nav_date >= from_date)
    if to_date:
        filters.append(DeMfNavDaily.nav_date <= to_date)

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeMfNavDaily).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeMfNavDaily)
        .where(*filters)
        .order_by(DeMfNavDaily.nav_date.desc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        NavRow(
            nav_date=r.nav_date,
            nav=r.nav,
            nav_adj=r.nav_adj,
            nav_change=r.nav_change,
            nav_change_pct=r.nav_change_pct,
        ).model_dump()
        for r in rows
    ]

    freshness = await check_mf_freshness(db)
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
    "/universe",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="MF universe (fund master)",
)
async def get_mf_universe(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    active_only: Optional[bool] = Query(default=True, description="Active funds only"),
    amc: Optional[str] = Query(default=None, description="Filter by AMC name (case-insensitive)"),
    category: Optional[str] = Query(default=None, description="Filter by category_name"),
    broad_category: Optional[str] = Query(default=None, description="Filter by broad_category"),
    is_etf: Optional[bool] = Query(default=None, description="Filter ETFs"),
    is_index: Optional[bool] = Query(default=None, description="Filter index funds"),
) -> EnvelopeResponse:
    """Return the MF universe from de_mf_master."""
    cache_key = (
        f"mf:universe:{active_only}:{amc}:{category}:{broad_category}"
        f":{is_etf}:{is_index}:{pagination.page}:{pagination.page_size}"
    )
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if active_only:
        filters.append(DeMfMaster.is_active.is_(True))
    if amc:
        filters.append(sa.func.lower(DeMfMaster.amc_name).contains(amc.lower()))
    if category:
        filters.append(sa.func.lower(DeMfMaster.category_name) == category.lower())
    if broad_category:
        filters.append(sa.func.lower(DeMfMaster.broad_category) == broad_category.lower())
    if is_etf is not None:
        filters.append(DeMfMaster.is_etf.is_(is_etf))
    if is_index is not None:
        filters.append(DeMfMaster.is_index_fund.is_(is_index))

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeMfMaster).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeMfMaster)
        .where(*filters)
        .order_by(DeMfMaster.fund_name.asc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        MfUniverseRow(
            mstar_id=r.mstar_id,
            amfi_code=r.amfi_code,
            fund_name=r.fund_name,
            amc_name=r.amc_name,
            category_name=r.category_name,
            broad_category=r.broad_category,
            is_index_fund=r.is_index_fund,
            is_etf=r.is_etf,
            is_active=r.is_active,
            expense_ratio=r.expense_ratio,
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

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_HIST)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/category-flows",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Monthly MF category flows",
)
async def get_mf_category_flows(
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
    pagination: Annotated[PaginationParams, Depends()],
    from_date: Optional[date] = Query(default=None, description="Start month date"),
    to_date: Optional[date] = Query(default=None, description="End month date"),
    category: Optional[str] = Query(default=None, description="Filter by category"),
) -> EnvelopeResponse:
    """Return monthly MF category-level flows and AUM."""
    cache_key = f"mf:catflows:{from_date}:{to_date}:{category}:{pagination.page}:{pagination.page_size}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    filters: list = []
    if from_date:
        filters.append(DeMfCategoryFlows.month_date >= from_date)
    if to_date:
        filters.append(DeMfCategoryFlows.month_date <= to_date)
    if category:
        filters.append(sa.func.lower(DeMfCategoryFlows.category) == category.lower())

    count_result = await db.execute(
        sa.select(sa.func.count()).select_from(DeMfCategoryFlows).where(*filters)
    )
    total = count_result.scalar_one()

    rows_result = await db.execute(
        sa.select(DeMfCategoryFlows)
        .where(*filters)
        .order_by(DeMfCategoryFlows.month_date.desc(), DeMfCategoryFlows.category.asc())
        .offset(pagination.offset)
        .limit(pagination.page_size)
    )
    rows = rows_result.scalars().all()

    data = [
        CategoryFlowRow(
            month_date=r.month_date,
            category=r.category,
            net_flow_cr=r.net_flow_cr,
            gross_inflow_cr=r.gross_inflow_cr,
            gross_outflow_cr=r.gross_outflow_cr,
            aum_cr=r.aum_cr,
            sip_flow_cr=r.sip_flow_cr,
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

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_HIST)
    response.headers.update(envelope_headers(meta))
    return envelope


@router.get(
    "/derived/{mstar_id}",
    response_model=EnvelopeResponse,
    status_code=status.HTTP_200_OK,
    summary="Derived stats for a fund (trailing returns, 52wk range)",
)
async def get_mf_derived(
    mstar_id: str,
    response: Response,
    _user: Annotated[dict, Depends(get_current_user)],
    db: Annotated[AsyncSession, Depends(get_db)],
    redis: Annotated[RedisService, Depends(get_redis)],
) -> EnvelopeResponse:
    """Return latest derived stats from de_mf_nav_daily for a fund."""
    await _assert_mf_exists(mstar_id, db)

    cache_key = f"mf:derived:{mstar_id}"
    cached = await redis.get(cache_key)
    if cached:
        return EnvelopeResponse(**json.loads(cached))

    result = await db.execute(
        sa.select(DeMfNavDaily)
        .where(
            DeMfNavDaily.mstar_id == mstar_id,
            DeMfNavDaily.data_status == "validated",
        )
        .order_by(DeMfNavDaily.nav_date.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No validated NAV data for fund '{mstar_id}'",
        )

    data = {
        "mstar_id": mstar_id,
        "nav_date": str(row.nav_date),
        "nav": row.nav,
        "return_1d": row.return_1d,
        "return_1w": row.return_1w,
        "return_1m": row.return_1m,
        "return_3m": row.return_3m,
        "return_6m": row.return_6m,
        "return_1y": row.return_1y,
        "return_3y": row.return_3y,
        "return_5y": row.return_5y,
        "return_10y": row.return_10y,
        "nav_52wk_high": row.nav_52wk_high,
        "nav_52wk_low": row.nav_52wk_low,
    }

    freshness = await check_mf_freshness(db)
    meta = ResponseMeta(data_freshness=freshness)
    envelope = build_envelope(data=data, meta=meta)

    await redis.set(cache_key, envelope.model_dump_json(), ttl_seconds=_CACHE_TTL_LIVE)
    response.headers.update(envelope_headers(meta))
    return envelope
