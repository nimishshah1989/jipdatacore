"""Mutual Fund API Endpoints."""

from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.symbol_resolver import format_envelope
from app.models.prices import DeMfNavDaily
from app.models.instruments import DeMfMaster

router = APIRouter(prefix="/api/v1/mf", tags=["Mutual Funds"])


@router.get("/nav/{mstar_id}")
async def get_mf_nav(
    mstar_id: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: AsyncSession = Depends(get_db)
):
    """Fetch NAV history and CAGRs for a specific Morningstar fund ID."""
    
    stmt = select(
        DeMfNavDaily.nav_date,
        DeMfNavDaily.nav,
        DeMfNavDaily.nav_adj,
        DeMfNavDaily.return_1d,
        DeMfNavDaily.return_1m,
        DeMfNavDaily.return_1y,
        DeMfNavDaily.return_3y,
        DeMfNavDaily.return_5y,
        DeMfNavDaily.return_10y
    ).where(
        DeMfNavDaily.mstar_id == mstar_id,
        DeMfNavDaily.data_status == 'validated'
    ).order_by(DeMfNavDaily.nav_date.desc())
    
    if from_date:
        stmt = stmt.where(DeMfNavDaily.nav_date >= from_date)
    if to_date:
        stmt = stmt.where(DeMfNavDaily.nav_date <= to_date)
        
    result = await session.execute(stmt)
    records = []
    
    for row in result:
        records.append({
            "date": row.nav_date.strftime("%Y-%m-%d"),
            "nav": float(row.nav) if row.nav else None,
            "nav_adj": float(row.nav_adj) if row.nav_adj else None,
            "return_1d": float(row.return_1d) if row.return_1d else None,
            "return_1m": float(row.return_1m) if row.return_1m else None,
            "return_1y": float(row.return_1y) if row.return_1y else None,
            "return_3y": float(row.return_3y) if row.return_3y else None,
            "return_5y": float(row.return_5y) if row.return_5y else None,
            "return_10y": float(row.return_10y) if row.return_10y else None
        })
        
    return format_envelope(records)


@router.get("/universe")
async def get_mf_universe(
    category: Optional[str] = None,
    session: AsyncSession = Depends(get_db)
):
    """Fetch the master configuration list of tracked actively available funds."""
    
    stmt = select(
        DeMfMaster.mstar_id,
        DeMfMaster.fund_name,
        DeMfMaster.category_name,
        DeMfMaster.expense_ratio,
        DeMfMaster.primary_benchmark
    ).where(DeMfMaster.is_active == True)
    
    if category:
        stmt = stmt.where(DeMfMaster.broad_category.ilike(f"%{category}%"))
        
    result = await session.execute(stmt)
    records = []
    for row in result:
        records.append({
            "mstar_id": row.mstar_id,
            "name": row.fund_name,
            "category": row.category_name,
            "expense_ratio": float(row.expense_ratio) if row.expense_ratio else None,
            "benchmark": row.primary_benchmark
        })
        
    return format_envelope(records)
