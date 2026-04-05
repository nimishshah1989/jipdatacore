"""V1 API Router Export."""

from fastapi import APIRouter
from app.api.v1.equity import router as equity_router
from app.api.v1.mf import router as mf_router
from app.api.v1.market import router as market_router

v1_router = APIRouter()

v1_router.include_router(equity_router)
v1_router.include_router(mf_router)
v1_router.include_router(market_router)
