"""
API v1 router registry.

Import all sub-routers here so main.py can do:
    from app.api.v1 import all_routers
    for r in all_routers:
        app.include_router(r)
"""

from app.api.v1.admin import router as admin_router
from app.api.v1.auth import router as auth_router
from app.api.v1.equity import router as equity_router
from app.api.v1.flows import router as flows_router
from app.api.v1.market import router as market_router
from app.api.v1.mf import router as mf_router
from app.api.v1.qualitative import router as qualitative_router

all_routers = [
    auth_router,
    equity_router,
    mf_router,
    market_router,
    flows_router,
    qualitative_router,
    admin_router,
]

__all__ = [
    "all_routers",
    "auth_router",
    "equity_router",
    "mf_router",
    "market_router",
    "flows_router",
    "qualitative_router",
    "admin_router",
]
