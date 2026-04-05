from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import get_settings
from app.logging import setup_logging, get_logger
from app.db.session import async_engine
from app.middleware.request_logging import RequestLoggingMiddleware
from app.api.v1 import all_routers

settings = get_settings()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger.info("data_engine_starting", version="2.0.0", env=settings.app_env)
    yield
    await async_engine.dispose()
    logger.info("data_engine_shutdown")


app = FastAPI(
    title="JIP Data Engine",
    version="2.0.0",
    description="Single source of truth for all financial data in the Jhaveri Intelligence Platform",
    docs_url="/docs" if settings.app_env != "production" else None,
    redoc_url=None,
    lifespan=lifespan,
)

# CORS — dashboard access only
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.dashboard_origin],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Request logging — must be added after CORS so it wraps all requests
app.add_middleware(RequestLoggingMiddleware)

# Prometheus metrics
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# Routers
for _router in all_routers:
    app.include_router(_router)


@app.get("/health")
async def health():
    return {"status": "healthy", "version": "2.0.0", "service": "data-engine"}


@app.get("/api/v1/health")
async def api_health():
    return {
        "status": "healthy",
        "version": "2.0.0",
        "service": "data-engine",
    }
