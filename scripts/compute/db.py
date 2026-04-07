"""Shared database connection helpers for computation scripts.

Uses DATABASE_URL from environment or falls back to app.config.
Never hardcode credentials in computation scripts — import from here.
"""

import os
from pathlib import Path


def _load_env() -> None:
    """Load .env file if present."""
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = val


_load_env()


def get_sync_url() -> str:
    """Get synchronous (psycopg2) database URL."""
    url = os.environ.get("DATABASE_URL_SYNC") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    if not url.startswith("postgresql://"):
        url = "postgresql://" + url.split("://", 1)[-1] if "://" in url else url
    return url


def get_async_url() -> str:
    """Get asynchronous (asyncpg) database URL."""
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql+psycopg2://"):
        url = url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    return url


def get_mp_url() -> str:
    """Get MarketPulse (fie_v3) database URL."""
    url = os.environ.get("FIE_V3_DATABASE_URL", "")
    if "sslmode" not in url and url:
        url += "?sslmode=require"
    return url
