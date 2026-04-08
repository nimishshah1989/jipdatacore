import os

# Set test secrets before importing app (config reads env at import time)
os.environ.setdefault("JWT_SECRET", "test-jwt-secret-for-unit-tests")
os.environ.setdefault("PLATFORM_SECRET_MARKETPULSE", "test-mp-secret")
os.environ.setdefault("PLATFORM_SECRET_MFPULSE", "test-mfp-secret")
os.environ.setdefault("PLATFORM_SECRET_CHAMPION", "test-champ-secret")
os.environ.setdefault("PLATFORM_SECRET_ADMIN", "test-admin-secret")

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
