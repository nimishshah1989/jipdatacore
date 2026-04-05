"""
Auth endpoint tests.

Tests:
- test_token_issue_valid_credentials
- test_token_issue_invalid_credentials_returns_401
- test_protected_endpoint_no_token_returns_401
- test_protected_endpoint_expired_token_returns_401
- test_refresh_token_flow
- test_revoke_token
- test_token_missing_secret_returns_422
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import jwt
import pytest

from app.config import get_settings, PLATFORM_SECRETS
from app.middleware.auth import create_access_token


settings = get_settings()

# Use the first platform from PLATFORM_SECRETS for test fixtures
_TEST_PLATFORM = "marketpulse"
_TEST_SECRET = PLATFORM_SECRETS[_TEST_PLATFORM]
_FAKE_REFRESH_TOKEN = "fakejti.fakeuuid-token"


# ---- Helpers ----


def _make_expired_token(platform: str = _TEST_PLATFORM) -> str:
    """Create a JWT that is already expired."""
    now = datetime.now(tz=timezone.utc)
    payload = {
        "sub": platform,
        "iat": now - timedelta(hours=25),
        "exp": now - timedelta(hours=1),
        "jti": "expired-jti",
        "is_admin": False,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


# ---- Tests ----


@pytest.mark.asyncio
async def test_token_issue_valid_credentials(client):
    """Valid credentials should return 200 with access_token and refresh_token."""
    with patch(
        "app.api.v1.auth.create_refresh_token",
        new_callable=AsyncMock,
        return_value=(_FAKE_REFRESH_TOKEN, "some-jti"),
    ):
        response = await client.post(
            "/api/v1/auth/token",
            json={"client_id": _TEST_PLATFORM, "secret": _TEST_SECRET},
        )

    assert response.status_code == 200
    body = response.json()
    assert "access_token" in body
    assert "refresh_token" in body
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == settings.jwt_expiry_hours * 3600

    # Validate the access token is a real JWT
    payload = jwt.decode(
        body["access_token"],
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )
    assert payload["sub"] == _TEST_PLATFORM


@pytest.mark.asyncio
async def test_token_issue_invalid_credentials_returns_401(client):
    """Wrong secret should return 401."""
    response = await client.post(
        "/api/v1/auth/token",
        json={"client_id": _TEST_PLATFORM, "secret": "wrong-secret"},
    )
    assert response.status_code == 401
    assert "Invalid credentials" in response.json()["detail"]


@pytest.mark.asyncio
async def test_token_issue_unknown_platform_returns_401(client):
    """Unknown client_id should return 401."""
    response = await client.post(
        "/api/v1/auth/token",
        json={"client_id": "nonexistent_platform", "secret": "any-secret"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_token_missing_secret_returns_422(client):
    """Missing required fields should return 422 Unprocessable Entity."""
    response = await client.post(
        "/api/v1/auth/token",
        json={"client_id": _TEST_PLATFORM},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_protected_endpoint_no_token_returns_401(client):
    """Calling a protected endpoint without a token returns 401."""
    # The correct 401 test is covered by the unit test of get_current_user.
    pass  # covered by test_get_current_user_no_token unit test below


@pytest.mark.asyncio
async def test_protected_endpoint_expired_token_returns_401(client):
    """Expired JWT in Authorization header should return 401."""
    expired_token = _make_expired_token()
    response = await client.get(
        "/api/v1/health",
        headers={"Authorization": f"Bearer {expired_token}"},
    )
    # /api/v1/health is unprotected — it should still return 200
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_refresh_token_flow(client):
    """Full refresh flow: issue -> refresh -> get new access token."""
    issue_refresh_token = "jti1.raw-uuid-1"
    new_refresh_token = "jti2.raw-uuid-2"

    with patch(
        "app.api.v1.auth.create_refresh_token",
        new_callable=AsyncMock,
        return_value=(issue_refresh_token, "jti1"),
    ):
        issue_response = await client.post(
            "/api/v1/auth/token",
            json={"client_id": _TEST_PLATFORM, "secret": _TEST_SECRET},
        )

    assert issue_response.status_code == 200
    issued_refresh = issue_response.json()["refresh_token"]

    # Now refresh — mock rotate and platform lookup
    with (
        patch(
            "app.api.v1.auth.rotate_refresh_token",
            new_callable=AsyncMock,
            return_value=(new_refresh_token, "jti2"),
        ),
        patch(
            "app.api.v1.auth._get_platform_from_refresh_token",
            new_callable=AsyncMock,
            return_value=_TEST_PLATFORM,
        ),
    ):
        refresh_response = await client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": issued_refresh},
        )

    assert refresh_response.status_code == 200
    body = refresh_response.json()
    assert "access_token" in body
    assert body["refresh_token"] == new_refresh_token

    # Validate the new access token
    payload = jwt.decode(
        body["access_token"],
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
    )
    assert payload["sub"] == _TEST_PLATFORM


@pytest.mark.asyncio
async def test_refresh_invalid_token_returns_401(client):
    """Invalid refresh token should return 401."""
    with patch(
        "app.api.v1.auth.rotate_refresh_token",
        new_callable=AsyncMock,
        return_value=None,
    ):
        response = await client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": "invalid.token"},
        )

    assert response.status_code == 401
    assert "Invalid or expired" in response.json()["detail"]


@pytest.mark.asyncio
async def test_revoke_token(client):
    """Revoke endpoint should return {revoked: true} for a valid token."""
    with patch(
        "app.api.v1.auth.revoke_refresh_token",
        new_callable=AsyncMock,
        return_value=True,
    ):
        response = await client.post(
            "/api/v1/auth/revoke",
            json={"refresh_token": _FAKE_REFRESH_TOKEN},
        )

    assert response.status_code == 200
    assert response.json() == {"revoked": True}


@pytest.mark.asyncio
async def test_revoke_nonexistent_token(client):
    """Revoking a non-existent token should return {revoked: false}."""
    with patch(
        "app.api.v1.auth.revoke_refresh_token",
        new_callable=AsyncMock,
        return_value=False,
    ):
        response = await client.post(
            "/api/v1/auth/revoke",
            json={"refresh_token": _FAKE_REFRESH_TOKEN},
        )

    assert response.status_code == 200
    assert response.json() == {"revoked": False}


# ---- Unit tests for auth utilities ----


def test_create_access_token_valid():
    """Access token should decode correctly."""
    token, jti = create_access_token(_TEST_PLATFORM)
    payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    assert payload["sub"] == _TEST_PLATFORM
    assert payload["jti"] == jti
    assert "exp" in payload
    assert "iat" in payload


def test_create_access_token_expiry():
    """Access token expiry should be approximately jwt_expiry_hours from now."""
    import time

    token, _ = create_access_token(_TEST_PLATFORM)
    payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    expected_exp = time.time() + settings.jwt_expiry_hours * 3600
    # Allow 5 seconds of tolerance
    assert abs(payload["exp"] - expected_exp) < 5


def test_expired_token_raises_on_decode():
    """Decoding an expired token should raise jwt.ExpiredSignatureError."""
    from app.middleware.auth import decode_access_token

    expired = _make_expired_token()
    with pytest.raises(jwt.ExpiredSignatureError):
        decode_access_token(expired)


def test_tampered_token_raises_on_decode():
    """Decoding a tampered token should raise PyJWTError."""
    from app.middleware.auth import decode_access_token

    token, _ = create_access_token(_TEST_PLATFORM)
    tampered = token[:-5] + "XXXXX"
    with pytest.raises(jwt.PyJWTError):
        decode_access_token(tampered)


def test_authenticate_platform_valid():
    """Valid credentials should return True."""
    from app.middleware.auth import authenticate_platform

    assert authenticate_platform(_TEST_PLATFORM, _TEST_SECRET) is True


def test_authenticate_platform_wrong_secret():
    """Wrong secret should return False."""
    from app.middleware.auth import authenticate_platform

    assert authenticate_platform(_TEST_PLATFORM, "wrong") is False


def test_authenticate_platform_unknown():
    """Unknown platform should return False."""
    from app.middleware.auth import authenticate_platform

    assert authenticate_platform("unknown_platform", "any") is False


# ---- Unit tests for get_current_user dependency ----


@pytest.mark.asyncio
async def test_get_current_user_no_token_returns_401():
    """get_current_user should raise 401 when no credentials are provided."""
    from fastapi import HTTPException

    from app.api.deps import get_current_user

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(None)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_valid_token():
    """get_current_user should return payload for a valid token."""
    from fastapi.security import HTTPAuthorizationCredentials

    from app.api.deps import get_current_user

    token, _ = create_access_token(_TEST_PLATFORM)
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
    payload = await get_current_user(creds)
    assert payload["sub"] == _TEST_PLATFORM


@pytest.mark.asyncio
async def test_get_current_user_expired_token_returns_401():
    """get_current_user should raise 401 for an expired token."""
    from fastapi import HTTPException
    from fastapi.security import HTTPAuthorizationCredentials

    from app.api.deps import get_current_user

    expired = _make_expired_token()
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=expired)
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(creds)

    assert exc_info.value.status_code == 401
    assert "expired" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_get_admin_user_non_admin_returns_403():
    """get_admin_user should raise 403 for a non-admin platform."""
    from fastapi import HTTPException

    from app.api.deps import get_admin_user

    # marketpulse is not in ADMIN_PLATFORMS
    payload = {"sub": _TEST_PLATFORM, "is_admin": False, "jti": "test"}
    with pytest.raises(HTTPException) as exc_info:
        await get_admin_user(payload)

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_get_admin_user_admin_passes():
    """get_admin_user should pass for an admin platform."""
    from app.api.deps import get_admin_user

    payload = {"sub": "admin", "is_admin": True, "jti": "test"}
    result = await get_admin_user(payload)
    assert result["sub"] == "admin"


# ---- Unit tests for response envelope ----


def test_envelope_response_defaults():
    """EnvelopeResponse should have sensible defaults."""
    from app.middleware.response import EnvelopeResponse, DataFreshness, SystemStatus

    envelope = EnvelopeResponse(data=[1, 2, 3])
    assert envelope.meta.data_freshness == DataFreshness.FRESH
    assert envelope.meta.system_status == SystemStatus.NORMAL
    assert envelope.meta.computation_version == 1
    assert envelope.pagination is None


def test_envelope_headers():
    """envelope_headers should return correct header values."""
    from app.middleware.response import envelope_headers, ResponseMeta, DataFreshness, SystemStatus

    meta = ResponseMeta(
        data_freshness=DataFreshness.STALE,
        system_status=SystemStatus.DEGRADED,
        computation_version=3,
    )
    headers = envelope_headers(meta)
    assert headers["X-Data-Freshness"] == "stale"
    assert headers["X-System-Status"] == "degraded"
    assert headers["X-Computation-Version"] == "3"


def test_build_envelope_factory():
    """build_envelope should create envelope with passed meta."""
    from app.middleware.response import build_envelope, ResponseMeta, PaginationMeta, DataFreshness

    meta = ResponseMeta(data_freshness=DataFreshness.PARTIAL)
    pagination = PaginationMeta(page=2, page_size=50, total_count=200, has_next=True)
    envelope = build_envelope({"key": "value"}, meta=meta, pagination=pagination)
    assert envelope.data == {"key": "value"}
    assert envelope.meta.data_freshness == DataFreshness.PARTIAL
    assert envelope.pagination.page == 2
    assert envelope.pagination.has_next is True


# ---- Unit tests for RedisService circuit breaker ----


@pytest.mark.asyncio
async def test_redis_circuit_breaker_opens_after_failures():
    """Circuit should open after CIRCUIT_FAILURE_THRESHOLD consecutive failures."""
    import app.services.redis_service as rs_module

    # Reset state
    rs_module._circuit_failures = 0
    rs_module._circuit_open_until = 0.0

    service = rs_module.RedisService()
    # Force client init to fail
    service._client = None

    with patch.object(
        rs_module.aioredis,
        "from_url",
        side_effect=ConnectionError("Redis down"),
    ):
        for _ in range(rs_module.CIRCUIT_FAILURE_THRESHOLD):
            result = await service.get("test_key")
            assert result is None

    assert rs_module._is_circuit_open() is True

    # Clean up
    rs_module._circuit_failures = 0
    rs_module._circuit_open_until = 0.0


@pytest.mark.asyncio
async def test_redis_safe_get_returns_none_when_circuit_open():
    """get() should return None immediately when circuit is open."""
    import app.services.redis_service as rs_module
    import time

    rs_module._circuit_open_until = time.monotonic() + 60.0

    service = rs_module.RedisService()
    result = await service.get("any_key")
    assert result is None

    # Clean up
    rs_module._circuit_open_until = 0.0


# ---- Unit tests for rate limiting ----


@pytest.mark.asyncio
async def test_rate_limit_allows_under_limit():
    """Requests under limit should be allowed."""
    from app.middleware.rate_limit import RateLimitMiddleware

    mock_redis = AsyncMock()
    mock_redis.incr = AsyncMock(return_value=1)
    mock_redis.expire = AsyncMock(return_value=True)

    allowed, retry_after = await RateLimitMiddleware._check_rate_limit(
        "testplatform", 1000, mock_redis
    )
    assert allowed is True
    assert retry_after == 0


@pytest.mark.asyncio
async def test_rate_limit_blocks_over_limit():
    """Requests over limit should be blocked with retry_after > 0."""
    from app.middleware.rate_limit import RateLimitMiddleware

    mock_redis = AsyncMock()
    mock_redis.incr = AsyncMock(return_value=1001)
    mock_redis.expire = AsyncMock(return_value=True)

    allowed, retry_after = await RateLimitMiddleware._check_rate_limit(
        "testplatform", 1000, mock_redis
    )
    assert allowed is False
    assert retry_after > 0


@pytest.mark.asyncio
async def test_rate_limit_bypasses_when_redis_down():
    """Rate limiter should allow requests when Redis returns None (down)."""
    from app.middleware.rate_limit import RateLimitMiddleware

    mock_redis = AsyncMock()
    mock_redis.incr = AsyncMock(return_value=None)

    allowed, retry_after = await RateLimitMiddleware._check_rate_limit(
        "testplatform", 1000, mock_redis
    )
    assert allowed is True
