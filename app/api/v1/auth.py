"""
Auth endpoints.

POST /api/v1/auth/token   — issue access + refresh tokens
POST /api/v1/auth/refresh — rotate refresh token, issue new pair
POST /api/v1/auth/revoke  — revoke refresh token (logout)
"""

from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.logging import get_logger
from app.middleware.auth import (
    authenticate_platform,
    create_access_token,
    create_refresh_token,
    revoke_refresh_token,
    rotate_refresh_token,
)
from app.api.deps import get_redis
from app.services.redis_service import RedisService

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# ---- Request/Response schemas ----


class TokenRequest(BaseModel):
    client_id: str = Field(..., min_length=1, max_length=64)
    secret: str = Field(..., min_length=1, max_length=256)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=1)


class RevokeRequest(BaseModel):
    refresh_token: str = Field(..., min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = 86400  # seconds


class RevokeResponse(BaseModel):
    revoked: bool


# ---- Endpoints ----


@router.post(
    "/token",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="Issue JWT access + refresh tokens",
)
async def issue_token(
    body: TokenRequest,
    redis: Annotated[RedisService, Depends(get_redis)],
) -> TokenResponse:
    """Authenticate with client_id + secret and receive a JWT pair."""
    if not authenticate_platform(body.client_id, body.secret):
        logger.warning("auth_token_invalid_credentials", client_id=body.client_id)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    access_token, _jti = create_access_token(body.client_id)
    refresh_token, _rt_jti = await create_refresh_token(body.client_id, redis)

    from app.config import get_settings

    settings = get_settings()

    logger.info("auth_token_issued", platform=body.client_id)
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=settings.jwt_expiry_hours * 3600,
    )


@router.post(
    "/refresh",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="Refresh JWT tokens",
)
async def refresh_token(
    body: RefreshRequest,
    redis: Annotated[RedisService, Depends(get_redis)],
) -> TokenResponse:
    """Rotate refresh token — invalidates old token and issues a new pair."""
    result = await rotate_refresh_token(body.refresh_token, redis)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    new_refresh_token, _jti = result

    # We need the platform from the new refresh token — extract from the old token
    # Platform is embedded in the Redis value; since token was already rotated,
    # we issue access token by verifying the new refresh token
    platform = await _get_platform_from_refresh_token(new_refresh_token, redis)
    if platform is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Token rotation error",
        )

    access_token, _at_jti = create_access_token(platform)

    from app.config import get_settings

    settings = get_settings()

    logger.info("auth_token_refreshed", platform=platform)
    return TokenResponse(
        access_token=access_token,
        refresh_token=new_refresh_token,
        expires_in=settings.jwt_expiry_hours * 3600,
    )


@router.post(
    "/revoke",
    response_model=RevokeResponse,
    status_code=status.HTTP_200_OK,
    summary="Revoke refresh token (logout)",
)
async def revoke_token(
    body: RevokeRequest,
    redis: Annotated[RedisService, Depends(get_redis)],
) -> RevokeResponse:
    """Invalidate a refresh token — use on logout."""
    revoked = await revoke_refresh_token(body.refresh_token, redis)
    logger.info("auth_token_revoked", revoked=revoked)
    return RevokeResponse(revoked=revoked)


# ---- Internal helpers ----


async def _get_platform_from_refresh_token(
    composite_token: str,
    redis: RedisService,
) -> Optional[str]:
    """Look up platform from stored refresh token without consuming it."""
    try:
        jti = composite_token.split(".", 1)[0]
    except (ValueError, IndexError):
        return None

    stored = await redis.get(f"rt:{jti}")
    if stored is None:
        return None

    try:
        platform, _ = stored.split(":", 1)
        return platform
    except ValueError:
        return None
