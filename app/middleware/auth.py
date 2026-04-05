"""
JWT authentication utilities.

- Issue access + refresh tokens
- Validate bearer tokens
- Revoke refresh tokens via Redis
- Refresh token stored as bcrypt hash in Redis; rotated on every use
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt
from app.config import get_settings, PLATFORM_SECRETS, ADMIN_PLATFORMS
from app.logging import get_logger
from app.services.redis_service import RedisService

logger = get_logger(__name__)

# Redis key prefixes
_REFRESH_TOKEN_PREFIX = "rt:"  # rt:<jti> -> bcrypt hash


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _make_jti() -> str:
    return str(uuid.uuid4())


def create_access_token(platform: str) -> tuple[str, str]:
    """
    Create a signed JWT access token for the given platform.

    Returns (token_string, jti).
    """
    settings = get_settings()
    jti = _make_jti()
    now = _now_utc()
    payload = {
        "sub": platform,
        "iat": now,
        "exp": now + timedelta(hours=settings.jwt_expiry_hours),
        "jti": jti,
        "is_admin": platform in ADMIN_PLATFORMS,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, jti


async def create_refresh_token(platform: str, redis: RedisService) -> tuple[str, str]:
    """
    Create a refresh token (opaque UUID), store its bcrypt hash in Redis.

    Returns (raw_refresh_token, jti).
    TTL = jwt_refresh_expiry_days * 86400 seconds.
    """
    settings = get_settings()
    jti = _make_jti()
    raw_token = str(uuid.uuid4())
    hashed = bcrypt.hashpw(raw_token.encode(), bcrypt.gensalt()).decode()
    ttl = settings.jwt_refresh_expiry_days * 86400
    # Store: rt:<jti> -> "<platform>:<bcrypt_hash>"
    await redis.set(f"{_REFRESH_TOKEN_PREFIX}{jti}", f"{platform}:{hashed}", ttl_seconds=ttl)
    # Embed jti in the raw token so we can look it up: "<jti>.<raw_uuid>"
    composite = f"{jti}.{raw_token}"
    return composite, jti


async def verify_refresh_token(
    composite_token: str,
    redis: RedisService,
) -> Optional[str]:
    """
    Verify a refresh token and return the platform name if valid.

    Returns None if token is invalid or not found in Redis.
    """
    try:
        jti, raw_token = composite_token.split(".", 1)
    except ValueError:
        return None

    stored = await redis.get(f"{_REFRESH_TOKEN_PREFIX}{jti}")
    if stored is None:
        return None

    try:
        platform, hashed = stored.split(":", 1)
    except ValueError:
        return None

    if not bcrypt.checkpw(raw_token.encode(), hashed.encode()):
        return None

    return platform


async def rotate_refresh_token(
    composite_token: str,
    redis: RedisService,
) -> Optional[tuple[str, str]]:
    """
    Validate old refresh token, invalidate it, issue new one.

    Returns (new_composite_token, jti) or None if old token was invalid.
    """
    platform = await verify_refresh_token(composite_token, redis)
    if platform is None:
        return None

    # Revoke old token
    jti = composite_token.split(".", 1)[0]
    await redis.delete(f"{_REFRESH_TOKEN_PREFIX}{jti}")

    # Issue new
    new_token, new_jti = await create_refresh_token(platform, redis)
    return new_token, new_jti


async def revoke_refresh_token(composite_token: str, redis: RedisService) -> bool:
    """
    Delete refresh token from Redis (logout).

    Returns True if token existed and was deleted.
    """
    try:
        jti = composite_token.split(".", 1)[0]
    except (ValueError, IndexError):
        return False

    key = f"{_REFRESH_TOKEN_PREFIX}{jti}"
    existed = await redis.get(key)
    if existed is None:
        return False
    await redis.delete(key)
    return True


def decode_access_token(token: str) -> dict:
    """
    Decode and validate a JWT access token.

    Raises jwt.PyJWTError on any failure (expired, invalid signature, etc.).
    Returns the payload dict.
    """
    settings = get_settings()
    payload = jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
        options={"require": ["sub", "exp", "iat", "jti"]},
    )
    return payload


def authenticate_platform(client_id: str, secret: str) -> bool:
    """
    Verify client_id + secret against PLATFORM_SECRETS.
    Returns True if credentials are valid.
    """
    expected = PLATFORM_SECRETS.get(client_id)
    if expected is None:
        return False
    return expected == secret
