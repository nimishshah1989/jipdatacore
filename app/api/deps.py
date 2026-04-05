"""
FastAPI dependency injection helpers.

- get_current_user  — validates JWT Bearer token, returns payload dict
- get_admin_user    — same, but also requires is_admin claim
- get_db            — async DB session (re-exported from db/session.py)
- get_redis         — RedisService singleton
- PaginationParams  — reusable pagination query params
"""

from typing import Annotated, Optional

import jwt
from fastapi import Depends, HTTPException, Query, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.db.session import get_db  # noqa: F401  (re-export)
from app.logging import get_logger
from app.middleware.auth import decode_access_token
from app.services.redis_service import RedisService, get_redis_service

logger = get_logger(__name__)

_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[Optional[HTTPAuthorizationCredentials], Depends(_bearer_scheme)],
) -> dict:
    """
    Extract and validate JWT from Authorization: Bearer <token>.
    Raises 401 on missing or invalid token.
    Returns the decoded JWT payload dict.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = decode_access_token(credentials.credentials)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError as exc:
        logger.warning("jwt_validation_failed", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


async def get_admin_user(
    payload: Annotated[dict, Depends(get_current_user)],
) -> dict:
    """
    Same as get_current_user but also requires is_admin claim.
    Raises 403 if the platform is not an admin.
    """
    if not payload.get("is_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return payload


async def get_redis() -> RedisService:
    """Return the module-level RedisService singleton."""
    return get_redis_service()


class PaginationParams:
    """
    Reusable pagination query parameters.

    Usage:
        @router.get(...)
        async def endpoint(pagination: Annotated[PaginationParams, Depends()]):
            ...
    """

    def __init__(
        self,
        page: Optional[int] = Query(default=1, ge=1, description="Page number (1-based)"),
        page_size: Optional[int] = Query(default=100, ge=1, le=10000, description="Records per page"),
    ) -> None:
        self.page = page or 1
        self.page_size = page_size or 100

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size
