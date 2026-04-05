"""
Rate limiting middleware using Redis INCR with sliding window (per-minute).

- 1000 requests/minute per platform token
- Returns 429 with Retry-After header when exceeded
- Gracefully bypasses if Redis is down
"""

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.config import get_settings
from app.logging import get_logger
from app.services.redis_service import get_redis_service

logger = get_logger(__name__)

_WINDOW_SECONDS = 60


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Per-platform rate limiting.
    Uses Redis key: rl:<platform>:<unix_minute_bucket>
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        platform = getattr(request.state, "platform", None)
        if platform is None:
            # No authenticated platform — let auth middleware handle it
            return await call_next(request)

        settings = get_settings()
        limit = settings.rate_limit_requests_per_minute

        redis = get_redis_service()
        allowed, retry_after = await self._check_rate_limit(platform, limit, redis)

        if not allowed:
            logger.warning(
                "rate_limit_exceeded",
                platform=platform,
                limit=limit,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded",
                    "retry_after_seconds": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        return await call_next(request)

    @staticmethod
    async def _check_rate_limit(
        platform: str,
        limit: int,
        redis,
    ) -> tuple[bool, int]:
        """
        Returns (is_allowed, retry_after_seconds).
        If Redis is down, always allows the request.
        """
        import time

        minute_bucket = int(time.time()) // _WINDOW_SECONDS
        key = f"rl:{platform}:{minute_bucket}"

        count = await redis.incr(key)
        if count is None:
            # Redis unavailable — bypass rate limiting
            return True, 0

        if count == 1:
            # First request in this window — set TTL
            await redis.expire(key, _WINDOW_SECONDS + 5)

        if count > limit:
            # Calculate seconds until next window
            seconds_elapsed = int(time.time()) % _WINDOW_SECONDS
            retry_after = _WINDOW_SECONDS - seconds_elapsed
            return False, retry_after

        return True, 0
