"""
Redis client wrapper with circuit breaker and cache stampede protection.

Circuit breaker: 3 consecutive failures -> bypass for 60 seconds.
Per-worker state (not shared via Redis itself).
"""

import time
from typing import Any, Optional

import redis.asyncio as aioredis

from app.config import get_settings
from app.logging import get_logger

logger = get_logger(__name__)

# Circuit breaker state — per-worker, not distributed
_circuit_failures: int = 0
_circuit_open_until: float = 0.0

CIRCUIT_FAILURE_THRESHOLD = 3
CIRCUIT_BYPASS_SECONDS = 60


def _is_circuit_open() -> bool:
    """Return True if circuit is open (Redis should be bypassed)."""
    global _circuit_open_until
    if _circuit_open_until > 0 and time.monotonic() < _circuit_open_until:
        return True
    # Reset open time once bypass period expires
    if _circuit_open_until > 0 and time.monotonic() >= _circuit_open_until:
        _circuit_open_until = 0.0
    return False


def _record_failure() -> None:
    """Record a Redis failure; open circuit after threshold."""
    global _circuit_failures, _circuit_open_until
    _circuit_failures += 1
    if _circuit_failures >= CIRCUIT_FAILURE_THRESHOLD:
        _circuit_open_until = time.monotonic() + CIRCUIT_BYPASS_SECONDS
        _circuit_failures = 0
        logger.warning(
            "redis_circuit_open",
            bypass_seconds=CIRCUIT_BYPASS_SECONDS,
        )


def _record_success() -> None:
    """Reset failure counter on successful Redis call."""
    global _circuit_failures
    _circuit_failures = 0


class RedisService:
    """
    Async Redis wrapper.
    All public methods are safe — they never raise; return None on failure.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._client: Optional[aioredis.Redis] = None
        self._redis_url: str = settings.redis_url

    async def _get_client(self) -> Optional[aioredis.Redis]:
        if self._client is None:
            try:
                self._client = aioredis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("redis_client_init_failed", error=str(exc))
                _record_failure()
                return None
        return self._client

    async def get(self, key: str) -> Optional[str]:
        """Safe get — returns None if Redis is unavailable."""
        if _is_circuit_open():
            return None
        try:
            client = await self._get_client()
            if client is None:
                return None
            value = await client.get(key)
            _record_success()
            return value
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_get_failed", key=key, error=str(exc))
            _record_failure()
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """Safe set — returns False if Redis is unavailable."""
        if _is_circuit_open():
            return False
        try:
            client = await self._get_client()
            if client is None:
                return False
            if ttl_seconds is not None:
                await client.setex(key, ttl_seconds, value)
            else:
                await client.set(key, value)
            _record_success()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_set_failed", key=key, error=str(exc))
            _record_failure()
            return False

    async def delete(self, key: str) -> bool:
        """Safe delete — returns False if Redis is unavailable."""
        if _is_circuit_open():
            return False
        try:
            client = await self._get_client()
            if client is None:
                return False
            await client.delete(key)
            _record_success()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_delete_failed", key=key, error=str(exc))
            _record_failure()
            return False

    async def incr(self, key: str) -> Optional[int]:
        """Safe incr — returns None if Redis is unavailable."""
        if _is_circuit_open():
            return None
        try:
            client = await self._get_client()
            if client is None:
                return None
            value = await client.incr(key)
            _record_success()
            return value
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_incr_failed", key=key, error=str(exc))
            _record_failure()
            return None

    async def expire(self, key: str, ttl_seconds: int) -> bool:
        """Safe expire — returns False if Redis is unavailable."""
        if _is_circuit_open():
            return False
        try:
            client = await self._get_client()
            if client is None:
                return False
            await client.expire(key, ttl_seconds)
            _record_success()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_expire_failed", key=key, error=str(exc))
            _record_failure()
            return False

    async def setnx(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """
        Set-if-not-exists for cache stampede protection.
        Returns True if key was set (lock acquired), False otherwise.
        """
        if _is_circuit_open():
            return False
        try:
            client = await self._get_client()
            if client is None:
                return False
            result = await client.setnx(key, value)
            if result and ttl_seconds is not None:
                await client.expire(key, ttl_seconds)
            _record_success()
            return bool(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_setnx_failed", key=key, error=str(exc))
            _record_failure()
            return False

    async def ttl(self, key: str) -> Optional[int]:
        """Safe ttl — returns None if Redis is unavailable."""
        if _is_circuit_open():
            return None
        try:
            client = await self._get_client()
            if client is None:
                return None
            value = await client.ttl(key)
            _record_success()
            return value
        except Exception as exc:  # noqa: BLE001
            logger.warning("redis_ttl_failed", key=key, error=str(exc))
            _record_failure()
            return None

    async def ping(self) -> bool:
        """Health check — returns True if Redis is reachable."""
        if _is_circuit_open():
            return False
        try:
            client = await self._get_client()
            if client is None:
                return False
            await client.ping()
            _record_success()
            return True
        except Exception:  # noqa: BLE001
            _record_failure()
            return False


# Module-level singleton — shared across requests in same worker
_redis_service: Optional[RedisService] = None


def get_redis_service() -> RedisService:
    """Return the module-level RedisService singleton."""
    global _redis_service
    if _redis_service is None:
        _redis_service = RedisService()
    return _redis_service
