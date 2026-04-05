"""Retry policies for JIP Data Engine pipeline failures."""

from __future__ import annotations


import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, TypeVar

from app.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class RetryCategory(str, Enum):
    """Classification of failure types for retry policy selection."""

    # Transient failures: network issues, rate limits, temporary service unavailability
    # Policy: retry 3x with exponential backoff (1m → 5m → 15m)
    TRANSIENT = "transient"

    # Persistent failures: bad data, parse errors, schema mismatches, auth failures
    # Policy: fail immediately, no retry (investigation required)
    PERSISTENT = "persistent"


@dataclass
class RetryPolicy:
    """Retry policy configuration for a given failure category.

    Transient: 3 attempts, delays [60, 300, 900] seconds
    Persistent: 1 attempt (fail immediately)
    """

    category: RetryCategory
    max_attempts: int = 3
    delay_seconds: list[int] = field(default_factory=lambda: [60, 300, 900])

    @classmethod
    def transient(cls) -> "RetryPolicy":
        """Retry up to 3x with backoff: 1m, 5m, 15m."""
        return cls(
            category=RetryCategory.TRANSIENT,
            max_attempts=3,
            delay_seconds=[60, 300, 900],
        )

    @classmethod
    def persistent(cls) -> "RetryPolicy":
        """Fail immediately — no retry for persistent errors."""
        return cls(
            category=RetryCategory.PERSISTENT,
            max_attempts=1,
            delay_seconds=[],
        )

    def get_delay(self, attempt: int) -> int:
        """Return the delay in seconds before the next attempt (0-indexed attempt number)."""
        if not self.delay_seconds:
            return 0
        idx = min(attempt, len(self.delay_seconds) - 1)
        return self.delay_seconds[idx]


# HTTP status codes that indicate a transient (retriable) error
TRANSIENT_HTTP_STATUS_CODES: frozenset[int] = frozenset({
    429,  # Too Many Requests
    500,  # Internal Server Error (might be transient)
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
})

# Exception types considered transient
TRANSIENT_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def classify_exception(exc: Exception) -> RetryCategory:
    """Classify an exception as transient or persistent.

    Rules:
    - Network/connection errors → TRANSIENT
    - HTTP 429, 500, 502, 503, 504 → TRANSIENT
    - Parse errors, ValueError, KeyError, data issues → PERSISTENT
    - Auth errors (403, 401) → PERSISTENT
    """
    exc_type_name = type(exc).__name__.lower()

    # Check exception type
    if isinstance(exc, TRANSIENT_EXCEPTION_TYPES):
        return RetryCategory.TRANSIENT

    # Check for HTTP status code embedded in exception message or attributes
    status_code: int | None = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status_code is not None:
        if status_code in TRANSIENT_HTTP_STATUS_CODES:
            return RetryCategory.TRANSIENT
        # 4xx (except 429) → persistent
        if 400 <= status_code < 500:
            return RetryCategory.PERSISTENT

    # Check exception class name patterns
    transient_keywords = ["timeout", "connection", "network", "temporary", "unavailable"]
    persistent_keywords = ["parse", "decode", "validation", "schema", "auth", "permission"]

    for keyword in transient_keywords:
        if keyword in exc_type_name or keyword in str(exc).lower():
            return RetryCategory.TRANSIENT

    for keyword in persistent_keywords:
        if keyword in exc_type_name:
            return RetryCategory.PERSISTENT

    # Check for common persistent exception types
    if isinstance(exc, (ValueError, KeyError, TypeError, AttributeError, IndexError)):
        return RetryCategory.PERSISTENT

    # Default: treat unknown as persistent (fail fast, investigate)
    return RetryCategory.PERSISTENT


async def execute_with_retry(
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    policy: RetryPolicy,
    pipeline_name: str = "unknown",
) -> T:
    """Execute an async coroutine with the given retry policy.

    Args:
        coro_factory: A callable that returns a fresh coroutine each time.
                      (Must be a factory, not a pre-created coroutine, for retries.)
        policy: RetryPolicy defining max_attempts and delays.
        pipeline_name: Used for logging context.

    Returns:
        Result of the coroutine on success.

    Raises:
        The last exception if all attempts are exhausted.
    """
    last_exc: Exception | None = None

    for attempt in range(policy.max_attempts):
        try:
            result = await coro_factory()
            if attempt > 0:
                logger.info(
                    "retry_succeeded",
                    pipeline=pipeline_name,
                    attempt=attempt + 1,
                    max_attempts=policy.max_attempts,
                )
            return result

        except Exception as exc:
            last_exc = exc
            is_last_attempt = attempt == policy.max_attempts - 1

            if is_last_attempt or policy.category == RetryCategory.PERSISTENT:
                logger.error(
                    "retry_exhausted",
                    pipeline=pipeline_name,
                    attempt=attempt + 1,
                    max_attempts=policy.max_attempts,
                    category=policy.category.value,
                    error=str(exc),
                )
                raise exc

            delay = policy.get_delay(attempt)
            logger.warning(
                "retry_attempt_failed",
                pipeline=pipeline_name,
                attempt=attempt + 1,
                max_attempts=policy.max_attempts,
                delay_seconds=delay,
                error=str(exc),
            )
            await asyncio.sleep(delay)

    # Should never reach here, but satisfy type checker
    assert last_exc is not None
    raise last_exc


async def execute_with_auto_retry(
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    pipeline_name: str = "unknown",
) -> T:
    """Execute with automatic retry policy classification.

    Runs the coroutine once; on failure, classifies the exception and
    applies the appropriate retry policy for subsequent attempts.

    This is a convenience wrapper — prefer using execute_with_retry with
    an explicit policy for clarity.
    """
    try:
        return await coro_factory()
    except Exception as exc:
        category = classify_exception(exc)
        if category == RetryCategory.PERSISTENT:
            logger.error(
                "persistent_failure_no_retry",
                pipeline=pipeline_name,
                error=str(exc),
            )
            raise

        policy = RetryPolicy.transient()
        # We've already used attempt 0, so adjust remaining attempts
        remaining_policy = RetryPolicy(
            category=RetryCategory.TRANSIENT,
            max_attempts=policy.max_attempts - 1,
            delay_seconds=policy.delay_seconds[1:],
        )
        if remaining_policy.max_attempts == 0:
            raise

        logger.warning(
            "transient_failure_retrying",
            pipeline=pipeline_name,
            error=str(exc),
            remaining_attempts=remaining_policy.max_attempts,
        )
        return await execute_with_retry(coro_factory, remaining_policy, pipeline_name)
