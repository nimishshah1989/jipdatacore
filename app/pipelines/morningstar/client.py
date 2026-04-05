"""Morningstar async HTTP client with retry and rate limiting.

Single endpoint pattern:
  GET {base_url}/{IdType}/{Identifier}?datapoints=Name,CategoryName,...

Credential: MORNINGSTAR_ACCESS_CODE from settings (never hardcoded).
Rate limits: configurable per-second and per-day caps.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Optional

import httpx

from app.config import get_settings
from app.logging import get_logger

logger = get_logger(__name__)

# Default rate limits — conservative to avoid 429s
DEFAULT_MAX_PER_SECOND: int = 5
DEFAULT_MAX_PER_DAY: int = 10_000

# Retry configuration
MAX_RETRIES: int = 3
RETRY_BACKOFF_BASE: float = 1.5  # seconds; exponential backoff


class RateLimitExceeded(Exception):
    """Raised when the per-day request cap is exhausted."""

    def __init__(self, cap: int) -> None:
        super().__init__(f"Morningstar daily request cap ({cap}) exhausted")
        self.cap = cap


class MorningstarClient:
    """Async HTTP client for Morningstar fund data API.

    Usage::

        async with MorningstarClient() as client:
            data = await client.fetch("ISIN", "INF205K01UP5",
                                      datapoints=["Name", "CategoryName"])

    The client enforces:
    - Per-second token-bucket rate limiting (``max_per_second``)
    - Per-day counter guard (``max_per_day``)
    - Exponential backoff retry on 5xx / network errors
    - Graceful stub if ``morningstar_base_url`` is not configured
    """

    def __init__(
        self,
        max_per_second: int = DEFAULT_MAX_PER_SECOND,
        max_per_day: int = DEFAULT_MAX_PER_DAY,
        timeout: float = 30.0,
    ) -> None:
        settings = get_settings()
        self._access_code: str = settings.morningstar_access_code
        self._base_url: str = settings.morningstar_base_url.rstrip("/")
        self._timeout = timeout
        self._max_per_second = max_per_second
        self._max_per_day = max_per_day

        # Per-second sliding window: timestamps of recent requests
        self._second_window: deque[float] = deque()
        self._day_count: int = 0

        # Underlying httpx client — initialised in __aenter__
        self._http: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "MorningstarClient":
        self._http = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch(
        self,
        id_type: str,
        identifier: str,
        datapoints: list[str],
    ) -> dict[str, Any]:
        """Fetch datapoints for a single fund.

        Args:
            id_type: Morningstar ID type, e.g. ``"ISIN"`` or ``"FundId"``.
            identifier: The fund identifier value.
            datapoints: List of Morningstar datapoint names.

        Returns:
            Dict of ``{datapoint_name: value}`` from the API response.
            Returns an empty dict if the API URL is not configured (stub mode).

        Raises:
            RateLimitExceeded: If the daily request cap is exhausted.
            httpx.HTTPStatusError: On persistent non-retryable HTTP errors.
        """
        if not self._base_url:
            logger.warning(
                "morningstar_client_stub_mode",
                reason="morningstar_base_url not configured",
                id_type=id_type,
                identifier=identifier,
            )
            return {}

        if not self._access_code:
            logger.warning(
                "morningstar_client_no_access_code",
                id_type=id_type,
                identifier=identifier,
            )
            return {}

        # Daily cap guard
        if self._day_count >= self._max_per_day:
            raise RateLimitExceeded(self._max_per_day)

        await self._throttle()

        url = f"{self._base_url}/{id_type}/{identifier}"
        params: dict[str, str] = {
            "datapoints": ",".join(datapoints),
            "accesscode": self._access_code,
        }

        return await self._get_with_retry(url, params, id_type=id_type, identifier=identifier)

    @property
    def day_count(self) -> int:
        """Number of HTTP requests made in the current day window."""
        return self._day_count

    def reset_day_count(self) -> None:
        """Reset the daily counter (call at midnight or in tests)."""
        self._day_count = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Enforce per-second rate limit via a sliding window."""
        now = time.monotonic()

        # Evict timestamps older than 1 second
        while self._second_window and now - self._second_window[0] >= 1.0:
            self._second_window.popleft()

        if len(self._second_window) >= self._max_per_second:
            # Wait until the oldest request falls out of the 1s window
            sleep_for = 1.0 - (now - self._second_window[0])
            if sleep_for > 0:
                logger.debug(
                    "morningstar_rate_limit_sleep",
                    sleep_seconds=round(sleep_for, 3),
                )
                await asyncio.sleep(sleep_for)

        self._second_window.append(time.monotonic())

    async def _get_with_retry(
        self,
        url: str,
        params: dict[str, str],
        id_type: str,
        identifier: str,
    ) -> dict[str, Any]:
        """Perform GET request with exponential backoff on retryable errors.

        404 responses are returned as empty dict (fund not found).
        5xx and network errors are retried up to MAX_RETRIES times.
        """
        assert self._http is not None, "Client not initialised — use async with"

        last_exc: Optional[Exception] = None

        for attempt in range(MAX_RETRIES + 1):
            try:
                response = await self._http.get(url, params=params)
                self._day_count += 1

                if response.status_code == 404:
                    logger.info(
                        "morningstar_fund_not_found",
                        id_type=id_type,
                        identifier=identifier,
                    )
                    return {}

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 60))
                    logger.warning(
                        "morningstar_429_rate_limited",
                        retry_after=retry_after,
                        attempt=attempt,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                response.raise_for_status()
                data: dict[str, Any] = response.json()
                logger.debug(
                    "morningstar_fetch_ok",
                    id_type=id_type,
                    identifier=identifier,
                    datapoints_returned=len(data),
                )
                return data

            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status < 500:
                    # 4xx (non-404) — not retryable
                    logger.error(
                        "morningstar_client_error",
                        status=status,
                        id_type=id_type,
                        identifier=identifier,
                    )
                    raise
                last_exc = exc

            except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as exc:
                last_exc = exc

            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "morningstar_retry",
                    attempt=attempt + 1,
                    max_retries=MAX_RETRIES,
                    backoff_seconds=round(backoff, 2),
                    id_type=id_type,
                    identifier=identifier,
                    error=str(last_exc),
                )
                await asyncio.sleep(backoff)

        logger.error(
            "morningstar_fetch_failed_all_retries",
            id_type=id_type,
            identifier=identifier,
            error=str(last_exc),
        )
        if last_exc is not None:
            raise last_exc
        return {}
