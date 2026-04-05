"""HTTP fetch utilities — retry logic and NSE-specific headers."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.logging import get_logger

logger = get_logger(__name__)

# NSE requires specific headers to avoid bot detection
NSE_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

# NSE archives endpoint (no JS challenge needed for archives)
NSE_ARCHIVES_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}


async def fetch_with_retry(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    max_retries: int = 3,
    base_delay: float = 1.0,
    timeout: float = 60.0,
    client: httpx.AsyncClient | None = None,
) -> bytes:
    """Fetch a URL with exponential backoff retry.

    Args:
        url: Target URL to fetch.
        headers: HTTP headers to include. Defaults to NSE_ARCHIVES_HEADERS.
        max_retries: Number of retry attempts (total = 1 + max_retries).
        base_delay: Base delay in seconds for exponential backoff.
        timeout: Request timeout in seconds.
        client: Optional pre-configured httpx.AsyncClient (injected for testing).

    Returns:
        Response body as bytes.

    Raises:
        httpx.HTTPStatusError: If all retries exhausted with non-2xx responses.
        httpx.RequestError: If all retries exhausted with connection errors.
    """
    effective_headers = headers if headers is not None else NSE_ARCHIVES_HEADERS
    last_exc: Exception | None = None

    async def _do_fetch(ac: httpx.AsyncClient) -> bytes:
        nonlocal last_exc
        for attempt in range(max_retries + 1):
            try:
                response = await ac.get(url, headers=effective_headers, timeout=timeout)
                response.raise_for_status()
                logger.info(
                    "fetch_success",
                    url=url,
                    attempt=attempt + 1,
                    status_code=response.status_code,
                    content_length=len(response.content),
                )
                return response.content
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_exc = exc
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(
                        "fetch_retry",
                        url=url,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        delay_seconds=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "fetch_failed_all_retries",
                        url=url,
                        max_retries=max_retries,
                        error=str(exc),
                    )
        raise last_exc  # type: ignore[misc]

    if client is not None:
        return await _do_fetch(client)

    async with httpx.AsyncClient(follow_redirects=True) as ac:
        return await _do_fetch(ac)


async def fetch_nse_json(
    url: str,
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
    timeout: float = 30.0,
    client: httpx.AsyncClient | None = None,
) -> Any:
    """Fetch NSE JSON endpoint with session warm-up.

    NSE's main site requires a session cookie obtained by hitting the homepage.
    This function first fetches the NSE homepage to establish a session, then
    fetches the target URL.

    Args:
        url: NSE API endpoint URL.
        max_retries: Retry attempts.
        base_delay: Exponential backoff base delay.
        timeout: Request timeout in seconds.
        client: Optional pre-configured httpx.AsyncClient.

    Returns:
        Parsed JSON response as dict or list.
    """
    import json

    nse_home = "https://www.nseindia.com/"

    async def _do_nse_fetch(ac: httpx.AsyncClient) -> Any:
        # Warm up session — NSE requires cookie from homepage
        try:
            await ac.get(nse_home, headers=NSE_HEADERS, timeout=30.0)
        except Exception as exc:
            logger.warning("nse_session_warmup_failed", error=str(exc))

        data = await fetch_with_retry(
            url,
            headers={**NSE_HEADERS, "Accept": "application/json"},
            max_retries=max_retries,
            base_delay=base_delay,
            timeout=timeout,
            client=ac,
        )
        return json.loads(data)

    if client is not None:
        return await _do_nse_fetch(client)

    async with httpx.AsyncClient(follow_redirects=True) as ac:
        return await _do_nse_fetch(ac)
