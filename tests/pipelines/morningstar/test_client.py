"""Tests for MorningstarClient — rate limiting, retry, stub mode."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.pipelines.morningstar.client import (
    MorningstarClient,
    RateLimitExceeded,
    DEFAULT_MAX_PER_SECOND,
    DEFAULT_MAX_PER_DAY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_client(
    access_code: str = "test-access-code",
    base_url: str = "https://api.morningstar.test/v2/service/mf",
    max_per_second: int = 10,
    max_per_day: int = 100,
) -> MorningstarClient:
    """Create a MorningstarClient with test settings injected."""
    client = MorningstarClient(max_per_second=max_per_second, max_per_day=max_per_day)
    client._access_code = access_code
    client._base_url = base_url
    return client


def make_response(status_code: int, json_data: dict) -> httpx.Response:
    """Create a mock httpx.Response."""
    return httpx.Response(
        status_code=status_code,
        json=json_data,
        request=httpx.Request("GET", "https://api.morningstar.test/"),
    )


# ---------------------------------------------------------------------------
# Stub mode (no base_url configured)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_stub_mode_no_base_url_returns_empty() -> None:
    """If morningstar_base_url is empty, fetch returns {} without HTTP call."""
    client = make_client(base_url="")
    async with client:
        result = await client.fetch("ISIN", "INF000001234", datapoints=["Name"])
    assert result == {}


@pytest.mark.asyncio
async def test_fetch_stub_mode_no_access_code_returns_empty() -> None:
    """If morningstar_access_code is empty, fetch returns {} without HTTP call."""
    client = make_client(access_code="")
    async with client:
        result = await client.fetch("ISIN", "INF000001234", datapoints=["Name"])
    assert result == {}


# ---------------------------------------------------------------------------
# Successful fetch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_success_returns_parsed_json() -> None:
    """Successful 200 response returns parsed JSON dict."""
    client = make_client()
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(
        return_value=make_response(200, {"Name": "Test Fund", "CategoryName": "Equity"})
    )
    client._http = mock_http

    result = await client._get_with_retry(
        "https://api.morningstar.test/FundId/F0GBR04M30",
        {"datapoints": "Name,CategoryName", "accesscode": "test"},
        id_type="FundId",
        identifier="F0GBR04M30",
    )

    assert result == {"Name": "Test Fund", "CategoryName": "Equity"}
    assert client.day_count == 1


@pytest.mark.asyncio
async def test_fetch_404_returns_empty_dict() -> None:
    """404 response returns empty dict (fund not found)."""
    client = make_client()
    mock_http = AsyncMock()
    mock_http.get = AsyncMock(return_value=make_response(404, {}))
    client._http = mock_http

    result = await client._get_with_retry(
        "https://api.morningstar.test/FundId/NOTEXIST",
        {"datapoints": "Name"},
        id_type="FundId",
        identifier="NOTEXIST",
    )

    assert result == {}


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_exceeded_raises() -> None:
    """When day_count >= max_per_day, fetch raises RateLimitExceeded."""
    client = make_client(max_per_day=5)
    client._day_count = 5

    async with client:
        with pytest.raises(RateLimitExceeded) as exc_info:
            await client.fetch("FundId", "F0GBR04M30", datapoints=["Name"])

    assert exc_info.value.cap == 5


@pytest.mark.asyncio
async def test_reset_day_count_resets_to_zero() -> None:
    """reset_day_count() zeroes the daily counter."""
    client = make_client(max_per_day=10)
    client._day_count = 9
    client.reset_day_count()
    assert client.day_count == 0


def test_day_count_property_initial_value() -> None:
    """day_count starts at 0."""
    client = make_client()
    assert client.day_count == 0


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_retries_on_500_then_succeeds() -> None:
    """5xx error triggers retry; succeeds on second attempt."""
    client = make_client()

    error_response = httpx.Response(
        status_code=500,
        text="Internal Server Error",
        request=httpx.Request("GET", "https://api.morningstar.test/"),
    )
    success_response = make_response(200, {"Name": "Retry Fund"})

    call_count = 0

    async def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.HTTPStatusError(
                "500", request=error_response.request, response=error_response
            )
        return success_response

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=side_effect)
    client._http = mock_http

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await client._get_with_retry(
            "https://api.morningstar.test/FundId/X",
            {},
            id_type="FundId",
            identifier="X",
        )

    assert result == {"Name": "Retry Fund"}
    assert call_count == 2


@pytest.mark.asyncio
async def test_fetch_raises_after_all_retries_exhausted() -> None:
    """After MAX_RETRIES exhausted on network error, exception is raised."""
    client = make_client()

    async def always_fail(*args, **kwargs):
        raise httpx.ConnectError("connection refused")

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=always_fail)
    client._http = mock_http

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(httpx.ConnectError):
            await client._get_with_retry(
                "https://api.morningstar.test/FundId/Y",
                {},
                id_type="FundId",
                identifier="Y",
            )


@pytest.mark.asyncio
async def test_fetch_4xx_non_404_raises_immediately() -> None:
    """4xx (non-404) error is not retried — raises immediately."""
    client = make_client()

    error_response = httpx.Response(
        status_code=403,
        text="Forbidden",
        request=httpx.Request("GET", "https://api.morningstar.test/"),
    )

    async def forbidden(*args, **kwargs):
        raise httpx.HTTPStatusError(
            "403", request=error_response.request, response=error_response
        )

    mock_http = AsyncMock()
    mock_http.get = AsyncMock(side_effect=forbidden)
    client._http = mock_http

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await client._get_with_retry(
            "https://api.morningstar.test/FundId/Z",
            {},
            id_type="FundId",
            identifier="Z",
        )

    assert exc_info.value.response.status_code == 403
    # Only 1 call — no retry
    assert mock_http.get.call_count == 1


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_context_manager_opens_and_closes_http_client() -> None:
    """async with MorningstarClient() creates and closes the underlying httpx client."""
    client = make_client()
    assert client._http is None

    async with client:
        assert client._http is not None

    assert client._http is None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

def test_default_constants_reasonable() -> None:
    """Default rate limit constants are within expected ranges."""
    assert DEFAULT_MAX_PER_SECOND >= 1
    assert DEFAULT_MAX_PER_DAY >= 1000
