"""Tests for cost guardrails: daily cap, per-source cap, audio cap."""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.qualitative.cost_guard import (
    DAILY_AUDIO_CAP,
    DAILY_TOTAL_CAP,
    PER_SOURCE_CAP,
    CostLimitExceededError,
    check_all_caps,
    check_audio_cap,
    check_daily_total_cap,
    check_per_source_cap,
)


def _make_session(count: int) -> MagicMock:
    """Create a mock session that returns the given count from scalar_one()."""
    session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = count
    session.execute = AsyncMock(return_value=mock_result)
    return session


class TestDailyTotalCap:
    """Tests for the 200 documents/day total cap."""

    @pytest.mark.asyncio
    async def test_daily_cap_200_enforced(self) -> None:
        """When count >= 200, check_daily_total_cap should raise CostLimitExceededError."""
        session = _make_session(200)
        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_daily_total_cap(session, today=date(2026, 4, 5))

        assert exc_info.value.cap_type == "daily_total"
        assert exc_info.value.current == 200
        assert exc_info.value.limit == DAILY_TOTAL_CAP

    @pytest.mark.asyncio
    async def test_daily_cap_below_limit_allowed(self) -> None:
        """When count < 200, check_daily_total_cap should return (True, count)."""
        session = _make_session(150)
        allowed, count = await check_daily_total_cap(session, today=date(2026, 4, 5))
        assert allowed is True
        assert count == 150

    @pytest.mark.asyncio
    async def test_daily_cap_exactly_at_limit_enforced(self) -> None:
        """Count == 200 should trigger the cap."""
        session = _make_session(DAILY_TOTAL_CAP)
        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_daily_total_cap(session, today=date(2026, 4, 5))
        assert exc_info.value.cap_type == "daily_total"

    @pytest.mark.asyncio
    async def test_daily_cap_one_below_limit_allowed(self) -> None:
        """Count == 199 should be allowed (under cap)."""
        session = _make_session(DAILY_TOTAL_CAP - 1)
        allowed, count = await check_daily_total_cap(session, today=date(2026, 4, 5))
        assert allowed is True


class TestPerSourceCap:
    """Tests for the 50 documents/day per source cap."""

    @pytest.mark.asyncio
    async def test_per_source_cap_50_enforced(self) -> None:
        """When source count >= 50, check_per_source_cap should raise."""
        session = _make_session(50)
        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_per_source_cap(session, source_id=7, today=date(2026, 4, 5))

        assert exc_info.value.cap_type == "per_source"
        assert exc_info.value.current == 50
        assert exc_info.value.limit == PER_SOURCE_CAP

    @pytest.mark.asyncio
    async def test_per_source_cap_below_limit_allowed(self) -> None:
        """When source count < 50, should return (True, count)."""
        session = _make_session(25)
        allowed, count = await check_per_source_cap(session, source_id=7, today=date(2026, 4, 5))
        assert allowed is True
        assert count == 25

    @pytest.mark.asyncio
    async def test_per_source_cap_zero_allowed(self) -> None:
        """No documents yet for source should be allowed."""
        session = _make_session(0)
        allowed, count = await check_per_source_cap(session, source_id=1, today=date(2026, 4, 5))
        assert allowed is True
        assert count == 0


class TestAudioCap:
    """Tests for the 10 audio files/day cap."""

    @pytest.mark.asyncio
    async def test_audio_cap_10_enforced(self) -> None:
        """When audio count >= 10, check_audio_cap should raise."""
        session = _make_session(10)
        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_audio_cap(session, today=date(2026, 4, 5))

        assert exc_info.value.cap_type == "audio_daily"
        assert exc_info.value.current == 10
        assert exc_info.value.limit == DAILY_AUDIO_CAP

    @pytest.mark.asyncio
    async def test_audio_cap_below_limit_allowed(self) -> None:
        """When audio count < 10, should return (True, count)."""
        session = _make_session(5)
        allowed, count = await check_audio_cap(session, today=date(2026, 4, 5))
        assert allowed is True
        assert count == 5

    @pytest.mark.asyncio
    async def test_audio_cap_nine_allowed(self) -> None:
        """9 audio files should still be allowed."""
        session = _make_session(DAILY_AUDIO_CAP - 1)
        allowed, count = await check_audio_cap(session, today=date(2026, 4, 5))
        assert allowed is True


class TestCheckAllCaps:
    """Tests for the combined check_all_caps function."""

    @pytest.mark.asyncio
    async def test_check_all_caps_passes_when_all_under_limit(self) -> None:
        """check_all_caps should not raise when all counts are below limits."""
        session = _make_session(5)
        # Should not raise
        await check_all_caps(session, source_id=1, is_audio=False, today=date(2026, 4, 5))

    @pytest.mark.asyncio
    async def test_check_all_caps_raises_on_daily_total_hit(self) -> None:
        """check_all_caps should propagate CostLimitExceededError from daily total cap."""
        session = _make_session(200)
        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_all_caps(session, source_id=1, is_audio=False, today=date(2026, 4, 5))
        assert exc_info.value.cap_type == "daily_total"

    @pytest.mark.asyncio
    async def test_check_all_caps_audio_true_checks_audio_cap(self) -> None:
        """With is_audio=True and audio count at limit, should raise for audio cap."""
        call_count = 0

        async def counting_execute(stmt, params=None):
            nonlocal call_count
            call_count += 1
            mock_result = MagicMock()
            # First call (total): under limit; second call (per_source): under limit;
            # third call (audio): at limit
            if call_count == 1:
                mock_result.scalar_one.return_value = 10  # total under 200
            elif call_count == 2:
                mock_result.scalar_one.return_value = 5   # source under 50
            else:
                mock_result.scalar_one.return_value = 10  # audio at 10 cap
            return mock_result

        session = MagicMock()
        session.execute = counting_execute

        with pytest.raises(CostLimitExceededError) as exc_info:
            await check_all_caps(session, source_id=1, is_audio=True, today=date(2026, 4, 5))
        assert exc_info.value.cap_type == "audio_daily"
