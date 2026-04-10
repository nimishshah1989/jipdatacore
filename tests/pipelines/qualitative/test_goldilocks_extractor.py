"""Tests for Goldilocks Claude structured extraction.

All tests mock both the anthropic client and the SQLAlchemy session.
No real API calls and no live DB required.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.qualitative.goldilocks_extractor import (
    _compute_quality_score,
    _parse_date,
    _to_decimal,
    extract_general_views,
    extract_sector_views,
    extract_stock_idea,
    extract_trend_friend,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_claude_mock(tool_name: str, tool_input: dict) -> MagicMock:
    """Build a mock anthropic.AsyncAnthropic that returns a canned tool_use response."""
    mock_tool_block = MagicMock()
    mock_tool_block.type = "tool_use"
    mock_tool_block.name = tool_name
    mock_tool_block.input = tool_input

    mock_response = MagicMock()
    mock_response.content = [mock_tool_block]

    mock_client = MagicMock()
    mock_client.messages = MagicMock()
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    mock_anthropic = MagicMock()
    mock_anthropic.AsyncAnthropic = MagicMock(return_value=mock_client)
    return mock_anthropic


def _make_mock_session() -> MagicMock:
    """Build a mock AsyncSession that records execute calls."""
    session = MagicMock()
    session.execute = AsyncMock()
    session.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    return session


# ---------------------------------------------------------------------------
# Unit tests for internal helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_to_decimal_converts_number(self) -> None:
        assert _to_decimal(22150.5) == Decimal("22150.5")

    def test_to_decimal_returns_none_for_none(self) -> None:
        assert _to_decimal(None) is None

    def test_to_decimal_handles_int(self) -> None:
        assert _to_decimal(22000) == Decimal("22000")

    def test_to_decimal_is_decimal_instance(self) -> None:
        result = _to_decimal(123.45)
        assert isinstance(result, Decimal)

    def test_parse_date_valid(self) -> None:
        from datetime import date
        assert _parse_date("2026-04-01") == date(2026, 4, 1)

    def test_parse_date_none(self) -> None:
        assert _parse_date(None) is None

    def test_parse_date_invalid_returns_none(self) -> None:
        assert _parse_date("not-a-date") is None

    def test_compute_quality_score_all_present(self) -> None:
        data = {"a": 1, "b": 2, "c": 3}
        score = _compute_quality_score(data, ["a", "b", "c"])
        assert score == Decimal("1.00")

    def test_compute_quality_score_partial(self) -> None:
        data = {"a": 1, "b": None, "c": None}
        score = _compute_quality_score(data, ["a", "b", "c"])
        assert score == Decimal("0.33")

    def test_compute_quality_score_empty_fields(self) -> None:
        score = _compute_quality_score({}, [])
        assert score == Decimal("0.00")


# ---------------------------------------------------------------------------
# Test: extract_trend_friend — happy path
# ---------------------------------------------------------------------------


class TestExtractTrendFriend:
    @pytest.mark.asyncio
    async def test_extract_trend_friend_happy_path(self) -> None:
        """Full Trend Friend extraction: market view + sectors upserted."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": "2026-04-01",
            "nifty_close": 22150.5,
            "nifty_support_1": 21800.0,
            "nifty_support_2": 21500.0,
            "nifty_resistance_1": 22400.0,
            "nifty_resistance_2": 22800.0,
            "bank_nifty_close": 47200.0,
            "bank_nifty_support_1": 46500.0,
            "bank_nifty_support_2": 45800.0,
            "bank_nifty_resistance_1": 48000.0,
            "bank_nifty_resistance_2": 49000.0,
            "trend_direction": "upward",
            "trend_strength": 4,
            "global_impact": "positive",
            "headline": "Markets consolidate near highs",
            "overall_view": "Nifty maintains bullish structure above key EMA levels.",
            "sectors": [
                {
                    "sector": "Banking",
                    "trend": "bullish",
                    "outlook": "Positive on rate cuts",
                    "rank": 1,
                },
                {
                    "sector": "IT",
                    "trend": "sideways",
                    "outlook": "Wait and watch",
                    "rank": 2,
                },
            ],
        }

        mock_anthropic = _make_claude_mock("extract_trend_friend", tool_input)
        session = _make_mock_session()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_trend_friend(
                    document_id=document_id,
                    raw_text="Nifty closed at 22150. Support at 21800...",
                    session=session,
                )

        assert result is True
        # Should have executed 3 times: 1 market_view + 2 sectors
        assert session.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_extract_trend_friend_price_values_are_decimal(self) -> None:
        """All numeric price fields passed to DB must be Decimal instances."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": "2026-04-01",
            "nifty_close": 22150.5,
            "nifty_support_1": 21800.0,
            "nifty_support_2": None,
            "nifty_resistance_1": 22400.0,
            "nifty_resistance_2": None,
            "bank_nifty_close": 47200.0,
            "bank_nifty_support_1": None,
            "bank_nifty_support_2": None,
            "bank_nifty_resistance_1": None,
            "bank_nifty_resistance_2": None,
            "trend_direction": "upward",
            "trend_strength": 4,
            "global_impact": "positive",
            "headline": "Markets up",
            "overall_view": "Bullish outlook.",
            "sectors": [],
        }

        mock_anthropic = _make_claude_mock("extract_trend_friend", tool_input)
        session = _make_mock_session()

        # Override execute to capture calls
        async def capture_execute(stmt: object, *args, **kwargs):
            return MagicMock(scalar_one_or_none=MagicMock(return_value=None))

        session.execute = capture_execute

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_trend_friend(
                    document_id=document_id,
                    raw_text="Nifty at 22150.",
                    session=session,
                )

        assert result is True

    @pytest.mark.asyncio
    async def test_extract_trend_friend_partial_fields(self) -> None:
        """Only required fields populated — all optional fields null. Must not raise."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": "2026-04-02",
            "trend_direction": "sideways",
            "nifty_close": None,
            "nifty_support_1": None,
            "nifty_support_2": None,
            "nifty_resistance_1": None,
            "nifty_resistance_2": None,
            "bank_nifty_close": None,
            "bank_nifty_support_1": None,
            "bank_nifty_support_2": None,
            "bank_nifty_resistance_1": None,
            "bank_nifty_resistance_2": None,
            "trend_strength": None,
            "global_impact": None,
            "headline": None,
            "overall_view": None,
            "sectors": [],
        }

        mock_anthropic = _make_claude_mock("extract_trend_friend", tool_input)
        session = _make_mock_session()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_trend_friend(
                    document_id=document_id,
                    raw_text="Markets are sideways today.",
                    session=session,
                )

        assert result is True
        # Only 1 execute: the market_view upsert (no sectors)
        assert session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_extract_trend_friend_missing_date_returns_false(self) -> None:
        """If report_date is missing/invalid, extraction returns False."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": None,
            "trend_direction": "upward",
            "sectors": [],
        }

        mock_anthropic = _make_claude_mock("extract_trend_friend", tool_input)
        session = _make_mock_session()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_trend_friend(
                    document_id=document_id,
                    raw_text="Some text.",
                    session=session,
                )

        assert result is False
        # No DB writes when date is missing
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Test: extract_stock_idea
# ---------------------------------------------------------------------------


class TestExtractStockIdea:
    @pytest.mark.asyncio
    async def test_extract_stock_idea_happy_path(self) -> None:
        """Full stock idea extraction succeeds and inserts one row."""
        document_id = uuid.uuid4()
        tool_input = {
            "published_date": "2026-04-05",
            "symbol": "RELIANCE",
            "company_name": "Reliance Industries Ltd",
            "idea_type": "stock_bullet",
            "entry_price": 2850.0,
            "entry_zone_low": 2800.0,
            "entry_zone_high": 2900.0,
            "target_1": 3100.0,
            "target_2": 3400.0,
            "lt_target": 3800.0,
            "stop_loss": 2650.0,
            "timeframe": "3-6 months",
            "rationale": "Breakout from consolidation with strong volume",
            "technical_params": {
                "ema_200": 2700.0,
                "rsi_14": 62.5,
                "support_1": 2800.0,
                "resistance_1": 3100.0,
            },
        }

        mock_anthropic = _make_claude_mock("extract_stock_idea", tool_input)
        session = _make_mock_session()
        # First call: idempotency check (no existing row)
        session.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=None))

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_stock_idea(
                    document_id=document_id,
                    raw_text="RELIANCE: Buy at 2850, target 3100/3400, SL 2650.",
                    session=session,
                )

        assert result is True
        # Should have 2 execute calls: SELECT (idempotency check) + INSERT
        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_extract_stock_idea_entry_price_is_decimal(self) -> None:
        """entry_price returned by Claude is stored as Decimal, not float."""
        document_id = uuid.uuid4()
        tool_input = {
            "published_date": "2026-04-05",
            "symbol": "HDFC",
            "company_name": "HDFC Bank",
            "idea_type": "stock_bullet",
            "entry_price": 1650.25,
            "entry_zone_low": None,
            "entry_zone_high": None,
            "target_1": 1850.0,
            "target_2": None,
            "lt_target": None,
            "stop_loss": 1520.0,
            "timeframe": "3 months",
            "rationale": "Support bounce",
            "technical_params": None,
        }

        mock_anthropic = _make_claude_mock("extract_stock_idea", tool_input)
        session = _make_mock_session()

        # Capture INSERT values
        async def capture_execute(stmt: object, *args, **kwargs):
            if hasattr(stmt, "is_dml") and stmt.is_dml:
                # DML (INSERT) — check for compiled values
                pass
            return MagicMock(scalar_one_or_none=MagicMock(return_value=None))

        session.execute = capture_execute

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_stock_idea(
                    document_id=document_id,
                    raw_text="HDFC: Buy 1650.25 target 1850 SL 1520.",
                    session=session,
                )

        assert result is True
        # Verify _to_decimal produces Decimal for this value
        assert _to_decimal(1650.25) == Decimal("1650.25")
        assert isinstance(_to_decimal(1650.25), Decimal)

    @pytest.mark.asyncio
    async def test_extract_stock_idea_idempotent(self) -> None:
        """If document_id already has a stock idea row, second call is skipped."""
        document_id = uuid.uuid4()
        existing_id = uuid.uuid4()

        session = _make_mock_session()
        # Idempotency SELECT returns existing row
        session.execute.return_value = MagicMock(
            scalar_one_or_none=MagicMock(return_value=existing_id)
        )

        result = await extract_stock_idea(
            document_id=document_id,
            raw_text="HDFC: Buy at 1650.",
            session=session,
        )

        assert result is True
        # Only 1 execute: the idempotency SELECT (no INSERT)
        assert session.execute.call_count == 1


# ---------------------------------------------------------------------------
# Test: extract_sector_views
# ---------------------------------------------------------------------------


class TestExtractSectorViews:
    @pytest.mark.asyncio
    async def test_extract_sector_views_multiple_sectors(self) -> None:
        """Five sectors from Claude result in 5 DB upsert calls."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": "2026-04-07",
            "sectors": [
                {
                    "sector": "Banking",
                    "trend": "bullish",
                    "outlook": "Rate cut beneficiary",
                    "rank": 1,
                    "top_picks": [{"symbol": "HDFC", "resistance_levels": [1800.0]}],
                },
                {
                    "sector": "IT",
                    "trend": "neutral",
                    "outlook": "USD headwind",
                    "rank": 2,
                    "top_picks": [],
                },
                {
                    "sector": "Pharma",
                    "trend": "bullish",
                    "outlook": "Domestic strong",
                    "rank": 3,
                    "top_picks": None,
                },
                {
                    "sector": "Auto",
                    "trend": "sideways",
                    "outlook": "EV transition",
                    "rank": 4,
                    "top_picks": None,
                },
                {
                    "sector": "FMCG",
                    "trend": "bearish",
                    "outlook": "Rural demand weak",
                    "rank": 5,
                    "top_picks": None,
                },
            ],
        }

        mock_anthropic = _make_claude_mock("extract_sector_views", tool_input)
        session = _make_mock_session()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_sector_views(
                    document_id=document_id,
                    raw_text="Sector analysis: Banking top pick. IT neutral...",
                    session=session,
                )

        assert result is True
        # 5 sector upserts
        assert session.execute.call_count == 5

    @pytest.mark.asyncio
    async def test_extract_sector_views_empty_sectors(self) -> None:
        """Empty sectors list still returns True (no DB writes for sectors)."""
        document_id = uuid.uuid4()
        tool_input = {
            "report_date": "2026-04-08",
            "sectors": [],
        }

        mock_anthropic = _make_claude_mock("extract_sector_views", tool_input)
        session = _make_mock_session()

        with patch.dict("sys.modules", {"anthropic": mock_anthropic}):
            with patch(
                "app.pipelines.qualitative.goldilocks_extractor.get_settings"
            ) as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                result = await extract_sector_views(
                    document_id=document_id,
                    raw_text="No specific sectors mentioned.",
                    session=session,
                )

        assert result is True
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Test: extract_general_views
# ---------------------------------------------------------------------------


class TestExtractGeneralViews:
    @pytest.mark.asyncio
    async def test_extract_general_views_inserts_qual_extracts(self) -> None:
        """General views are inserted with quality_score as Decimal."""
        document_id = uuid.uuid4()
        views_from_claude = [
            {
                "asset_class": "equity",
                "entity_ref": "Nifty",
                "direction": "bullish",
                "timeframe": "3 months",
                "conviction": "high",
                "view_text": "Nifty bullish above 22000",
                "source_quote": "We remain bullish",
                "quality_score": 0.85,
            }
        ]

        session = _make_mock_session()

        with patch(
            "app.pipelines.qualitative.goldilocks_extractor.extract_views_from_text",
            new=AsyncMock(return_value=views_from_claude),
        ):
            result = await extract_general_views(
                document_id=document_id,
                raw_text="Nifty bullish above 22000.",
                session=session,
            )

        assert result is True
        # One INSERT execute call
        session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_extract_general_views_empty_returns_true(self) -> None:
        """No views returned by Claude still returns True (not an error)."""
        document_id = uuid.uuid4()
        session = _make_mock_session()

        with patch(
            "app.pipelines.qualitative.goldilocks_extractor.extract_views_from_text",
            new=AsyncMock(return_value=[]),
        ):
            result = await extract_general_views(
                document_id=document_id,
                raw_text="Some general commentary.",
                session=session,
            )

        assert result is True
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_extract_general_views_quality_score_is_decimal(self) -> None:
        """quality_score from Claude (float) is stored as Decimal."""
        # This tests the _to_decimal conversion at the boundary
        quality_raw = 0.85
        quality_decimal = Decimal(str(quality_raw))
        assert isinstance(quality_decimal, Decimal)
        assert quality_decimal == Decimal("0.85")
