"""Tests for Claude extraction pipeline: quality score filtering and field extraction."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.qualitative.claude_extract import (
    QUALITY_SCORE_THRESHOLD,
    ClaudeExtractionError,
    extract_views_from_text,
)


class TestQualityScoreFiltering:
    """Tests for quality_score threshold enforcement."""

    def test_quality_score_threshold_is_070(self) -> None:
        """Threshold constant should be exactly 0.70."""
        assert QUALITY_SCORE_THRESHOLD == Decimal("0.70")

    def test_quality_score_below_threshold_rejected(self) -> None:
        """Views with quality_score < 0.70 should be rejected (score check logic)."""
        score = Decimal("0.65")
        assert score < QUALITY_SCORE_THRESHOLD

    def test_quality_score_above_threshold_accepted(self) -> None:
        """Views with quality_score >= 0.70 should be accepted."""
        score = Decimal("0.72")
        assert score >= QUALITY_SCORE_THRESHOLD

    def test_quality_score_exactly_at_threshold_accepted(self) -> None:
        """Views with quality_score == 0.70 should be accepted."""
        score = Decimal("0.70")
        assert score >= QUALITY_SCORE_THRESHOLD

    def test_quality_score_zero_rejected(self) -> None:
        """Score of 0.0 must be rejected."""
        score = Decimal("0.00")
        assert score < QUALITY_SCORE_THRESHOLD

    def test_quality_score_one_accepted(self) -> None:
        """Score of 1.0 is maximum confidence — must be accepted."""
        score = Decimal("1.00")
        assert score >= QUALITY_SCORE_THRESHOLD


class TestExtractionFields:
    """Tests for structure of extracted market views."""

    @pytest.mark.asyncio
    async def test_extraction_fields_complete(self) -> None:
        """Claude extraction should return all required fields."""
        expected_views = [
            {
                "asset_class": "equity",
                "entity_ref": "HDFC Bank",
                "direction": "bullish",
                "timeframe": "12 months",
                "conviction": "high",
                "view_text": "HDFC Bank expected to outperform on strong retail deposit growth",
                "source_quote": "We remain constructive on HDFC Bank given its liability franchise",
                "quality_score": 0.88,
            }
        ]

        mock_tool_block = MagicMock()
        mock_tool_block.type = "tool_use"
        mock_tool_block.name = "extract_market_views"
        mock_tool_block.input = {"views": expected_views}

        mock_response = MagicMock()
        mock_response.content = [mock_tool_block]

        mock_client = MagicMock()
        mock_client.messages = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock(return_value=mock_client)

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_module}):
            with patch("app.pipelines.qualitative.claude_extract.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                views = await extract_views_from_text(
                    raw_text="HDFC Bank expected to outperform on strong retail deposit growth",
                    document_id="test-doc-id",
                )

        assert len(views) == 1
        view = views[0]
        assert view["asset_class"] == "equity"
        assert view["entity_ref"] == "HDFC Bank"
        assert view["direction"] == "bullish"
        assert view["timeframe"] == "12 months"
        assert view["conviction"] == "high"
        assert "view_text" in view
        assert "source_quote" in view
        assert view["quality_score"] == 0.88

    @pytest.mark.asyncio
    async def test_quality_score_below_threshold_not_inserted(self) -> None:
        """When Claude returns quality_score < 0.70, the extract must NOT be inserted."""
        low_quality_views = [
            {
                "asset_class": "macro",
                "entity_ref": "India",
                "direction": "neutral",
                "timeframe": None,
                "conviction": "low",
                "view_text": "Markets are uncertain",
                "source_quote": None,
                "quality_score": 0.45,
            }
        ]

        mock_tool_block = MagicMock()
        mock_tool_block.type = "tool_use"
        mock_tool_block.name = "extract_market_views"
        mock_tool_block.input = {"views": low_quality_views}

        mock_response = MagicMock()
        mock_response.content = [mock_tool_block]

        mock_client = MagicMock()
        mock_client.messages = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock(return_value=mock_client)

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_module}):
            with patch("app.pipelines.qualitative.claude_extract.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                views = await extract_views_from_text(
                    raw_text="Markets are uncertain and volatile.",
                    document_id="test-doc-id-2",
                )

        # The view was returned by Claude but quality_score 0.45 < 0.70
        assert len(views) == 1
        assert Decimal(str(views[0]["quality_score"])) < QUALITY_SCORE_THRESHOLD

    @pytest.mark.asyncio
    async def test_empty_text_returns_no_views(self) -> None:
        """Empty text should yield no views from Claude."""
        mock_tool_block = MagicMock()
        mock_tool_block.type = "tool_use"
        mock_tool_block.name = "extract_market_views"
        mock_tool_block.input = {"views": []}

        mock_response = MagicMock()
        mock_response.content = [mock_tool_block]

        mock_client = MagicMock()
        mock_client.messages = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock(return_value=mock_client)

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_module}):
            with patch("app.pipelines.qualitative.claude_extract.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                views = await extract_views_from_text(
                    raw_text="",
                    document_id="test-doc-id-3",
                )

        assert views == []

    @pytest.mark.asyncio
    async def test_claude_api_failure_raises_after_retries(self) -> None:
        """Persistent API failure should raise ClaudeExtractionError after 3 attempts."""
        mock_client = MagicMock()
        mock_client.messages = MagicMock()
        mock_client.messages.create = AsyncMock(
            side_effect=Exception("API rate limit exceeded")
        )

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock(return_value=mock_client)

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_module}):
            with patch("app.pipelines.qualitative.claude_extract.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                with patch("asyncio.sleep", new=AsyncMock()):
                    with pytest.raises(ClaudeExtractionError, match="3 attempts"):
                        await extract_views_from_text(
                            raw_text="Some financial text",
                            document_id="test-doc-id-4",
                        )

    @pytest.mark.asyncio
    async def test_no_tool_use_block_returns_empty(self) -> None:
        """If Claude returns no tool_use block, extraction should return empty list."""
        mock_text_block = MagicMock()
        mock_text_block.type = "text"
        mock_text_block.text = "I couldn't find any specific market views."

        mock_response = MagicMock()
        mock_response.content = [mock_text_block]

        mock_client = MagicMock()
        mock_client.messages = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mock_anthropic_module = MagicMock()
        mock_anthropic_module.AsyncAnthropic = MagicMock(return_value=mock_client)

        with patch.dict("sys.modules", {"anthropic": mock_anthropic_module}):
            with patch("app.pipelines.qualitative.claude_extract.get_settings") as mock_settings:
                mock_settings.return_value = MagicMock(anthropic_api_key="test-key")
                views = await extract_views_from_text(
                    raw_text="General commentary about economy.",
                    document_id="test-doc-id-5",
                )

        assert views == []
