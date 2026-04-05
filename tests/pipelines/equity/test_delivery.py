"""Tests for DeliveryPipeline — T+1 delivery data update."""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.pipelines.equity.delivery import (
    DeliveryPipeline,
    build_delivery_url,
    parse_delivery_data,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_delivery_csv(symbols: list[str]) -> bytes:
    """Generate a minimal delivery CSV with given symbols."""
    header = "Record Type,SR NO.,SYMBOL,SERIES,TRADED QTY,DELIVERABLE QTY,% DEL QTY TO TRADED QTY"
    lines = [header]
    for i, sym in enumerate(symbols):
        lines.append(f"20,{i+1},{sym},EQ,100000,60000,60.00")
    return "\n".join(lines).encode("utf-8")


# ---------------------------------------------------------------------------
# Tests: URL building
# ---------------------------------------------------------------------------

class TestBuildDeliveryUrl:
    def test_delivery_url_format(self) -> None:
        """Delivery URL built correctly for a given date."""
        url = build_delivery_url(date(2023, 4, 1))
        assert "MTO_01042023.DAT" in url

    def test_delivery_url_leading_zeros(self) -> None:
        """Single-digit month/day get leading zeros."""
        url = build_delivery_url(date(2023, 1, 5))
        assert "MTO_05012023.DAT" in url


# ---------------------------------------------------------------------------
# Tests: Parsing
# ---------------------------------------------------------------------------

class TestParseDeliveryData:
    def test_parse_delivery_basic(self) -> None:
        """Basic delivery CSV parses correctly."""
        raw = _make_delivery_csv(["RELIANCE", "TCS", "INFY"])
        df = parse_delivery_data(raw)
        assert len(df) >= 1  # At least some rows parsed
        assert "symbol" in df.columns
        assert "delivery_qty" in df.columns
        assert "delivery_pct" in df.columns

    def test_parse_delivery_eq_filter(self) -> None:
        """Non-EQ series rows filtered out."""
        header = "Record Type,SR NO.,SYMBOL,SERIES,TRADED QTY,DELIVERABLE QTY,% DEL QTY TO TRADED QTY"
        lines = [header]
        lines.append("20,1,RELIANCE,EQ,100000,60000,60.00")
        lines.append("20,2,RELIANCE,BE,5000,5000,100.00")  # Should be excluded
        raw = "\n".join(lines).encode("utf-8")
        df = parse_delivery_data(raw)
        # Should only have EQ row
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "RELIANCE"


# ---------------------------------------------------------------------------
# Tests: Pipeline execution
# ---------------------------------------------------------------------------

class TestDeliveryPipeline:
    @pytest.mark.asyncio
    async def test_delivery_update_previous_day(self) -> None:
        """Delivery pipeline updates OHLCV for previous trading day."""
        instrument_id = uuid.uuid4()
        prev_day = date(2023, 3, 31)
        business_date = date(2023, 4, 1)

        raw_delivery = _make_delivery_csv(["RELIANCE"])

        mock_run_log = MagicMock()
        mock_run_log.id = 1

        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()

        # UPDATE rowcount = 1 (success)
        mock_update_result = MagicMock()
        mock_update_result.rowcount = 1
        mock_session.execute = AsyncMock(return_value=mock_update_result)

        with (
            patch(
                "app.pipelines.equity.delivery.get_last_trading_day",
                return_value=prev_day,
            ),
            patch(
                "app.pipelines.equity.delivery.fetch_with_retry",
                return_value=raw_delivery,
            ),
            patch(
                "app.pipelines.equity.delivery.bulk_resolve_symbols",
                return_value={"RELIANCE": instrument_id},
            ),
        ):
            pipeline = DeliveryPipeline()
            result = await pipeline.execute(business_date, mock_session, mock_run_log)

        # RELIANCE row should be processed
        assert result.rows_processed == 1
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_delivery_no_previous_trading_day(self) -> None:
        """Returns 0 rows if no previous trading day found."""
        mock_run_log = MagicMock()
        mock_run_log.id = 1
        mock_session = AsyncMock()

        with patch(
            "app.pipelines.equity.delivery.get_last_trading_day",
            return_value=None,
        ):
            pipeline = DeliveryPipeline()
            result = await pipeline.execute(date(2023, 4, 1), mock_session, mock_run_log)

        assert result.rows_processed == 0
        assert result.rows_failed == 0

    @pytest.mark.asyncio
    async def test_delivery_unknown_symbol_counted_as_failed(self) -> None:
        """Unknown symbols are counted in rows_failed."""
        raw_delivery = _make_delivery_csv(["UNKNOWNSYM"])

        mock_run_log = MagicMock()
        mock_run_log.id = 1
        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()

        with (
            patch(
                "app.pipelines.equity.delivery.get_last_trading_day",
                return_value=date(2023, 3, 31),
            ),
            patch(
                "app.pipelines.equity.delivery.fetch_with_retry",
                return_value=raw_delivery,
            ),
            patch(
                "app.pipelines.equity.delivery.bulk_resolve_symbols",
                return_value={},  # No symbols resolved
            ),
        ):
            pipeline = DeliveryPipeline()
            result = await pipeline.execute(date(2023, 4, 1), mock_session, mock_run_log)

        assert result.rows_processed == 0
        assert result.rows_failed >= 1

    @pytest.mark.asyncio
    async def test_delivery_validate_returns_empty_list(self) -> None:
        """DeliveryPipeline.validate returns empty list."""
        mock_session = AsyncMock()
        mock_run_log = MagicMock()
        pipeline = DeliveryPipeline()
        anomalies = await pipeline.validate(date(2023, 4, 1), mock_session, mock_run_log)
        assert anomalies == []
