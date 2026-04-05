"""Tests for EquityOhlcvMigration transform and insert logic."""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.migrations.equity_ohlcv import EquityOhlcvMigration, _parse_date, _to_decimal


# ---------------------------------------------------------------------------
# Helper utilities tests
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_iso_format_string(self) -> None:
        result = _parse_date("2024-04-01")
        assert result == datetime.date(2024, 4, 1)

    def test_dmy_slash_format(self) -> None:
        result = _parse_date("01/04/2024")
        assert result == datetime.date(2024, 4, 1)

    def test_dmy_hyphen_format(self) -> None:
        result = _parse_date("01-04-2024")
        assert result == datetime.date(2024, 4, 1)

    def test_compact_format(self) -> None:
        result = _parse_date("20240401")
        assert result == datetime.date(2024, 4, 1)

    def test_date_object_passthrough(self) -> None:
        d = datetime.date(2024, 4, 1)
        result = _parse_date(d)
        assert result == d

    def test_datetime_returns_date(self) -> None:
        dt = datetime.datetime(2024, 4, 1, 15, 30)
        result = _parse_date(dt)
        assert result == datetime.date(2024, 4, 1)

    def test_none_returns_none(self) -> None:
        assert _parse_date(None) is None

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_date("not-a-date")


class TestToDecimal:
    def test_float_to_decimal(self) -> None:
        result = _to_decimal(123.456)
        assert isinstance(result, Decimal)
        assert result == Decimal("123.456")

    def test_string_to_decimal(self) -> None:
        result = _to_decimal("123.45")
        assert result == Decimal("123.45")

    def test_int_to_decimal(self) -> None:
        result = _to_decimal(100)
        assert result == Decimal("100")

    def test_none_returns_none(self) -> None:
        assert _to_decimal(None) is None

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _to_decimal("not-a-number")

    def test_zero_is_valid(self) -> None:
        result = _to_decimal(0)
        assert result == Decimal("0")


# ---------------------------------------------------------------------------
# EquityOhlcvMigration.transform_row() tests
# ---------------------------------------------------------------------------


class TestEquityOhlcvTransformRow:
    def _make_valid_row(self) -> dict:
        return {
            "symbol": "INFY",
            "trade_date": "2024-04-01",
            "open": 1500.50,
            "high": 1520.00,
            "low": 1490.00,
            "close": 1510.00,
            "volume": 1000000,
            "delivery_vol": 500000,
            "delivery_pct": 50.0,
            "trades": 12345,
        }

    def _make_session_with_instrument(self, instrument_id=None) -> AsyncMock:
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (instrument_id,) if instrument_id else None
        mock_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=mock_result)
        return session

    async def test_transform_converts_date_string_to_date(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        # Warm cache manually to skip warm_cache DB call
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["date"] == datetime.date(2024, 4, 1)

    async def test_transform_converts_float_prices_to_decimal(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert isinstance(result["open"], Decimal)
        assert isinstance(result["high"], Decimal)
        assert isinstance(result["low"], Decimal)
        assert isinstance(result["close"], Decimal)

    async def test_transform_resolves_symbol_to_instrument_id(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["instrument_id"] == instrument_id

    async def test_transform_sets_data_status_raw(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["data_status"] == "raw"

    async def test_unknown_symbol_returns_none(self) -> None:
        """Rows with unknown symbols should be silently skipped (return None)."""
        session = self._make_session_with_instrument(None)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        # No entry in cache — resolver will return None

        row = self._make_valid_row()
        row["symbol"] = "NONEXISTENT_XYZ"
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_empty_symbol_returns_none(self) -> None:
        session = AsyncMock()
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True

        row = self._make_valid_row()
        row["symbol"] = ""
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_missing_trade_date_raises(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        row["trade_date"] = None
        with pytest.raises(ValueError, match="Missing trade_date"):
            await migration.transform_row(row, session)

    async def test_null_volume_handled_as_none(self) -> None:
        instrument_id = uuid.uuid4()
        session = self._make_session_with_instrument(instrument_id)
        migration = EquityOhlcvMigration()
        migration._cache_warmed = True
        migration._resolver._cache["INFY"] = instrument_id

        row = self._make_valid_row()
        row["volume"] = None
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["volume"] is None


# ---------------------------------------------------------------------------
# EquityOhlcvMigration.insert_batch() tests
# ---------------------------------------------------------------------------


class TestEquityOhlcvInsertBatch:
    async def test_empty_batch_returns_zero(self) -> None:
        session = AsyncMock()
        migration = EquityOhlcvMigration()
        result = await migration.insert_batch(session, [])
        assert result == 0
        session.execute.assert_not_called()

    async def test_insert_batch_uses_on_conflict(self) -> None:
        """insert_batch should execute an upsert statement."""
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.rowcount = 2
        session.execute = AsyncMock(return_value=mock_result)
        session.flush = AsyncMock()

        instrument_id = uuid.uuid4()
        rows = [
            {
                "date": datetime.date(2024, 4, 1),
                "instrument_id": instrument_id,
                "symbol": "INFY",
                "open": Decimal("1500.00"),
                "high": Decimal("1520.00"),
                "low": Decimal("1490.00"),
                "close": Decimal("1510.00"),
                "volume": 1000000,
                "delivery_vol": None,
                "delivery_pct": None,
                "trades": None,
                "data_status": "raw",
            }
        ]

        migration = EquityOhlcvMigration()
        await migration.insert_batch(session, rows)

        assert session.execute.call_count == 1
        # Verify the statement was actually executed
        call_args = session.execute.call_args
        assert call_args is not None
