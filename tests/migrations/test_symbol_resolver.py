"""Tests for SymbolResolver in equity_ohlcv migration."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock


from app.migrations.equity_ohlcv import SymbolResolver


class TestSymbolResolver:
    def _make_session_with_result(self, instrument_id=None) -> AsyncMock:
        """Build a mock session that returns the given instrument_id."""
        session = AsyncMock()
        mock_result = MagicMock()
        if instrument_id is not None:
            mock_result.fetchone.return_value = (instrument_id,)
        else:
            mock_result.fetchone.return_value = None
        session.execute = AsyncMock(return_value=mock_result)
        return session

    async def test_cache_miss_triggers_db_query(self) -> None:
        """First lookup for a symbol should query the database."""
        instrument_id = uuid.uuid4()
        session = self._make_session_with_result(instrument_id)
        resolver = SymbolResolver()

        result = await resolver.resolve(session, "INFY")

        assert result == instrument_id
        assert session.execute.call_count == 1

    async def test_cache_hit_skips_db_query(self) -> None:
        """Second lookup for the same symbol should use the cache, not re-query DB."""
        instrument_id = uuid.uuid4()
        session = self._make_session_with_result(instrument_id)
        resolver = SymbolResolver()

        # First lookup populates cache
        result1 = await resolver.resolve(session, "INFY")
        # Second lookup should hit cache
        result2 = await resolver.resolve(session, "INFY")

        assert result1 == instrument_id
        assert result2 == instrument_id
        # DB should only have been called once
        assert session.execute.call_count == 1

    async def test_cache_is_case_insensitive(self) -> None:
        """Symbol lookup should be case-insensitive."""
        instrument_id = uuid.uuid4()
        session = self._make_session_with_result(instrument_id)
        resolver = SymbolResolver()

        result_lower = await resolver.resolve(session, "infy")
        result_upper = await resolver.resolve(session, "INFY")

        assert result_lower == instrument_id
        assert result_upper == instrument_id
        # Only one DB call — the second hits cache
        assert session.execute.call_count == 1

    async def test_unknown_symbol_returns_none(self) -> None:
        """A symbol not found in de_instrument should return None."""
        session = self._make_session_with_result(None)
        resolver = SymbolResolver()

        result = await resolver.resolve(session, "UNKNOWN_XYZ")

        assert result is None

    async def test_unknown_symbol_is_cached_to_avoid_re_query(self) -> None:
        """None result for an unknown symbol should be cached."""
        session = self._make_session_with_result(None)
        resolver = SymbolResolver()

        await resolver.resolve(session, "UNKNOWN_XYZ")
        await resolver.resolve(session, "UNKNOWN_XYZ")

        # DB should only be queried once
        assert session.execute.call_count == 1

    async def test_empty_symbol_returns_none_without_db_query(self) -> None:
        """Empty string should return None without querying DB."""
        session = AsyncMock()
        resolver = SymbolResolver()

        result = await resolver.resolve(session, "")

        assert result is None
        session.execute.assert_not_called()

    async def test_warm_cache_loads_all_instruments(self) -> None:
        """warm_cache() should pre-load all active instruments into the cache."""
        id1 = uuid.uuid4()
        id2 = uuid.uuid4()

        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("INFY", id1), ("TCS", id2)]
        session.execute = AsyncMock(return_value=mock_result)

        resolver = SymbolResolver()
        await resolver.warm_cache(session)

        # After warming, lookups should hit cache (no further DB calls)
        session2 = AsyncMock()  # Fresh session that would error if called
        result_infy = await resolver.resolve(session2, "INFY")
        result_tcs = await resolver.resolve(session2, "TCS")

        assert result_infy == id1
        assert result_tcs == id2
        session2.execute.assert_not_called()

    async def test_warm_cache_then_unknown_still_queries_db(self) -> None:
        """Symbols not in warm cache should still hit DB on demand."""
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("INFY", uuid.uuid4())]
        session.execute = AsyncMock(return_value=mock_result)

        resolver = SymbolResolver()
        await resolver.warm_cache(session)

        # Now query for something not in the warm cache
        unknown_result = MagicMock()
        unknown_result.fetchone.return_value = None
        session.execute = AsyncMock(return_value=unknown_result)

        result = await resolver.resolve(session, "UNKNOWN")
        assert result is None
        session.execute.assert_called_once()
