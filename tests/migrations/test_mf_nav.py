"""Tests for MfNavMigration transform and insert logic."""

from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.migrations.mf_nav import MfNavMigration, SchemeCodeResolver


# ---------------------------------------------------------------------------
# SchemeCodeResolver tests
# ---------------------------------------------------------------------------


class TestSchemeCodeResolver:
    def _make_session_with_mstar(self, mstar_id=None) -> AsyncMock:
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (mstar_id,) if mstar_id else None
        session.execute = AsyncMock(return_value=mock_result)
        return session

    async def test_resolve_returns_mstar_id(self) -> None:
        session = self._make_session_with_mstar("F00001ABCD")
        resolver = SchemeCodeResolver()

        result = await resolver.resolve(session, "119551")

        assert result == "F00001ABCD"

    async def test_resolve_unknown_scheme_returns_none(self) -> None:
        session = self._make_session_with_mstar(None)
        resolver = SchemeCodeResolver()

        result = await resolver.resolve(session, "UNKNOWN99999")

        assert result is None

    async def test_cache_hit_skips_db(self) -> None:
        session = self._make_session_with_mstar("F00001ABCD")
        resolver = SchemeCodeResolver()

        await resolver.resolve(session, "119551")
        await resolver.resolve(session, "119551")

        assert session.execute.call_count == 1

    async def test_warm_cache_loads_all_mappings(self) -> None:
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("119551", "F00001ABCD"), ("119552", "F00002EFGH")]
        session.execute = AsyncMock(return_value=mock_result)

        resolver = SchemeCodeResolver()
        await resolver.warm_cache(session)

        # After warming, use a different session to confirm cache is used
        fresh_session = AsyncMock()
        result = await resolver.resolve(fresh_session, "119551")
        assert result == "F00001ABCD"
        fresh_session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# MfNavMigration.transform_row() tests
# ---------------------------------------------------------------------------


class TestMfNavTransformRow:
    def _make_valid_row(self) -> dict:
        return {
            "scheme_code": "119551",
            "nav_date": "2024-04-01",
            "nav": 150.75,
        }

    async def test_transform_resolves_scheme_code_to_mstar_id(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["mstar_id"] == "F00001ABCD"

    async def test_transform_nav_float_to_decimal(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert isinstance(result["nav"], Decimal)
        assert result["nav"] == Decimal("150.75")

    async def test_transform_sets_data_status_raw(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["data_status"] == "raw"

    async def test_unknown_scheme_code_returns_none(self) -> None:
        """Rows with scheme_code not in de_mf_master should be skipped."""
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        session.execute = AsyncMock(return_value=mock_result)
        migration = MfNavMigration()
        migration._cache_warmed = True
        # No entry in cache — resolver will query DB and return None

        row = self._make_valid_row()
        row["scheme_code"] = "UNKNOWN99999"
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_zero_nav_returns_none(self) -> None:
        """Zero or negative NAV should be filtered out."""
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        row["nav"] = 0.0
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_negative_nav_returns_none(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        row["nav"] = -5.0
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_missing_scheme_code_returns_none(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True

        row = self._make_valid_row()
        row["scheme_code"] = None
        result = await migration.transform_row(row, session)

        assert result is None

    async def test_missing_nav_date_raises(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        row["nav_date"] = None
        with pytest.raises(ValueError, match="Missing nav_date"):
            await migration.transform_row(row, session)

    async def test_nav_date_parsed_correctly(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        migration._cache_warmed = True
        migration._resolver._cache["119551"] = "F00001ABCD"

        row = self._make_valid_row()
        row["nav_date"] = "2024-04-01"
        result = await migration.transform_row(row, session)

        assert result is not None
        assert result["nav_date"] == datetime.date(2024, 4, 1)


# ---------------------------------------------------------------------------
# MfNavMigration.insert_batch() tests
# ---------------------------------------------------------------------------


class TestMfNavInsertBatch:
    async def test_empty_batch_returns_zero(self) -> None:
        session = AsyncMock()
        migration = MfNavMigration()
        result = await migration.insert_batch(session, [])
        assert result == 0
        session.execute.assert_not_called()

    async def test_insert_executes_upsert(self) -> None:
        session = AsyncMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        session.execute = AsyncMock(return_value=mock_result)
        session.flush = AsyncMock()

        rows = [
            {
                "nav_date": datetime.date(2024, 4, 1),
                "mstar_id": "F00001ABCD",
                "nav": Decimal("150.75"),
                "data_status": "raw",
            }
        ]

        migration = MfNavMigration()
        result = await migration.insert_batch(session, rows)

        assert session.execute.call_count == 1
        assert result >= 0
