"""Tests for migration base classes."""

from __future__ import annotations

from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch


from app.migrations.base import BaseMigration, MigrationResult


# ---------------------------------------------------------------------------
# Concrete stub migration for testing BaseMigration
# ---------------------------------------------------------------------------


class StubMigration(BaseMigration):
    """Minimal concrete implementation for testing the base class."""

    source_db_name = "test_db"
    source_table = "test_table"
    target_table = "de_test_table"
    batch_size = 2

    def __init__(
        self,
        source_url: str = "postgresql+asyncpg://user:pw@localhost/test",
        rows: Optional[list[dict]] = None,
        fail_transform: bool = False,
        fail_insert: bool = False,
    ) -> None:
        self._source_url = source_url
        self._rows = rows or []
        self._fail_transform = fail_transform
        self._fail_insert = fail_insert
        self._inserted: list[dict] = []

    def get_source_db_url(self) -> str:
        return self._source_url

    def build_source_query(self, offset: int, limit: int) -> str:
        return f"SELECT * FROM test_table ORDER BY id OFFSET {offset} LIMIT {limit}"

    async def transform_row(
        self, row: dict[str, Any], target_session: Any
    ) -> Optional[dict[str, Any]]:
        if self._fail_transform:
            raise ValueError("transform_error")
        return row

    async def insert_batch(self, session: Any, rows: list[dict[str, Any]]) -> int:
        if self._fail_insert:
            raise RuntimeError("insert_error")
        self._inserted.extend(rows)
        return len(rows)


# ---------------------------------------------------------------------------
# MigrationResult tests
# ---------------------------------------------------------------------------


class TestMigrationResult:
    def test_defaults(self) -> None:
        result = MigrationResult(
            source_db="fie_v3",
            source_table="compass_stock_prices",
            target_table="de_equity_ohlcv",
            rows_read=100,
            rows_written=98,
            rows_errored=2,
            status="partial",
            duration_seconds=1.5,
        )
        assert result.errors == []
        assert result.rows_read == 100
        assert result.rows_written == 98
        assert result.rows_errored == 2
        assert result.status == "partial"

    def test_errors_list(self) -> None:
        result = MigrationResult(
            source_db="db",
            source_table="tbl",
            target_table="de_tbl",
            rows_read=1,
            rows_written=0,
            rows_errored=1,
            status="failed",
            duration_seconds=0.1,
            errors=[{"row": "bad"}],
        )
        assert len(result.errors) == 1
        assert result.errors[0] == {"row": "bad"}

    def test_status_values(self) -> None:
        for status in ("success", "failed", "partial", "dry_run"):
            result = MigrationResult(
                source_db="db",
                source_table="t",
                target_table="dt",
                rows_read=0,
                rows_written=0,
                rows_errored=0,
                status=status,
                duration_seconds=0.0,
            )
            assert result.status == status


# ---------------------------------------------------------------------------
# BaseMigration.run() tests
# ---------------------------------------------------------------------------


class TestBaseMigrationRun:
    """Tests for BaseMigration.run() with mocked source DB connections."""

    def _make_migration_log(self) -> MagicMock:
        log = MagicMock()
        log.id = 1
        log.rows_read = None
        log.rows_written = None
        return log

    def _make_target_session(self) -> AsyncMock:
        session = AsyncMock()
        session.add = MagicMock()
        session.flush = AsyncMock()
        return session

    @patch("app.migrations.base.create_async_engine")
    @patch("app.migrations.base.async_sessionmaker")
    async def test_run_creates_migration_log_entry(
        self,
        mock_session_factory: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """run() should add a DeMigrationLog to the target session."""
        target_session = self._make_target_session()

        # Source session returns empty (no rows)
        mock_source_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = []
        mock_source_session.execute = AsyncMock(return_value=mock_result)
        mock_source_session.__aenter__ = AsyncMock(return_value=mock_source_session)
        mock_source_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory.return_value.return_value = mock_source_session

        engine_mock = AsyncMock()
        engine_mock.dispose = AsyncMock()
        mock_create_engine.return_value = engine_mock

        migration = StubMigration(rows=[])
        await migration.run(target_session)

        # A DeMigrationLog object should have been added to the session
        assert target_session.add.called
        added_obj = target_session.add.call_args_list[0][0][0]
        assert added_obj.source_db == "test_db"
        assert added_obj.source_table == "test_table"
        assert added_obj.target_table == "de_test_table"
        # Status is mutated to final value by the time run() returns — confirm it settled
        assert added_obj.status in ("success", "failed", "partial")

    @patch("app.migrations.base.create_async_engine")
    @patch("app.migrations.base.async_sessionmaker")
    async def test_run_empty_source_returns_success(
        self,
        mock_session_factory: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """When source has no rows, migration should still succeed."""
        target_session = self._make_target_session()

        mock_source_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = []
        mock_source_session.execute = AsyncMock(return_value=mock_result)
        mock_source_session.__aenter__ = AsyncMock(return_value=mock_source_session)
        mock_source_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory.return_value.return_value = mock_source_session
        engine_mock = AsyncMock()
        engine_mock.dispose = AsyncMock()
        mock_create_engine.return_value = engine_mock

        migration = StubMigration()
        result = await migration.run(target_session)

        assert result.rows_read == 0
        assert result.rows_written == 0
        assert result.rows_errored == 0
        assert result.status == "success"

    @patch("app.migrations.base.create_async_engine")
    @patch("app.migrations.base.async_sessionmaker")
    async def test_run_processes_batches_correctly(
        self,
        mock_session_factory: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """run() should process all rows across multiple batches."""
        target_session = self._make_target_session()

        # Simulate 3 rows with batch_size=2: two batches (2 rows + 1 row)
        batch1_rows = [("row1",), ("row2",)]
        batch2_rows = [("row3",)]

        call_count = 0

        async def mock_execute(query, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            mock_result = MagicMock()
            mock_result.keys.return_value = ["id"]
            if call_count == 1:
                mock_result.fetchall.return_value = batch1_rows
            else:
                mock_result.fetchall.return_value = batch2_rows
            return mock_result

        mock_source_session = AsyncMock()
        mock_source_session.execute = mock_execute
        mock_source_session.__aenter__ = AsyncMock(return_value=mock_source_session)
        mock_source_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory.return_value.return_value = mock_source_session
        engine_mock = AsyncMock()
        engine_mock.dispose = AsyncMock()
        mock_create_engine.return_value = engine_mock

        migration = StubMigration()
        result = await migration.run(target_session)

        assert result.rows_read == 3
        assert result.rows_written == 3
        assert result.rows_errored == 0
        assert result.status == "success"

    @patch("app.migrations.base.create_async_engine")
    @patch("app.migrations.base.async_sessionmaker")
    async def test_run_logs_transform_errors_to_de_migration_errors(
        self,
        mock_session_factory: MagicMock,
        mock_create_engine: MagicMock,
    ) -> None:
        """Rows that fail transform should be logged to de_migration_errors."""
        target_session = self._make_target_session()

        mock_source_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = [("bad_row",)]
        mock_source_session.execute = AsyncMock(return_value=mock_result)
        mock_source_session.__aenter__ = AsyncMock(return_value=mock_source_session)
        mock_source_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory.return_value.return_value = mock_source_session
        engine_mock = AsyncMock()
        engine_mock.dispose = AsyncMock()
        mock_create_engine.return_value = engine_mock

        migration = StubMigration(fail_transform=True)
        result = await migration.run(target_session)

        assert result.rows_errored == 1
        # DeMigrationErrors should have been added for the failing row
        added_objects = [call[0][0] for call in target_session.add.call_args_list]
        error_objects = [o for o in added_objects if hasattr(o, "error_reason")]
        assert len(error_objects) >= 1
        assert "transform_error" in error_objects[0].error_reason

    def test_run_fails_when_source_url_empty(self) -> None:
        """run() should return status=failed if source DB URL is not configured."""
        migration = StubMigration(source_url="")
        target_session = self._make_target_session()

        import asyncio

        result = asyncio.get_event_loop().run_until_complete(migration.run(target_session))
        assert result.status == "failed"
        assert result.rows_read == 0


# ---------------------------------------------------------------------------
# BaseMigration.validate() tests
# ---------------------------------------------------------------------------


class TestBaseMigrationValidate:
    def _make_migration_log(self, rows_read: int, rows_written: int) -> MagicMock:
        log = MagicMock()
        log.id = 42
        log.rows_read = rows_read
        log.rows_written = rows_written
        return log

    async def test_validate_passes_high_write_rate(self) -> None:
        target_session = AsyncMock()
        migration = StubMigration()
        log = self._make_migration_log(rows_read=100, rows_written=99)
        result = await migration.validate(target_session, log)
        assert result is True

    async def test_validate_fails_low_write_rate(self) -> None:
        target_session = AsyncMock()
        migration = StubMigration()
        log = self._make_migration_log(rows_read=100, rows_written=90)
        result = await migration.validate(target_session, log)
        assert result is False

    async def test_validate_passes_exact_threshold(self) -> None:
        """99 out of 100 is exactly the 99% threshold — should pass."""
        target_session = AsyncMock()
        migration = StubMigration()
        log = self._make_migration_log(rows_read=100, rows_written=99)
        result = await migration.validate(target_session, log)
        assert result is True

    async def test_validate_zero_source_rows_passes(self) -> None:
        """Zero source rows should pass validation (nothing to validate)."""
        target_session = AsyncMock()
        migration = StubMigration()
        log = self._make_migration_log(rows_read=0, rows_written=0)
        result = await migration.validate(target_session, log)
        assert result is True
