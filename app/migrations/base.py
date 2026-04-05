"""Base class for data migrations from legacy databases."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.logging import get_logger
from app.models.pipeline import DeMigrationErrors, DeMigrationLog

logger = get_logger(__name__)


@dataclass
class MigrationResult:
    """Result of a migration run."""

    source_db: str
    source_table: str
    target_table: str
    rows_read: int
    rows_written: int
    rows_errored: int
    status: str  # success/failed/partial
    duration_seconds: float
    errors: list[dict] = field(default_factory=list)


class BaseMigration(ABC):
    """Abstract base for all data migrations.

    Each migration:
    1. Connects to source DB (read-only)
    2. Reads source data in batches
    3. Transforms each row (type conversion, FK resolution, etc.)
    4. Inserts into target table with ON CONFLICT
    5. Logs progress and errors to de_migration_log / de_migration_errors
    """

    source_db_name: str  # e.g. "fie_v3"
    source_table: str  # e.g. "compass_stock_prices"
    target_table: str  # e.g. "de_equity_ohlcv"
    batch_size: int = 5000

    @abstractmethod
    def get_source_db_url(self) -> str:
        """Return the source database connection URL."""
        ...

    @abstractmethod
    def build_source_query(self, offset: int, limit: int) -> str:
        """Return SQL to read a batch from the source table.
        Must include ORDER BY for deterministic pagination."""
        ...

    @abstractmethod
    async def transform_row(self, row: dict[str, Any], target_session: AsyncSession) -> Optional[dict[str, Any]]:
        """Transform a source row to target schema.
        Return None to skip the row. Raise to record as error."""
        ...

    @abstractmethod
    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """Insert a batch of transformed rows. Return count inserted.
        Must use ON CONFLICT for idempotency."""
        ...

    async def get_source_count(self, source_session: AsyncSession) -> int:
        """Get total row count from source for progress tracking."""
        result = await source_session.execute(
            sa.text(f"SELECT COUNT(*) FROM {self.source_table}")
        )
        row = result.fetchone()
        return int(row[0]) if row else 0

    async def run(self, target_session: AsyncSession) -> MigrationResult:
        """Execute the full migration."""
        started_at = datetime.now(tz=timezone.utc)
        start_time = time.monotonic()

        rows_read = 0
        rows_written = 0
        rows_errored = 0
        status = "running"
        error_records: list[dict[str, Any]] = []

        # 1. Create migration log entry
        migration_log = DeMigrationLog(
            source_db=self.source_db_name,
            source_table=self.source_table,
            target_table=self.target_table,
            status="running",
            started_at=started_at,
        )
        target_session.add(migration_log)
        await target_session.flush()
        migration_id = migration_log.id

        await logger.ainfo(
            "migration_started",
            source_db=self.source_db_name,
            source_table=self.source_table,
            target_table=self.target_table,
            migration_id=migration_id,
        )

        # 2. Connect to source DB
        source_url = self.get_source_db_url()
        if not source_url:
            status = "failed"
            migration_log.status = status
            migration_log.completed_at = datetime.now(tz=timezone.utc)
            migration_log.rows_read = 0
            migration_log.rows_written = 0
            migration_log.rows_errored = 0
            migration_log.notes = f"Source DB URL not configured for {self.source_db_name}"
            await target_session.flush()
            duration = time.monotonic() - start_time
            return MigrationResult(
                source_db=self.source_db_name,
                source_table=self.source_table,
                target_table=self.target_table,
                rows_read=0,
                rows_written=0,
                rows_errored=0,
                status=status,
                duration_seconds=duration,
            )

        # Ensure async driver
        if "postgresql://" in source_url and "asyncpg" not in source_url:
            source_url = source_url.replace("postgresql://", "postgresql+asyncpg://")
        if "postgresql+psycopg2" in source_url:
            source_url = source_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")

        source_engine = create_async_engine(source_url, echo=False)
        source_session_factory = async_sessionmaker(source_engine, expire_on_commit=False)

        try:
            async with source_session_factory() as source_session:
                # 3. Read in batches, transform, insert
                offset = 0
                batch_num = 0

                while True:
                    batch_num += 1
                    query = self.build_source_query(offset=offset, limit=self.batch_size)
                    result = await source_session.execute(sa.text(query))
                    column_names = list(result.keys())
                    raw_rows = result.fetchall()

                    if not raw_rows:
                        break

                    rows_read += len(raw_rows)
                    transformed: list[dict[str, Any]] = []

                    for raw_row in raw_rows:
                        row_dict = dict(zip(column_names, raw_row))
                        try:
                            transformed_row = await self.transform_row(row_dict, target_session)
                            if transformed_row is not None:
                                transformed.append(transformed_row)
                        except Exception as exc:
                            rows_errored += 1
                            error_records.append(
                                {
                                    "migration_id": migration_id,
                                    "source_row": {
                                        k: (str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v)
                                        for k, v in row_dict.items()
                                    },
                                    "error_reason": str(exc),
                                }
                            )
                            await logger.awarning(
                                "row_transform_error",
                                migration_id=migration_id,
                                error=str(exc),
                                batch=batch_num,
                            )

                    # Insert transformed batch
                    if transformed:
                        try:
                            inserted = await self.insert_batch(target_session, transformed)
                            rows_written += inserted
                        except Exception as exc:
                            rows_errored += len(transformed)
                            await logger.aerror(
                                "batch_insert_error",
                                migration_id=migration_id,
                                batch=batch_num,
                                error=str(exc),
                            )

                    # 4. Log errors to de_migration_errors (flush per batch)
                    if error_records:
                        for err in error_records:
                            target_session.add(
                                DeMigrationErrors(
                                    migration_id=err["migration_id"],
                                    source_row=err["source_row"],
                                    error_reason=err["error_reason"],
                                )
                            )
                        await target_session.flush()
                        error_records = []

                    await logger.ainfo(
                        "migration_batch_progress",
                        migration_id=migration_id,
                        batch=batch_num,
                        rows_read=rows_read,
                        rows_written=rows_written,
                        rows_errored=rows_errored,
                    )

                    if len(raw_rows) < self.batch_size:
                        break
                    offset += self.batch_size

        except Exception as exc:
            status = "failed"
            await logger.aerror(
                "migration_failed",
                migration_id=migration_id,
                error=str(exc),
            )
        finally:
            await source_engine.dispose()

        # Determine final status
        if status != "failed":
            if rows_errored == 0:
                status = "success"
            elif rows_written > 0:
                status = "partial"
            else:
                status = "failed"

        # 5. Update migration log with final counts
        completed_at = datetime.now(tz=timezone.utc)
        duration = time.monotonic() - start_time

        migration_log.status = status
        migration_log.completed_at = completed_at
        migration_log.rows_read = rows_read
        migration_log.rows_written = rows_written
        migration_log.rows_errored = rows_errored
        await target_session.flush()

        await logger.ainfo(
            "migration_completed",
            migration_id=migration_id,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_errored=rows_errored,
            duration_seconds=round(duration, 2),
        )

        # 6. Run validation gates
        await self.validate(target_session, migration_log)

        return MigrationResult(
            source_db=self.source_db_name,
            source_table=self.source_table,
            target_table=self.target_table,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_errored=rows_errored,
            status=status,
            duration_seconds=duration,
        )

    async def validate(self, target_session: AsyncSession, migration_log: DeMigrationLog) -> bool:
        """Post-migration validation gates:
        - Row count comparison (source vs target, tolerance 1%)
        - Spot check: target rows_written > 0 if source had rows
        Returns True if valid."""
        rows_read = migration_log.rows_read or 0
        rows_written = migration_log.rows_written or 0

        if rows_read == 0:
            await logger.awarning(
                "migration_validate_zero_source",
                migration_id=migration_log.id,
                target_table=self.target_table,
            )
            return True

        # Tolerance gate: at least 99% of rows must be written
        if rows_read > 0:
            write_rate = rows_written / rows_read
            if write_rate < 0.99:
                await logger.awarning(
                    "migration_validate_low_write_rate",
                    migration_id=migration_log.id,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    write_rate=round(write_rate, 4),
                    target_table=self.target_table,
                )
                return False

        await logger.ainfo(
            "migration_validate_passed",
            migration_id=migration_log.id,
            rows_read=rows_read,
            rows_written=rows_written,
            target_table=self.target_table,
        )
        return True
