"""Pipeline state tables — source files, logs, flags, migration tracking."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeSourceFiles(Base):
    """Tracks every ingested file with deduplication via checksum."""

    __tablename__ = "de_source_files"
    __table_args__ = (
        UniqueConstraint("source_name", "file_date", "checksum", name="uq_source_files_dedup"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source_name: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    file_name: Mapped[str] = mapped_column(sa.String(500), nullable=False)
    file_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    checksum: Mapped[Optional[str]] = mapped_column(sa.String(64), nullable=True)
    file_size_bytes: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    row_count: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    format_version: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DePipelineLog(Base):
    """Pipeline execution log with status tracking."""

    __tablename__ = "de_pipeline_log"
    __table_args__ = (
        UniqueConstraint("pipeline_name", "business_date", "run_number", name="uq_pipeline_log_run"),
        sa.CheckConstraint(
            "status IN ('pending','running','success','partial','failed','skipped')",
            name="chk_pipeline_log_status",
        ),
    )

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    business_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    run_number: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)
    status: Mapped[str] = mapped_column(sa.String(20), nullable=False, default="pending")
    started_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    rows_processed: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    rows_failed: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    source_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    source_rowcount: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    source_checksum: Mapped[Optional[str]] = mapped_column(sa.String(64), nullable=True)
    error_detail: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    track_status: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeSystemFlags(Base):
    """System-wide boolean feature flags."""

    __tablename__ = "de_system_flags"

    key: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    value: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    updated_by: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    reason: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeMigrationLog(Base):
    """Tracks data migrations from legacy databases."""

    __tablename__ = "de_migration_log"
    __table_args__ = (
        sa.CheckConstraint(
            "status IN ('pending','running','success','failed','partial')",
            name="chk_migration_log_status",
        ),
    )

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    source_db: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    source_table: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    target_table: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    rows_read: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    rows_written: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    rows_errored: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(sa.String(20), nullable=False, default="pending")
    started_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    checksum_source: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    checksum_dest: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMigrationErrors(Base):
    """Individual row-level errors during data migration."""

    __tablename__ = "de_migration_errors"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    migration_id: Mapped[int] = mapped_column(
        sa.Integer,
        ForeignKey("de_migration_log.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_row: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    error_reason: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeRequestLog(Base):
    """API request audit log."""

    __tablename__ = "de_request_log"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    request_id: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    actor: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    source_ip: Mapped[Optional[Any]] = mapped_column(INET, nullable=True)
    method: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    endpoint: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    status_code: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    requested_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
