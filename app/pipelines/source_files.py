"""Source file registration and checksum utilities."""

from __future__ import annotations

import hashlib
import uuid
from datetime import date

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DeSourceFiles

logger = get_logger(__name__)


async def register_source_file(
    session: AsyncSession,
    source_name: str,
    file_name: str,
    file_date: date | None = None,
    checksum: str | None = None,
    file_size_bytes: int | None = None,
    row_count: int | None = None,
    format_version: str | None = None,
) -> DeSourceFiles:
    """Register a source file in de_source_files.

    Uses ON CONFLICT (source_name, file_date, checksum) DO UPDATE for idempotency.
    Updates mutable fields (file_name, row_count, file_size_bytes) on conflict
    so re-runs don't fail but do record the latest state.

    Returns the registered (or updated) DeSourceFiles record.
    """
    file_id = uuid.uuid4()

    stmt = pg_insert(DeSourceFiles).values(
        id=file_id,
        source_name=source_name,
        file_name=file_name,
        file_date=file_date,
        checksum=checksum,
        file_size_bytes=file_size_bytes,
        row_count=row_count,
        format_version=format_version,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_source_files_dedup",
        set_={
            "file_name": stmt.excluded.file_name,
            "file_size_bytes": stmt.excluded.file_size_bytes,
            "row_count": stmt.excluded.row_count,
            "format_version": stmt.excluded.format_version,
        },
    ).returning(DeSourceFiles)

    result = await session.execute(stmt)
    record = result.scalar_one()

    logger.info(
        "source_file_registered",
        source_name=source_name,
        file_name=file_name,
        file_date=file_date.isoformat() if file_date else None,
        checksum=checksum,
        row_count=row_count,
        record_id=str(record.id),
    )
    return record


async def compute_file_checksum(file_path: str) -> str:
    """Compute SHA-256 checksum of a file.

    Reads the file in 64KB chunks to handle large files efficiently.
    Returns hex digest string (64 characters).
    """
    sha256 = hashlib.sha256()
    chunk_size = 65536  # 64KB

    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256.update(chunk)

    digest = sha256.hexdigest()
    logger.debug("file_checksum_computed", file_path=file_path, checksum=digest)
    return digest
