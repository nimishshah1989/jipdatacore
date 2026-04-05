"""Advisory lock guard for pipeline concurrency control."""

from __future__ import annotations

from datetime import date

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)


async def acquire_pipeline_lock(
    session: AsyncSession,
    pipeline_name: str,
    business_date: date,
) -> bool:
    """Acquire session-level advisory lock using hashtext() for deterministic ID.

    Uses pg_try_advisory_lock (non-blocking) so if another instance is running
    the same pipeline for the same date, we skip rather than block.

    Lock key: hashtext('pipeline_name:YYYY-MM-DD')

    Returns True if lock was acquired, False if already held by another session.
    """
    lock_key = f"{pipeline_name}:{business_date.isoformat()}"
    result = await session.execute(
        sa.text("SELECT pg_try_advisory_lock(hashtext(:key))"),
        {"key": lock_key},
    )
    acquired: bool = result.scalar()
    logger.info(
        "pipeline_lock_attempt",
        pipeline_name=pipeline_name,
        business_date=business_date.isoformat(),
        acquired=acquired,
        lock_key=lock_key,
    )
    return acquired


async def release_pipeline_lock(
    session: AsyncSession,
    pipeline_name: str,
    business_date: date,
) -> None:
    """Release the advisory lock. Should be called in a finally block.

    Note: session-level advisory locks are also released automatically when
    the session/connection is closed.
    """
    lock_key = f"{pipeline_name}:{business_date.isoformat()}"
    await session.execute(
        sa.text("SELECT pg_advisory_unlock(hashtext(:key))"),
        {"key": lock_key},
    )
    logger.info(
        "pipeline_lock_released",
        pipeline_name=pipeline_name,
        business_date=business_date.isoformat(),
        lock_key=lock_key,
    )
