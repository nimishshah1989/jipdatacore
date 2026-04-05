"""Daily cost guardrails for qualitative document processing.

Caps:
- 200 documents per day total
- 50 documents per source per day
- 10 audio files per day
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.qualitative import DeQualDocuments

logger = get_logger(__name__)

DAILY_TOTAL_CAP = 200
PER_SOURCE_CAP = 50
DAILY_AUDIO_CAP = 10


class CostLimitExceededError(Exception):
    """Raised when a daily processing cap is exceeded."""

    def __init__(self, cap_type: str, current: int, limit: int) -> None:
        super().__init__(
            f"Cost limit exceeded: cap_type={cap_type!r}, current={current}, limit={limit}"
        )
        self.cap_type = cap_type
        self.current = current
        self.limit = limit


async def check_daily_total_cap(
    session: AsyncSession,
    today: Optional[date] = None,
) -> tuple[bool, int]:
    """Check if the daily total document cap (200) has been reached.

    Args:
        session: Async SQLAlchemy session.
        today: Date to check (defaults to today UTC).

    Returns:
        Tuple of (allowed, current_count).

    Raises:
        CostLimitExceededError: When current_count >= DAILY_TOTAL_CAP.
    """
    if today is None:
        from datetime import datetime, timezone

        today = datetime.now(tz=timezone.utc).date()

    stmt = sa.select(sa.func.count()).where(
        sa.cast(DeQualDocuments.ingested_at, sa.Date) == today,
    )
    result = await session.execute(stmt)
    count: int = result.scalar_one()

    if count >= DAILY_TOTAL_CAP:
        raise CostLimitExceededError(
            cap_type="daily_total",
            current=count,
            limit=DAILY_TOTAL_CAP,
        )

    logger.debug("cost_guard_daily_total_ok", count=count, cap=DAILY_TOTAL_CAP)
    return True, count


async def check_per_source_cap(
    session: AsyncSession,
    source_id: int,
    today: Optional[date] = None,
) -> tuple[bool, int]:
    """Check if the per-source daily cap (50) has been reached.

    Args:
        session: Async SQLAlchemy session.
        source_id: Source to check.
        today: Date to check (defaults to today UTC).

    Returns:
        Tuple of (allowed, current_count).

    Raises:
        CostLimitExceededError: When current_count >= PER_SOURCE_CAP.
    """
    if today is None:
        from datetime import datetime, timezone

        today = datetime.now(tz=timezone.utc).date()

    stmt = sa.select(sa.func.count()).where(
        DeQualDocuments.source_id == source_id,
        sa.cast(DeQualDocuments.ingested_at, sa.Date) == today,
    )
    result = await session.execute(stmt)
    count: int = result.scalar_one()

    if count >= PER_SOURCE_CAP:
        raise CostLimitExceededError(
            cap_type="per_source",
            current=count,
            limit=PER_SOURCE_CAP,
        )

    logger.debug("cost_guard_per_source_ok", source_id=source_id, count=count, cap=PER_SOURCE_CAP)
    return True, count


async def check_audio_cap(
    session: AsyncSession,
    today: Optional[date] = None,
) -> tuple[bool, int]:
    """Check if the daily audio file cap (10) has been reached.

    Args:
        session: Async SQLAlchemy session.
        today: Date to check (defaults to today UTC).

    Returns:
        Tuple of (allowed, current_count).

    Raises:
        CostLimitExceededError: When current_count >= DAILY_AUDIO_CAP.
    """
    if today is None:
        from datetime import datetime, timezone

        today = datetime.now(tz=timezone.utc).date()

    stmt = sa.select(sa.func.count()).where(
        DeQualDocuments.original_format == "audio",
        sa.cast(DeQualDocuments.ingested_at, sa.Date) == today,
    )
    result = await session.execute(stmt)
    count: int = result.scalar_one()

    if count >= DAILY_AUDIO_CAP:
        raise CostLimitExceededError(
            cap_type="audio_daily",
            current=count,
            limit=DAILY_AUDIO_CAP,
        )

    logger.debug("cost_guard_audio_ok", count=count, cap=DAILY_AUDIO_CAP)
    return True, count


async def check_all_caps(
    session: AsyncSession,
    source_id: int,
    is_audio: bool = False,
    today: Optional[date] = None,
) -> None:
    """Run all applicable cost cap checks.

    Checks in order: daily total → per source → audio (if is_audio=True).
    Raises CostLimitExceededError on the first cap hit.

    Args:
        session: Async SQLAlchemy session.
        source_id: Source to check per-source cap for.
        is_audio: Whether this document is an audio file.
        today: Date to check (defaults to today UTC).
    """
    if today is None:
        from datetime import datetime, timezone

        today = datetime.now(tz=timezone.utc).date()

    await check_daily_total_cap(session, today=today)
    await check_per_source_cap(session, source_id=source_id, today=today)
    if is_audio:
        await check_audio_cap(session, today=today)
