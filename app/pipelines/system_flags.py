"""System flags helper for pipeline kill switches and feature flags."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.pipeline import DeSystemFlags

logger = get_logger(__name__)

GLOBAL_KILL_SWITCH_KEY = "global_kill_switch"
PIPELINE_ENABLED_PREFIX = "pipeline_{pipeline_name}_enabled"


async def check_system_flag(session: AsyncSession, flag_key: str) -> bool:
    """Check if a system flag is True.

    Returns False if the flag doesn't exist (safe default — treat missing as disabled).
    """
    result = await session.execute(
        select(DeSystemFlags.value).where(DeSystemFlags.key == flag_key)
    )
    value = result.scalar_one_or_none()
    if value is None:
        logger.debug("system_flag_not_found", flag_key=flag_key, default=False)
        return False
    return bool(value)


async def is_pipeline_enabled(session: AsyncSession, pipeline_name: str) -> bool:
    """Check if a specific pipeline is enabled.

    Checks two flags in order:
    1. global_kill_switch — if True, ALL pipelines are disabled
    2. pipeline_{pipeline_name}_enabled — if False, this specific pipeline is disabled

    Returns True only if global kill switch is NOT active AND pipeline flag is enabled
    (or the pipeline-specific flag doesn't exist — treated as enabled).
    """
    # Check global kill switch — if active, no pipelines run
    global_kill = await check_system_flag(session, GLOBAL_KILL_SWITCH_KEY)
    if global_kill:
        logger.warning(
            "global_kill_switch_active",
            pipeline_name=pipeline_name,
        )
        return False

    # Check pipeline-specific flag — if it exists AND is False, pipeline is disabled
    pipeline_key = f"pipeline_{pipeline_name}_enabled"
    result = await session.execute(
        select(DeSystemFlags.value).where(DeSystemFlags.key == pipeline_key)
    )
    value = result.scalar_one_or_none()

    if value is None:
        # No pipeline-specific flag: treat as enabled (fail-open)
        return True

    enabled = bool(value)
    if not enabled:
        logger.warning(
            "pipeline_flag_disabled",
            pipeline_name=pipeline_name,
            flag_key=pipeline_key,
        )
    return enabled


async def set_system_flag(
    session: AsyncSession,
    flag_key: str,
    value: bool,
    reason: str | None = None,
    updated_by: str = "system",
) -> None:
    """Set a system flag value. Creates the flag if it doesn't exist (upsert).

    Uses PostgreSQL INSERT ... ON CONFLICT DO UPDATE for idempotency.
    """
    now = datetime.now(tz=timezone.utc)

    stmt = pg_insert(DeSystemFlags).values(
        key=flag_key,
        value=value,
        reason=reason,
        updated_by=updated_by,
        updated_at=now,
        created_at=now,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["key"],
        set_={
            "value": stmt.excluded.value,
            "reason": stmt.excluded.reason,
            "updated_by": stmt.excluded.updated_by,
            "updated_at": stmt.excluded.updated_at,
        },
    )
    await session.execute(stmt)
    logger.info(
        "system_flag_set",
        flag_key=flag_key,
        value=value,
        reason=reason,
        updated_by=updated_by,
    )
