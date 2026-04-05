"""MF lifecycle detection — merge and closure events."""

from __future__ import annotations

from datetime import date
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeMfLifecycle, DeMfMaster

logger = get_logger(__name__)


async def detect_merged_funds(
    session: AsyncSession,
    active_amfi_codes: set[str],
    business_date: date,
) -> list[str]:
    """Detect funds that are no longer present in today's AMFI feed.

    A fund is considered merged/closed if:
    - It was previously is_active=True in de_mf_master
    - Its amfi_code is NOT in today's active_amfi_codes set

    Returns list of mstar_ids that were deactivated.
    """
    result = await session.execute(
        select(DeMfMaster.mstar_id, DeMfMaster.amfi_code).where(
            DeMfMaster.is_active == True,  # noqa: E712
            DeMfMaster.amfi_code.is_not(None),
            DeMfMaster.closure_date.is_(None),
        )
    )
    all_active = result.all()

    deactivated_mstar_ids: list[str] = []

    for mstar_id, amfi_code in all_active:
        if amfi_code and str(amfi_code) not in active_amfi_codes:
            deactivated_mstar_ids.append(mstar_id)
            logger.info(
                "mf_merge_detected",
                mstar_id=mstar_id,
                amfi_code=amfi_code,
                business_date=business_date.isoformat(),
            )

    return deactivated_mstar_ids


async def mark_funds_inactive(
    session: AsyncSession,
    mstar_ids: list[str],
    business_date: date,
    merged_into_mstar_id: Optional[str] = None,
) -> int:
    """Set is_active=False and optionally merged_into_mstar_id on de_mf_master.

    Returns number of rows updated.
    """
    if not mstar_ids:
        return 0

    values: dict = {"is_active": False}
    if merged_into_mstar_id is not None:
        values["merged_into_mstar_id"] = merged_into_mstar_id

    result = await session.execute(
        update(DeMfMaster)
        .where(DeMfMaster.mstar_id.in_(mstar_ids))
        .values(**values)
        .returning(DeMfMaster.mstar_id)
    )
    updated_ids = result.fetchall()
    count = len(updated_ids)

    logger.info(
        "mf_funds_marked_inactive",
        count=count,
        merged_into=merged_into_mstar_id,
        business_date=business_date.isoformat(),
    )
    return count


async def mark_fund_closed(
    session: AsyncSession,
    mstar_id: str,
    closure_date: date,
) -> None:
    """Set closure_date and is_active=False on a specific fund."""
    await session.execute(
        update(DeMfMaster)
        .where(DeMfMaster.mstar_id == mstar_id)
        .values(is_active=False, closure_date=closure_date)
    )
    logger.info("mf_fund_closed", mstar_id=mstar_id, closure_date=closure_date.isoformat())


async def insert_lifecycle_event(
    session: AsyncSession,
    mstar_id: str,
    event_type: str,
    event_date: date,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """Insert a lifecycle event into de_mf_lifecycle.

    event_type must be one of:
      launch, merge, name_change, category_change, amc_change,
      closure, benchmark_change, reopen
    """
    await session.execute(
        sa.insert(DeMfLifecycle).values(
            mstar_id=mstar_id,
            event_type=event_type,
            event_date=event_date,
            old_value=old_value,
            new_value=new_value,
            notes=notes,
        )
    )
    logger.info(
        "mf_lifecycle_event_inserted",
        mstar_id=mstar_id,
        event_type=event_type,
        event_date=event_date.isoformat(),
    )


async def run_lifecycle_check(
    session: AsyncSession,
    active_amfi_codes: set[str],
    business_date: date,
) -> int:
    """Run full lifecycle detection for today's business_date.

    Steps:
    1. Detect merged/disappeared funds
    2. Mark them inactive in de_mf_master
    3. Insert 'merge' lifecycle events

    Returns total number of lifecycle events created.
    """
    deactivated = await detect_merged_funds(session, active_amfi_codes, business_date)

    events_created = 0
    for mstar_id in deactivated:
        await mark_funds_inactive(session, [mstar_id], business_date)
        await insert_lifecycle_event(
            session,
            mstar_id=mstar_id,
            event_type="merge",
            event_date=business_date,
            notes="Fund no longer present in AMFI NAVAll.txt feed",
        )
        events_created += 1

    logger.info(
        "lifecycle_check_complete",
        business_date=business_date.isoformat(),
        events_created=events_created,
    )
    return events_created
