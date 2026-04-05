"""ISIN → instrument_id resolution helpers.

Resolves ISINs against de_instrument table. Unresolved ISINs are allowed
(instrument_id = NULL) — not all MF holdings will have a listed equity match.
"""

from __future__ import annotations

import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument

logger = get_logger(__name__)


async def resolve_isin(
    session: AsyncSession,
    isin: str,
) -> Optional[uuid.UUID]:
    """Resolve a single ISIN to its instrument_id.

    Queries de_instrument for a matching isin value.
    Returns None if not found — callers must handle NULL gracefully.

    Args:
        session: Active async database session.
        isin: 12-character ISIN string.

    Returns:
        UUID instrument_id, or None if no match.
    """
    if not isin or len(isin) != 12:
        logger.debug("isin_resolver_invalid_isin", isin=isin)
        return None

    result = await session.execute(
        select(DeInstrument.id).where(DeInstrument.isin == isin).limit(1)
    )
    row = result.scalar_one_or_none()

    if row is None:
        logger.debug("isin_resolver_not_found", isin=isin)
    else:
        logger.debug("isin_resolver_found", isin=isin, instrument_id=str(row))

    return row


async def resolve_isin_batch(
    session: AsyncSession,
    isins: list[str],
) -> dict[str, Optional[uuid.UUID]]:
    """Resolve a batch of ISINs to instrument_ids in a single query.

    Returns a dict mapping each input ISIN to its instrument_id (or None).
    ISINs with invalid format are mapped to None without querying the DB.

    Args:
        session: Active async database session.
        isins: List of ISIN strings to resolve.

    Returns:
        Dict[isin, Optional[uuid.UUID]].
    """
    if not isins:
        return {}

    # Deduplicate and filter valid ISINs
    valid_isins = list({isin for isin in isins if isin and len(isin) == 12})
    invalid_isins = [isin for isin in isins if not isin or len(isin) != 12]

    result_map: dict[str, Optional[uuid.UUID]] = {}

    # Pre-populate invalid ones with None
    for isin in invalid_isins:
        result_map[isin] = None

    if not valid_isins:
        return result_map

    rows = await session.execute(
        select(DeInstrument.isin, DeInstrument.id).where(
            DeInstrument.isin.in_(valid_isins)
        )
    )

    found_map: dict[str, uuid.UUID] = {}
    for isin_val, instrument_id in rows:
        if isin_val:
            found_map[isin_val] = instrument_id

    for isin in valid_isins:
        result_map[isin] = found_map.get(isin)

    resolved_count = sum(1 for v in result_map.values() if v is not None)
    logger.info(
        "isin_resolver_batch_complete",
        total=len(isins),
        valid=len(valid_isins),
        resolved=resolved_count,
        unresolved=len(valid_isins) - resolved_count,
    )
    return result_map
