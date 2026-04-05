"""Async symbol → instrument_id resolver with LRU cache."""

from __future__ import annotations

import uuid
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger
from app.models.instruments import DeInstrument

logger = get_logger(__name__)

# In-process LRU cache keyed by symbol string.
# Cache is intentionally process-local and not cleared between pipeline runs within
# the same process — instruments rarely change symbol mid-session.
# Cache invalidation on restart is acceptable.
_SYMBOL_CACHE: dict[str, uuid.UUID] = {}


def _clear_symbol_cache() -> None:
    """Clear the in-process symbol cache. Useful in tests."""
    _SYMBOL_CACHE.clear()


async def resolve_symbol(
    symbol: str,
    session: AsyncSession,
) -> Optional[uuid.UUID]:
    """Resolve a trading symbol to its instrument_id from de_instrument.

    Uses an in-process dict cache to avoid repeated DB lookups for the same
    symbol within a pipeline run.

    Args:
        symbol: NSE equity symbol (e.g. "RELIANCE", "TCS").
        session: Async SQLAlchemy session.

    Returns:
        instrument_id UUID if found, None if symbol not in de_instrument.
    """
    if symbol in _SYMBOL_CACHE:
        return _SYMBOL_CACHE[symbol]

    result = await session.execute(
        select(DeInstrument.id).where(
            DeInstrument.current_symbol == symbol,
            DeInstrument.is_active == sa.true(),
        )
    )
    row = result.scalar_one_or_none()

    if row is not None:
        _SYMBOL_CACHE[symbol] = row
        logger.debug(
            "symbol_resolved",
            symbol=symbol,
            instrument_id=str(row),
        )
    else:
        logger.warning(
            "symbol_not_found",
            symbol=symbol,
        )

    return row


async def bulk_resolve_symbols(
    symbols: list[str],
    session: AsyncSession,
) -> dict[str, uuid.UUID]:
    """Resolve multiple symbols to instrument_ids in a single DB query.

    Fetches all symbols not already cached in one SELECT, then updates the cache.

    Args:
        symbols: List of NSE equity symbols.
        session: Async SQLAlchemy session.

    Returns:
        Dict mapping symbol → instrument_id for all found symbols.
        Unknown symbols are absent from the dict.
    """
    # Identify which symbols need DB lookup
    missing = [s for s in symbols if s not in _SYMBOL_CACHE]

    if missing:
        result = await session.execute(
            select(DeInstrument.current_symbol, DeInstrument.id).where(
                DeInstrument.current_symbol.in_(missing),
                DeInstrument.is_active == sa.true(),
            )
        )
        rows = result.all()
        for sym, inst_id in rows:
            _SYMBOL_CACHE[sym] = inst_id

        resolved_count = len(rows)
        missing_count = len(missing) - resolved_count

        logger.info(
            "bulk_symbol_resolve",
            requested=len(missing),
            resolved=resolved_count,
            not_found=missing_count,
        )

        if missing_count > 0:
            found_symbols = {sym for sym, _ in rows}
            unknown = [s for s in missing if s not in found_symbols]
            logger.warning(
                "bulk_symbol_unknown",
                unknown_symbols=unknown[:20],  # Log first 20 to avoid log bloat
                total_unknown=missing_count,
            )

    # Return resolved symbols (both from cache and freshly fetched)
    return {s: _SYMBOL_CACHE[s] for s in symbols if s in _SYMBOL_CACHE}
