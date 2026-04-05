"""Migrate index constituents from fie_v3 to de_index_constituents.

Source: ~4,638 rows
Small migration — resolves symbol -> instrument_id, straightforward mapping.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.migrations.base import BaseMigration
from app.models.instruments import DeIndexConstituents

logger = get_logger(__name__)

_DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"]


def _parse_date(value: Any) -> Optional[dt.date]:
    """Parse a date from string or date object."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        for fmt in _DATE_FORMATS:
            try:
                return dt.datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert to Decimal or None."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


class IndexSymbolResolver:
    """Cache for symbol -> instrument_id resolution for index constituents."""

    def __init__(self) -> None:
        self._cache: dict[str, Optional[uuid.UUID]] = {}

    async def resolve(self, session: AsyncSession, symbol: str) -> Optional[uuid.UUID]:
        """Resolve symbol to instrument_id. Cached after first lookup."""
        if not symbol:
            return None
        key = symbol.strip().upper()
        if key in self._cache:
            return self._cache[key]

        result = await session.execute(
            sa.text("SELECT id FROM de_instrument WHERE UPPER(current_symbol) = :symbol LIMIT 1"),
            {"symbol": key},
        )
        row = result.fetchone()
        instrument_id = row[0] if row else None
        self._cache[key] = instrument_id
        return instrument_id

    async def warm_cache(self, session: AsyncSession) -> None:
        """Pre-load all active instruments."""
        result = await session.execute(
            sa.text("SELECT UPPER(current_symbol), id FROM de_instrument WHERE is_active = TRUE")
        )
        rows = result.fetchall()
        for symbol, instrument_id in rows:
            self._cache[symbol] = instrument_id
        await logger.ainfo("index_symbol_cache_warmed", count=len(self._cache))


class IndexConstituentsMigration(BaseMigration):
    """Migrate index constituents from fie_v3."""

    source_db_name = "fie_v3"
    source_table = "index_constituents"
    target_table = "de_index_constituents"
    batch_size = 5000

    def __init__(self) -> None:
        self._resolver = IndexSymbolResolver()
        self._cache_warmed = False

    def get_source_db_url(self) -> str:
        return get_settings().fie_v3_database_url

    def build_source_query(self, offset: int, limit: int) -> str:
        return f"""
            SELECT index_code, symbol, effective_from, effective_to, weight_pct
            FROM index_constituents
            ORDER BY index_code, effective_from, symbol
            OFFSET {offset} LIMIT {limit}
        """

    async def transform_row(
        self, row: dict[str, Any], target_session: AsyncSession
    ) -> Optional[dict[str, Any]]:
        """Transform index constituent row.

        - symbol -> instrument_id
        - date parsing
        """
        if not self._cache_warmed:
            await self._resolver.warm_cache(target_session)
            self._cache_warmed = True

        index_code = row.get("index_code")
        if not index_code:
            return None

        symbol = row.get("symbol")
        if not symbol:
            return None

        instrument_id = await self._resolver.resolve(target_session, str(symbol))
        if instrument_id is None:
            return None

        effective_from = _parse_date(row.get("effective_from"))
        if effective_from is None:
            raise ValueError(f"Missing effective_from for index_code={index_code} symbol={symbol}")

        return {
            "index_code": str(index_code).strip().upper(),
            "instrument_id": instrument_id,
            "effective_from": effective_from,
            "effective_to": _parse_date(row.get("effective_to")),
            "weight_pct": _to_decimal(row.get("weight_pct")),
        }

    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """INSERT ... ON CONFLICT (index_code, instrument_id, effective_from) DO UPDATE."""
        if not rows:
            return 0

        stmt = pg_insert(DeIndexConstituents).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["index_code", "instrument_id", "effective_from"],
            set_={
                "effective_to": stmt.excluded.effective_to,
                "weight_pct": stmt.excluded.weight_pct,
            },
        )
        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount if result.rowcount >= 0 else len(rows)
