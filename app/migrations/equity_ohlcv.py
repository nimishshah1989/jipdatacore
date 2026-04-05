"""Migrate equity OHLCV from fie_v3.compass_stock_prices to de_equity_ohlcv.

Source: ~1.4M rows
Transforms:
- VARCHAR date -> DATE type
- DOUBLE precision -> NUMERIC(18,4)
- symbol -> instrument_id (resolve via de_instrument.current_symbol)
- Add data_status = 'raw'
"""

from __future__ import annotations

import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.migrations.base import BaseMigration
from app.models.prices import DeEquityOhlcv

logger = get_logger(__name__)

# Supported date formats in source data
_DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"]


def _parse_date(value: Any) -> Optional[Any]:
    """Parse a date value from various string/date formats."""
    if value is None:
        return None
    # Already a date or datetime
    import datetime as dt

    if isinstance(value, (dt.date, dt.datetime)):
        return value.date() if isinstance(value, dt.datetime) else value
    if isinstance(value, str):
        value = value.strip()
        for fmt in _DATE_FORMATS:
            try:
                return dt.datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Cannot parse date: {value!r}")


def _to_decimal(value: Any) -> Optional[Decimal]:
    """Convert a value to Decimal, returning None for nulls."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Cannot convert {value!r} to Decimal: {exc}") from exc


class SymbolResolver:
    """Cache for symbol -> instrument_id resolution."""

    def __init__(self) -> None:
        self._cache: dict[str, uuid.UUID] = {}

    async def resolve(self, session: AsyncSession, symbol: str) -> Optional[uuid.UUID]:
        """Resolve symbol to instrument_id. Cached after first lookup."""
        if not symbol:
            return None
        symbol_upper = symbol.upper().strip()
        if symbol_upper in self._cache:
            return self._cache[symbol_upper]

        result = await session.execute(
            sa.text(
                "SELECT id FROM de_instrument WHERE UPPER(current_symbol) = :symbol LIMIT 1"
            ),
            {"symbol": symbol_upper},
        )
        row = result.fetchone()
        if row:
            instrument_id = row[0]
            self._cache[symbol_upper] = instrument_id
            return instrument_id

        # Cache misses to avoid repeated lookups
        self._cache[symbol_upper] = None  # type: ignore[assignment]
        return None

    async def warm_cache(self, session: AsyncSession) -> None:
        """Pre-load all active instruments into cache."""
        result = await session.execute(
            sa.text("SELECT UPPER(current_symbol), id FROM de_instrument WHERE is_active = TRUE")
        )
        rows = result.fetchall()
        for symbol, instrument_id in rows:
            self._cache[symbol] = instrument_id
        await logger.ainfo("symbol_cache_warmed", count=len(self._cache))


class EquityOhlcvMigration(BaseMigration):
    """Migrate equity OHLCV from fie_v3."""

    source_db_name = "fie_v3"
    source_table = "compass_stock_prices"
    target_table = "de_equity_ohlcv"
    batch_size = 10000  # Large batches for 1.4M rows

    def __init__(self) -> None:
        self._resolver = SymbolResolver()
        self._cache_warmed = False

    def get_source_db_url(self) -> str:
        return get_settings().fie_v3_database_url

    def build_source_query(self, offset: int, limit: int) -> str:
        return f"""
            SELECT symbol, trade_date, open, high, low, close, volume,
                   delivery_vol, delivery_pct, trades
            FROM compass_stock_prices
            ORDER BY trade_date, symbol
            OFFSET {offset} LIMIT {limit}
        """

    async def transform_row(
        self, row: dict[str, Any], target_session: AsyncSession
    ) -> Optional[dict[str, Any]]:
        """Transform a source row to target schema.

        - Parse trade_date VARCHAR -> date
        - Resolve symbol -> instrument_id via cache/lookup
        - Convert float prices -> Decimal
        - Set data_status = 'raw'
        """
        if not self._cache_warmed:
            await self._resolver.warm_cache(target_session)
            self._cache_warmed = True

        symbol = row.get("symbol", "")
        if not symbol:
            return None

        instrument_id = await self._resolver.resolve(target_session, str(symbol))
        if instrument_id is None:
            # Unknown symbol — skip row silently
            return None

        trade_date = _parse_date(row.get("trade_date"))
        if trade_date is None:
            raise ValueError(f"Missing trade_date for symbol={symbol}")

        return {
            "date": trade_date,
            "instrument_id": instrument_id,
            "symbol": str(symbol).upper().strip(),
            "open": _to_decimal(row.get("open")),
            "high": _to_decimal(row.get("high")),
            "low": _to_decimal(row.get("low")),
            "close": _to_decimal(row.get("close")),
            "volume": int(row["volume"]) if row.get("volume") is not None else None,
            "delivery_vol": int(row["delivery_vol"]) if row.get("delivery_vol") is not None else None,
            "delivery_pct": _to_decimal(row.get("delivery_pct")),
            "trades": int(row["trades"]) if row.get("trades") is not None else None,
            "data_status": "raw",
        }

    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """INSERT ... ON CONFLICT (date, instrument_id) DO UPDATE."""
        if not rows:
            return 0

        stmt = pg_insert(DeEquityOhlcv).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "instrument_id"],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "delivery_vol": stmt.excluded.delivery_vol,
                "delivery_pct": stmt.excluded.delivery_pct,
                "trades": stmt.excluded.trades,
                "data_status": stmt.excluded.data_status,
            },
        )
        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount if result.rowcount >= 0 else len(rows)
