"""Migrate MF holdings from mf_pulse.fund_holding_detail to de_mf_holdings.

Source: ~2M+ rows
Transforms:
- ISIN -> instrument_id resolution (de_instrument.isin)
- scheme_code -> mstar_id resolution (de_mf_master.amfi_code)
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
from app.models.holdings import DeMfHoldings

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


class IsinResolver:
    """Cache for ISIN -> instrument_id resolution."""

    def __init__(self) -> None:
        self._cache: dict[str, Optional[uuid.UUID]] = {}

    async def resolve(self, session: AsyncSession, isin: str) -> Optional[uuid.UUID]:
        """Resolve ISIN to instrument_id. Cached after first lookup."""
        if not isin:
            return None
        key = isin.strip().upper()
        if key in self._cache:
            return self._cache[key]

        result = await session.execute(
            sa.text("SELECT id FROM de_instrument WHERE isin = :isin LIMIT 1"),
            {"isin": key},
        )
        row = result.fetchone()
        instrument_id = row[0] if row else None
        self._cache[key] = instrument_id
        return instrument_id

    async def warm_cache(self, session: AsyncSession) -> None:
        """Pre-load all ISIN -> instrument_id mappings."""
        result = await session.execute(
            sa.text("SELECT UPPER(isin), id FROM de_instrument WHERE isin IS NOT NULL")
        )
        rows = result.fetchall()
        for isin, instrument_id in rows:
            self._cache[isin] = instrument_id
        await logger.ainfo("isin_cache_warmed", count=len(self._cache))


class HoldingsSchemeResolver:
    """Cache for amfi_code -> mstar_id resolution for holdings."""

    def __init__(self) -> None:
        self._cache: dict[str, Optional[str]] = {}

    async def resolve(self, session: AsyncSession, scheme_code: str) -> Optional[str]:
        """Resolve scheme_code to mstar_id. Cached after first lookup."""
        if not scheme_code:
            return None
        key = str(scheme_code).strip()
        if key in self._cache:
            return self._cache[key]

        result = await session.execute(
            sa.text("SELECT mstar_id FROM de_mf_master WHERE amfi_code = :code LIMIT 1"),
            {"code": key},
        )
        row = result.fetchone()
        mstar_id = row[0] if row else None
        self._cache[key] = mstar_id
        return mstar_id

    async def warm_cache(self, session: AsyncSession) -> None:
        """Pre-load all amfi_code -> mstar_id mappings."""
        result = await session.execute(
            sa.text("SELECT amfi_code, mstar_id FROM de_mf_master WHERE amfi_code IS NOT NULL")
        )
        rows = result.fetchall()
        for amfi_code, mstar_id in rows:
            self._cache[str(amfi_code).strip()] = mstar_id
        await logger.ainfo("holdings_scheme_cache_warmed", count=len(self._cache))


class MfHoldingsMigration(BaseMigration):
    """Migrate MF portfolio holdings from mf_pulse."""

    source_db_name = "mf_pulse"
    source_table = "fund_holding_detail"
    target_table = "de_mf_holdings"
    batch_size = 10000

    def __init__(self) -> None:
        self._isin_resolver = IsinResolver()
        self._scheme_resolver = HoldingsSchemeResolver()
        self._cache_warmed = False

    def get_source_db_url(self) -> str:
        return get_settings().mf_pulse_database_url

    def build_source_query(self, offset: int, limit: int) -> str:
        return f"""
            SELECT
                scheme_code,
                as_of_date,
                holding_name,
                isin,
                weight_pct,
                shares_held,
                market_value,
                sector_code
            FROM fund_holding_detail
            ORDER BY as_of_date, scheme_code, isin
            OFFSET {offset} LIMIT {limit}
        """

    async def transform_row(
        self, row: dict[str, Any], target_session: AsyncSession
    ) -> Optional[dict[str, Any]]:
        """Transform holdings row.

        - scheme_code -> mstar_id
        - ISIN -> instrument_id (optional, set is_mapped flag)
        - Numeric conversions
        """
        if not self._cache_warmed:
            await self._isin_resolver.warm_cache(target_session)
            await self._scheme_resolver.warm_cache(target_session)
            self._cache_warmed = True

        scheme_code = row.get("scheme_code")
        if not scheme_code:
            return None

        mstar_id = await self._scheme_resolver.resolve(target_session, str(scheme_code))
        if mstar_id is None:
            return None

        as_of_date = _parse_date(row.get("as_of_date"))
        if as_of_date is None:
            raise ValueError(f"Missing as_of_date for scheme_code={scheme_code}")

        isin = str(row["isin"]).strip().upper() if row.get("isin") else None
        instrument_id: Optional[uuid.UUID] = None
        is_mapped = False

        if isin:
            instrument_id = await self._isin_resolver.resolve(target_session, isin)
            is_mapped = instrument_id is not None

        return {
            "mstar_id": mstar_id,
            "as_of_date": as_of_date,
            "holding_name": str(row["holding_name"]).strip() if row.get("holding_name") else None,
            "isin": isin,
            "instrument_id": instrument_id,
            "weight_pct": _to_decimal(row.get("weight_pct")),
            "shares_held": int(row["shares_held"]) if row.get("shares_held") is not None else None,
            "market_value": _to_decimal(row.get("market_value")),
            "sector_code": str(row["sector_code"]).strip() if row.get("sector_code") else None,
            "is_mapped": is_mapped,
        }

    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """INSERT ... ON CONFLICT (mstar_id, as_of_date, isin) DO UPDATE."""
        if not rows:
            return 0

        stmt = pg_insert(DeMfHoldings).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_mf_holdings",
            set_={
                "holding_name": stmt.excluded.holding_name,
                "instrument_id": stmt.excluded.instrument_id,
                "weight_pct": stmt.excluded.weight_pct,
                "shares_held": stmt.excluded.shares_held,
                "market_value": stmt.excluded.market_value,
                "sector_code": stmt.excluded.sector_code,
                "is_mapped": stmt.excluded.is_mapped,
            },
        )
        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount if result.rowcount >= 0 else len(rows)
