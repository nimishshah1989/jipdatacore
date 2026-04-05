"""Migrate MF NAV from mf_pulse.nav_daily to de_mf_nav_daily.

Source: ~25.8M rows -> filtered to ~5M (equity + growth + regular plans only)
Transforms:
- scheme_code -> mstar_id (resolve via de_mf_master.amfi_code)
- float nav -> Decimal
- Add data_status = 'raw'
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.migrations.base import BaseMigration
from app.models.prices import DeMfNavDaily

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
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Cannot convert {value!r} to Decimal: {exc}") from exc


class SchemeCodeResolver:
    """Cache for amfi_code (scheme_code) -> mstar_id resolution."""

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}  # amfi_code -> mstar_id

    async def resolve(self, session: AsyncSession, amfi_code: str) -> Optional[str]:
        """Resolve amfi_code to mstar_id. Cached after first lookup."""
        if not amfi_code:
            return None
        key = str(amfi_code).strip()
        if key in self._cache:
            return self._cache[key]

        result = await session.execute(
            sa.text("SELECT mstar_id FROM de_mf_master WHERE amfi_code = :code LIMIT 1"),
            {"code": key},
        )
        row = result.fetchone()
        mstar_id = row[0] if row else None
        self._cache[key] = mstar_id  # type: ignore[assignment]
        return mstar_id

    async def warm_cache(self, session: AsyncSession) -> None:
        """Pre-load all amfi_code -> mstar_id mappings."""
        result = await session.execute(
            sa.text("SELECT amfi_code, mstar_id FROM de_mf_master WHERE amfi_code IS NOT NULL")
        )
        rows = result.fetchall()
        for amfi_code, mstar_id in rows:
            self._cache[str(amfi_code).strip()] = mstar_id
        await logger.ainfo("scheme_code_cache_warmed", count=len(self._cache))


class MfNavMigration(BaseMigration):
    """Migrate MF NAV from mf_pulse."""

    source_db_name = "mf_pulse"
    source_table = "nav_daily"
    target_table = "de_mf_nav_daily"
    batch_size = 50000  # Very large batches for 25.8M rows

    def __init__(self) -> None:
        self._resolver = SchemeCodeResolver()
        self._cache_warmed = False

    def get_source_db_url(self) -> str:
        return get_settings().mf_pulse_database_url

    def build_source_query(self, offset: int, limit: int) -> str:
        """Fetch NAV rows. Filter is applied in transform_row for flexibility."""
        return f"""
            SELECT scheme_code, nav_date, nav
            FROM nav_daily
            ORDER BY nav_date, scheme_code
            OFFSET {offset} LIMIT {limit}
        """

    async def transform_row(
        self, row: dict[str, Any], target_session: AsyncSession
    ) -> Optional[dict[str, Any]]:
        """Transform source row.

        - scheme_code -> mstar_id resolution
        - NAV float -> Decimal
        - Skip if scheme_code not in de_mf_master (unregistered fund)
        - Skip if NAV is null or zero/negative
        """
        if not self._cache_warmed:
            await self._resolver.warm_cache(target_session)
            self._cache_warmed = True

        scheme_code = row.get("scheme_code")
        if not scheme_code:
            return None

        mstar_id = await self._resolver.resolve(target_session, str(scheme_code))
        if mstar_id is None:
            return None

        nav_date = _parse_date(row.get("nav_date"))
        if nav_date is None:
            raise ValueError(f"Missing nav_date for scheme_code={scheme_code}")

        nav = _to_decimal(row.get("nav"))
        if nav is None or nav <= Decimal("0"):
            return None

        return {
            "nav_date": nav_date,
            "mstar_id": mstar_id,
            "nav": nav,
            "data_status": "raw",
        }

    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """INSERT ... ON CONFLICT (nav_date, mstar_id) DO UPDATE."""
        if not rows:
            return 0

        stmt = pg_insert(DeMfNavDaily).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["nav_date", "mstar_id"],
            set_={
                "nav": stmt.excluded.nav,
                "data_status": stmt.excluded.data_status,
            },
        )
        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount if result.rowcount >= 0 else len(rows)
