"""Migrate MF master from mf_pulse.fund_master to de_mf_master.

Source: ~13,380 funds
Straightforward mapping with minimal transformation.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging import get_logger
from app.migrations.base import BaseMigration
from app.models.instruments import DeMfMaster

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


def _to_bool(value: Any, default: bool = False) -> bool:
    """Convert various truthy values to bool."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y", "t")
    return default


class MfMasterMigration(BaseMigration):
    """Migrate MF fund master from mf_pulse."""

    source_db_name = "mf_pulse"
    source_table = "fund_master"
    target_table = "de_mf_master"
    batch_size = 5000

    def get_source_db_url(self) -> str:
        return get_settings().mf_pulse_database_url

    def build_source_query(self, offset: int, limit: int) -> str:
        return f"""
            SELECT
                mstar_id,
                amfi_code,
                isin,
                fund_name,
                amc_name,
                category_name,
                broad_category,
                is_index_fund,
                is_etf,
                is_active,
                inception_date,
                closure_date,
                primary_benchmark,
                expense_ratio,
                investment_strategy
            FROM fund_master
            ORDER BY mstar_id
            OFFSET {offset} LIMIT {limit}
        """

    async def transform_row(
        self, row: dict[str, Any], target_session: AsyncSession
    ) -> Optional[dict[str, Any]]:
        """Map source columns to de_mf_master schema."""
        mstar_id = row.get("mstar_id")
        fund_name = row.get("fund_name")

        if not mstar_id:
            return None
        if not fund_name:
            return None

        return {
            "mstar_id": str(mstar_id).strip(),
            "amfi_code": str(row["amfi_code"]).strip() if row.get("amfi_code") else None,
            "isin": str(row["isin"]).strip() if row.get("isin") else None,
            "fund_name": str(fund_name).strip(),
            "amc_name": str(row["amc_name"]).strip() if row.get("amc_name") else None,
            "category_name": str(row["category_name"]).strip() if row.get("category_name") else None,
            "broad_category": str(row["broad_category"]).strip() if row.get("broad_category") else None,
            "is_index_fund": _to_bool(row.get("is_index_fund")),
            "is_etf": _to_bool(row.get("is_etf")),
            "is_active": _to_bool(row.get("is_active"), default=True),
            "inception_date": _parse_date(row.get("inception_date")),
            "closure_date": _parse_date(row.get("closure_date")),
            "primary_benchmark": str(row["primary_benchmark"]).strip() if row.get("primary_benchmark") else None,
            "expense_ratio": _to_decimal(row.get("expense_ratio")),
            "investment_strategy": str(row["investment_strategy"]).strip() if row.get("investment_strategy") else None,
        }

    async def insert_batch(self, session: AsyncSession, rows: list[dict[str, Any]]) -> int:
        """INSERT ... ON CONFLICT (mstar_id) DO UPDATE."""
        if not rows:
            return 0

        stmt = pg_insert(DeMfMaster).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["mstar_id"],
            set_={
                "amfi_code": stmt.excluded.amfi_code,
                "isin": stmt.excluded.isin,
                "fund_name": stmt.excluded.fund_name,
                "amc_name": stmt.excluded.amc_name,
                "category_name": stmt.excluded.category_name,
                "broad_category": stmt.excluded.broad_category,
                "is_index_fund": stmt.excluded.is_index_fund,
                "is_etf": stmt.excluded.is_etf,
                "is_active": stmt.excluded.is_active,
                "inception_date": stmt.excluded.inception_date,
                "closure_date": stmt.excluded.closure_date,
                "primary_benchmark": stmt.excluded.primary_benchmark,
                "expense_ratio": stmt.excluded.expense_ratio,
                "investment_strategy": stmt.excluded.investment_strategy,
            },
        )
        result = await session.execute(stmt)
        await session.flush()
        return result.rowcount if result.rowcount >= 0 else len(rows)
