"""Derived Metrics Models."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeMfDerivedDaily(Base):
    """Derived mutual fund metrics computed daily from stock-level holdings."""

    __tablename__ = "de_mf_derived_daily"

    nav_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    mstar_id: Mapped[str] = mapped_column(
        sa.String(20),
        ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    derived_rs_composite: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    nav_rs_composite: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    manager_alpha: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    coverage_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    
    # Fund-level risk metrics
    sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    sharpe_3y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    max_drawdown_3y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    volatility_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    volatility_3y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    beta_vs_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
