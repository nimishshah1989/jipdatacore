"""Historical fundamentals time-series from screener.in — annual, quarterly, TTM."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeEquityFundamentalsHistory(Base):
    __tablename__ = "de_equity_fundamentals_history"
    __table_args__ = (
        sa.PrimaryKeyConstraint("instrument_id", "fiscal_period_end", "period_type"),
        Index(
            "ix_de_eq_fh_inst_type_period",
            "instrument_id", "period_type", sa.text("fiscal_period_end DESC"),
        ),
    )

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
    )
    fiscal_period_end: Mapped[date] = mapped_column(sa.Date, nullable=False)
    period_type: Mapped[str] = mapped_column(sa.String(10), nullable=False)

    # P&L
    revenue_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    expenses_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    operating_profit_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    opm_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    other_income_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    interest_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    depreciation_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    profit_before_tax_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    tax_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    net_profit_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    eps: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)

    # Balance sheet
    equity_capital_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    reserves_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    borrowings_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    other_liabilities_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    fixed_assets_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    cwip_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    investments_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    other_assets_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    total_assets_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)

    # Cash flow
    cfo_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    cfi_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    cff_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)

    # Audit
    source: Mapped[str] = mapped_column(sa.String(50), server_default="screener", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
