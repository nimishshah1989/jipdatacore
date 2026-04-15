"""Equity fundamentals snapshot — valuation, profitability, ownership from Screener.in."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeEquityFundamentals(Base):
    __tablename__ = "de_equity_fundamentals"
    __table_args__ = (
        sa.PrimaryKeyConstraint("instrument_id", "as_of_date"),
    )

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
    )
    as_of_date: Mapped[date] = mapped_column(sa.Date, nullable=False)

    # Valuation
    market_cap_cr: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 2), nullable=True)
    pe_ratio: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    pb_ratio: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    peg_ratio: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    ev_ebitda: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)

    # Profitability
    roe_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(8, 4), nullable=True)
    roce_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(8, 4), nullable=True)
    operating_margin_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(8, 4), nullable=True)
    net_margin_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(8, 4), nullable=True)

    # Balance sheet
    debt_to_equity: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    interest_coverage: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    current_ratio: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)

    # Per-share
    eps_ttm: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)
    book_value: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)
    face_value: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 2), nullable=True)
    dividend_per_share: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)
    dividend_yield_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(8, 4), nullable=True)

    # Ownership
    promoter_holding_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(6, 2), nullable=True)
    pledged_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(6, 2), nullable=True)
    fii_holding_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(6, 2), nullable=True)
    dii_holding_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(6, 2), nullable=True)

    # Growth (TTM or latest FY)
    revenue_growth_yoy_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)
    profit_growth_yoy_pct: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(10, 4), nullable=True)

    # 52-week
    high_52w: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)
    low_52w: Mapped[Optional[Decimal]] = mapped_column(sa.Numeric(18, 4), nullable=True)

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
