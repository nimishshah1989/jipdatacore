"""MF holdings — portfolio holdings disclosure per fund per month."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Index, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeMfHoldings(Base):
    """Monthly MF portfolio holdings disclosure."""

    __tablename__ = "de_mf_holdings"
    __table_args__ = (
        UniqueConstraint("mstar_id", "as_of_date", "isin", name="uq_mf_holdings"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    mstar_id: Mapped[str] = mapped_column(
        sa.String(20),
        ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    as_of_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    holding_name: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    isin: Mapped[Optional[str]] = mapped_column(sa.String(12), nullable=True)
    instrument_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    weight_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    shares_held: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    market_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sector_code: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    is_mapped: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeShareholdingPattern(Base):
    """Quarterly Regulation 31 shareholding disclosure per listed company.

    Promoter / FII / DII / public breakdown sourced from NSE/BSE shareholding
    pattern filings. Summary % by category only — full XBRL parsing deferred.
    Natural key: (symbol, as_of_date) where as_of_date is quarter-end.
    """

    __tablename__ = "de_shareholding_pattern"
    __table_args__ = (
        Index("ix_shareholding_pattern_as_of_date", "as_of_date"),
        Index("ix_shareholding_pattern_symbol", "symbol"),
    )

    symbol: Mapped[str] = mapped_column(sa.String(60), primary_key=True)
    as_of_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)

    promoter_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    promoter_pledged_pct: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(8, 4), nullable=True
    )
    public_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    fii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    dii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    mf_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    insurance_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    banks_fi_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    retail_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    hni_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    other_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    total_shares: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)

    exchange: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    filing_url: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    raw_payload: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
