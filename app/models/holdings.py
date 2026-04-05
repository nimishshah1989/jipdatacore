"""MF holdings — portfolio holdings disclosure per fund per month."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
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
