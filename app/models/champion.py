"""Champion trades table — high-conviction trade ideas and outcomes."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeChampionTrades(Base):
    """Champion trade ideas with entry/exit tracking."""

    __tablename__ = "de_champion_trades"
    __table_args__ = (
        sa.CheckConstraint(
            "direction IN ('long','short','neutral')",
            name="chk_champion_trade_direction",
        ),
        sa.CheckConstraint(
            "stage IN ('idea','active','partial_exit','closed','cancelled')",
            name="chk_champion_trade_stage",
        ),
        UniqueConstraint("source_ref", name="uq_champion_trades_source_ref"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    trade_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    instrument_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    direction: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    entry_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    exit_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stop_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    target_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stage: Mapped[str] = mapped_column(sa.String(20), nullable=False, default="idea")
    signal_type: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(
        sa.String(200), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
