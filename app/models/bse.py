"""BSE corporate filings — announcements, corporate actions, result calendar."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeBseAnnouncements(Base):
    __tablename__ = "de_bse_announcements"
    __table_args__ = (
        sa.Index("ix_bse_ann_inst_dt", "instrument_id", sa.text("announcement_dt DESC")),
        sa.Index("ix_bse_ann_dt", sa.text("announcement_dt DESC")),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scripcode: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    announcement_dt: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=False)
    headline: Mapped[str] = mapped_column(sa.Text, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    subcategory: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    attachment_url: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    dedup_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBseCorpActions(Base):
    __tablename__ = "de_bse_corp_actions"
    __table_args__ = (
        sa.Index("ix_bse_ca_inst_ex", "instrument_id", sa.text("ex_date DESC")),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scripcode: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    action_type: Mapped[str] = mapped_column(sa.String(30), nullable=False)
    ex_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    record_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    announced_at: Mapped[Optional[datetime]] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    purpose_code: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    ratio: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    amount_per_share: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    dedup_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBseResultCalendar(Base):
    __tablename__ = "de_bse_result_calendar"
    __table_args__ = (
        sa.Index("ix_bse_rc_inst_dt", "instrument_id", "result_date"),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scripcode: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    result_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    period: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    announced_at: Mapped[Optional[datetime]] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)
    dedup_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
