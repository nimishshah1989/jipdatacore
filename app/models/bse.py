"""BSE models — filings (announcements, corp actions, result calendar) + ownership."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy.dialects.postgresql import JSONB

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


# ---------------------------------------------------------------------------
# Ownership models (GAP-18b) — shareholding, pledge, insider, SAST
# ---------------------------------------------------------------------------


class DeBseShareholding(Base):
    __tablename__ = "de_bse_shareholding"
    __table_args__ = (
        sa.UniqueConstraint("instrument_id", "quarter_end", name="uq_bse_sh_inst_qtr"),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scripcode: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    quarter_end: Mapped[date] = mapped_column(sa.Date, nullable=False)
    promoter_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    promoter_pledged_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    public_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    fii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    dii_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    insurance_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    mutual_funds_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    retail_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    body_corporate_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    total_shareholders: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    raw_json: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBsePledgeHistory(Base):
    __tablename__ = "de_bse_pledge_history"
    __table_args__ = (
        sa.UniqueConstraint("instrument_id", "as_of_date", name="uq_bse_pledge_inst_dt"),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    as_of_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    promoter_holding_qty: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    promoter_pledged_qty: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    pledged_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    total_shares: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBseInsiderTrades(Base):
    __tablename__ = "de_bse_insider_trades"
    __table_args__ = (
        sa.Index("ix_bse_insider_inst_dt", "instrument_id", sa.text("transaction_date DESC")),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    filer_name: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    filer_category: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    transaction_type: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    qty: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    value_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    transaction_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    acquisition_mode: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    intimation_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    dedup_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBseSastDisclosures(Base):
    __tablename__ = "de_bse_sast_disclosures"
    __table_args__ = (
        sa.Index("ix_bse_sast_inst_dt", "instrument_id", sa.text("disclosure_date DESC")),
    )

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    acquirer_name: Mapped[Optional[str]] = mapped_column(sa.String(300), nullable=True)
    acquirer_type: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    pre_holding_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    post_holding_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    delta_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    transaction_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    disclosure_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    regulation: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    dedup_hash: Mapped[str] = mapped_column(sa.String(64), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
