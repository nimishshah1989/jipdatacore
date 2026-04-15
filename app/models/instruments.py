"""Instrument master tables — equities, MFs, indices, macro, global instruments."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeInstrument(Base):
    """Equity/security master record."""

    __tablename__ = "de_instrument"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    current_symbol: Mapped[str] = mapped_column(sa.String(50), unique=True, nullable=False)
    isin: Mapped[Optional[str]] = mapped_column(sa.String(12), nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    series: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    industry: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    nifty_50: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    nifty_200: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    nifty_500: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    listing_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    bse_symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    is_suspended: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    suspended_from: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    delisted_on: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    is_tradeable: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMarketCapHistory(Base):
    """Market cap category history per instrument."""

    __tablename__ = "de_market_cap_history"
    __table_args__ = (
        sa.CheckConstraint(
            "cap_category IN ('large','mid','small','micro')",
            name="chk_market_cap_category",
        ),
    )

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    effective_from: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    cap_category: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    effective_to: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeSymbolHistory(Base):
    """Historical symbol changes for instruments."""

    __tablename__ = "de_symbol_history"

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    effective_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    old_symbol: Mapped[str] = mapped_column(sa.String(50), nullable=False)
    new_symbol: Mapped[str] = mapped_column(sa.String(50), nullable=False)
    reason: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeIndexMaster(Base):
    """Index master — NSE/BSE indices."""

    __tablename__ = "de_index_master"
    __table_args__ = (
        sa.CheckConstraint(
            "category IN ('broad','sectoral','thematic','strategy')",
            name="chk_index_category",
        ),
    )

    index_code: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    index_name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    category: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeSectorMapping(Base):
    """Maps JIP internal sector names to NSE sectoral/thematic index codes."""

    __tablename__ = "de_sector_mapping"

    jip_sector_name: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    primary_nse_index: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_index_master.index_code"),
        nullable=False,
    )
    secondary_nse_indices: Mapped[Optional[list[str]]] = mapped_column(
        sa.ARRAY(sa.Text), nullable=True
    )
    notes: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeIndexConstituents(Base):
    """Index constituents with weight and validity range."""

    __tablename__ = "de_index_constituents"

    index_code: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_index_master.index_code", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    effective_from: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    weight_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    effective_to: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMfMaster(Base):
    """Mutual fund master — Morningstar-keyed."""

    __tablename__ = "de_mf_master"

    mstar_id: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    amfi_code: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    isin: Mapped[Optional[str]] = mapped_column(sa.String(12), nullable=True)
    fund_name: Mapped[str] = mapped_column(sa.String(500), nullable=False)
    amc_name: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    category_name: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    broad_category: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    is_index_fund: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    is_etf: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    inception_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    closure_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    merged_into_mstar_id: Mapped[Optional[str]] = mapped_column(
        sa.String(20),
        ForeignKey("de_mf_master.mstar_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    primary_benchmark: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    expense_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    investment_strategy: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    # Morningstar purchase mode: 1=Regular, 2=Direct. Added by migration 007.
    # Drives MF technical eligibility filter in chunk 10 (only purchase_mode=1 funds).
    purchase_mode: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMfLifecycle(Base):
    """Lifecycle events for mutual funds."""

    __tablename__ = "de_mf_lifecycle"
    __table_args__ = (
        sa.CheckConstraint(
            "event_type IN ("
            "'launch','merge','name_change','category_change',"
            "'amc_change','closure','benchmark_change','reopen'"
            ")",
            name="chk_mf_lifecycle_event_type",
        ),
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
    event_type: Mapped[str] = mapped_column(sa.String(30), nullable=False)
    event_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    old_value: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMacroMaster(Base):
    """Macro indicator master."""

    __tablename__ = "de_macro_master"
    __table_args__ = (
        sa.CheckConstraint(
            "source IN ('FRED','RBI','MOSPI','NSO','SEBI','BSE','NSE','manual')",
            name="chk_macro_source",
        ),
        sa.CheckConstraint(
            "frequency IN ('daily','weekly','monthly','quarterly','annual')",
            name="chk_macro_frequency",
        ),
    )

    ticker: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    source: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    unit: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    frequency: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeGlobalInstrumentMaster(Base):
    """Global instruments — indices, ETFs, bonds, commodities, FX, and crypto."""

    __tablename__ = "de_global_instrument_master"
    __table_args__ = (
        sa.CheckConstraint(
            "instrument_type IN ('index','etf','bond','commodity','forex','crypto')",
            name="chk_global_instrument_type",
        ),
    )

    ticker: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    instrument_type: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    exchange: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(sa.String(5), nullable=True)
    country: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    source: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeContributors(Base):
    """System contributors / analysts."""

    __tablename__ = "de_contributors"
    __table_args__ = (
        sa.CheckConstraint(
            "role IN ('admin','analyst','pipeline','viewer','external')",
            name="chk_contributor_role",
        ),
        UniqueConstraint("name", name="uq_contributors_name"),
    )

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    role: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    is_admin: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeTradingCalendar(Base):
    """Exchange trading calendar."""

    __tablename__ = "de_trading_calendar"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    is_trading: Mapped[bool] = mapped_column(sa.Boolean, nullable=False)
    exchange: Mapped[str] = mapped_column(sa.String(10), nullable=False, default="NSE")
    notes: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
