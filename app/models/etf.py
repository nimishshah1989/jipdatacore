"""ETF models — de_etf_master, de_etf_ohlcv, de_etf_technical_daily."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeEtfMaster(Base):
    """ETF instrument registry — US, India, UK, HK, JP ETFs."""

    __tablename__ = "de_etf_master"

    ticker: Mapped[str] = mapped_column(sa.String(30), primary_key=True)
    name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    exchange: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    country: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    currency: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    asset_class: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    category: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    benchmark: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    expense_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 4), nullable=True)
    inception_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=True, nullable=False)
    source: Mapped[str] = mapped_column(sa.String(20), default="stooq", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False
    )


class DeEtfOhlcv(Base):
    """ETF daily OHLCV prices."""

    __tablename__ = "de_etf_ohlcv"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(30),
        sa.ForeignKey("de_etf_master.ticker", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False
    )


class DeEtfTechnicalDaily(Base):
    """ETF daily technical indicators — same structure as equity technicals."""

    __tablename__ = "de_etf_technical_daily"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(30),
        sa.ForeignKey("de_etf_master.ticker", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    volatility_20d: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    volatility_60d: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    bollinger_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bollinger_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    relative_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    adx_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    above_50dma: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean, sa.Computed("close > sma_50", persisted=True), nullable=True
    )
    above_200dma: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean, sa.Computed("close > sma_200", persisted=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False
    )
