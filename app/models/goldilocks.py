"""Goldilocks Research — structured tables for market views, sector views, stock ideas."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeGoldilocksMarketView(Base):
    """Daily market view extracted from Trend Friend reports."""

    __tablename__ = "de_goldilocks_market_view"
    __table_args__ = (
        sa.CheckConstraint(
            "trend_direction IN ('upward','downward','sideways')",
            name="chk_gl_mv_trend_dir",
        ),
        sa.CheckConstraint(
            "trend_strength BETWEEN 1 AND 5",
            name="chk_gl_mv_trend_str",
        ),
        sa.CheckConstraint(
            "global_impact IN ('positive','negative','neutral')",
            name="chk_gl_mv_global",
        ),
    )

    report_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    nifty_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nifty_support_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nifty_support_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nifty_resistance_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nifty_resistance_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bank_nifty_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bank_nifty_support_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bank_nifty_support_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bank_nifty_resistance_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bank_nifty_resistance_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    trend_direction: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    trend_strength: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    headline: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    overall_view: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    global_impact: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeGoldilocksSectorView(Base):
    """Sector rankings from Trend Friend and Sector Trends reports."""

    __tablename__ = "de_goldilocks_sector_view"

    report_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    sector: Mapped[str] = mapped_column(sa.String(100), primary_key=True)
    trend: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    outlook: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    rank: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    top_picks: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeGoldilocksStockIdeas(Base):
    """Stock recommendations from Stock Bullet and Big Catch reports."""

    __tablename__ = "de_goldilocks_stock_ideas"
    __table_args__ = (
        sa.CheckConstraint(
            "idea_type IN ('stock_bullet','big_catch')",
            name="chk_gl_idea_type",
        ),
        sa.CheckConstraint(
            "status IN ('active','target_1_hit','target_2_hit','sl_hit','expired','closed')",
            name="chk_gl_idea_status",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    document_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_qual_documents.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    published_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    idea_type: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    entry_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    entry_zone_low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    entry_zone_high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    target_1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    target_2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    lt_target: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stop_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    timeframe: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    rationale: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    technical_params: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(sa.String(20), nullable=False, default="active")
    status_updated_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
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


class DeOscillatorWeekly(Base):
    """Weekly oscillator values — stochastic, RSI, disparity."""

    __tablename__ = "de_oscillator_weekly"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    disparity_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeOscillatorMonthly(Base):
    """Monthly oscillator values — stochastic, RSI, disparity."""

    __tablename__ = "de_oscillator_monthly"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    disparity_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeDivergenceSignals(Base):
    """Detected price-vs-oscillator divergence signals."""

    __tablename__ = "de_divergence_signals"
    __table_args__ = (
        sa.CheckConstraint(
            "timeframe IN ('daily','weekly','monthly')",
            name="chk_div_timeframe",
        ),
        sa.CheckConstraint(
            "divergence_type IN ('bullish','bearish','triple_bullish','triple_bearish')",
            name="chk_div_type",
        ),
        sa.CheckConstraint(
            "indicator IN ('rsi','stochastic','macd')",
            name="chk_div_indicator",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    timeframe: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    divergence_type: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    indicator: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    price_direction: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    indicator_direction: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    strength: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeFibLevels(Base):
    """Fibonacci retracement levels from auto-detected swings."""

    __tablename__ = "de_fib_levels"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    swing_high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    swing_low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fib_236: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fib_382: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fib_500: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fib_618: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fib_786: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeIndexPivots(Base):
    """Daily pivot points for major indices."""

    __tablename__ = "de_index_pivots"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    index_code: Mapped[str] = mapped_column(sa.String(30), primary_key=True)
    pivot: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    s1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    s2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    s3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    r1: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    r2: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    r3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeIntermarketRatios(Base):
    """Intermarket ratio values for cross-asset analysis."""

    __tablename__ = "de_intermarket_ratios"
    __table_args__ = (
        sa.CheckConstraint(
            "direction IN ('rising','falling','flat')",
            name="chk_imr_direction",
        ),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ratio_name: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    direction: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
