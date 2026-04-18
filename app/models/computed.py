"""Derived/computed tables — technicals, RS scores, market regime, breadth, F&O."""

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


class DeEquityTechnicalDaily(Base):
    """Daily technical indicator values for equities."""

    __tablename__ = "de_equity_technical_daily"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    disparity_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    disparity_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bollinger_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    # GENERATED columns — server-computed; declared via Computed() for ORM reads
    above_50dma: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > sma_50", persisted=True),
        nullable=True,
    )
    above_200dma: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > sma_200", persisted=True),
        nullable=True,
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


class DeRsScores(Base):
    """Relative strength scores for equities and MFs vs benchmark."""

    __tablename__ = "de_rs_scores"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    entity_type: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    entity_id: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    vs_benchmark: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    rs_1w: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_1m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_3m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_6m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_12m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_composite: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    computation_version: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeRsDailySummary(Base):
    """Denormalised RS daily summary for fast dashboard queries."""

    __tablename__ = "de_rs_daily_summary"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    vs_benchmark: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    rs_composite: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_1m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    rs_3m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMarketRegime(Base):
    """Market regime classification with scoring breakdown."""

    __tablename__ = "de_market_regime"
    __table_args__ = (
        sa.CheckConstraint(
            "regime IN ('BULL','BEAR','SIDEWAYS','RECOVERY')",
            name="chk_market_regime_type",
        ),
        sa.CheckConstraint(
            "confidence BETWEEN 0 AND 100",
            name="chk_market_regime_confidence",
        ),
        sa.CheckConstraint(
            "breadth_score BETWEEN 0 AND 100",
            name="chk_market_regime_breadth",
        ),
        sa.CheckConstraint(
            "momentum_score BETWEEN 0 AND 100",
            name="chk_market_regime_momentum",
        ),
        sa.CheckConstraint(
            "volume_score BETWEEN 0 AND 100",
            name="chk_market_regime_volume",
        ),
        sa.CheckConstraint(
            "global_score BETWEEN 0 AND 100",
            name="chk_market_regime_global",
        ),
        sa.CheckConstraint(
            "fii_score BETWEEN 0 AND 100",
            name="chk_market_regime_fii",
        ),
    )

    computed_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), primary_key=True
    )
    date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    regime: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    breadth_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    momentum_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    volume_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    global_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    fii_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    indicator_detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    computation_version: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeBreadthDaily(Base):
    """Market breadth statistics — advances/declines, new highs/lows."""

    __tablename__ = "de_breadth_daily"
    __table_args__ = (
        sa.CheckConstraint(
            "pct_above_200dma BETWEEN 0 AND 100",
            name="chk_breadth_pct_200dma",
        ),
        sa.CheckConstraint(
            "pct_above_50dma BETWEEN 0 AND 100",
            name="chk_breadth_pct_50dma",
        ),
        sa.CheckConstraint("advance >= 0", name="chk_breadth_advance"),
        sa.CheckConstraint("decline >= 0", name="chk_breadth_decline"),
        sa.CheckConstraint("unchanged >= 0", name="chk_breadth_unchanged"),
        sa.CheckConstraint("total_stocks >= 0", name="chk_breadth_total"),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    advance: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    decline: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    unchanged: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    total_stocks: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    ad_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    pct_above_200dma: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    pct_above_50dma: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    new_52w_highs: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    new_52w_lows: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeSectorBreadthDaily(Base):
    """Per-sector breadth rollups — % above DMA, RSI, MACD splits."""

    __tablename__ = "de_sector_breadth_daily"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    sector: Mapped[str] = mapped_column(sa.String(100), primary_key=True)
    stocks_total: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    stocks_above_50dma: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    stocks_above_200dma: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    stocks_above_20ema: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    pct_above_50dma: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    pct_above_200dma: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    pct_above_20ema: Mapped[Decimal] = mapped_column(Numeric(6, 2), nullable=False)
    stocks_rsi_overbought: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    stocks_rsi_oversold: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    stocks_macd_bullish: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    breadth_regime: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeFoBanList(Base):
    """Daily NSE F&O Securities-in-Ban list (market-wide position limit breach).

    Populated from NSE's published fo_secban.csv (primary) or the
    equity-stockIndices API fallback. A given business_date may have zero rows
    when no security is in ban; this is a valid, successful pipeline outcome.
    """

    __tablename__ = "de_fo_ban_list"

    business_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    symbol: Mapped[str] = mapped_column(sa.String(60), primary_key=True)
    ban_count: Mapped[Optional[int]] = mapped_column(
        sa.SmallInteger, nullable=True, default=0
    )
    source: Mapped[str] = mapped_column(
        sa.String(50), nullable=False, default="NSE"
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeFoSummary(Base):
    """Daily F&O market summary — PCR, OI, FII positions."""

    __tablename__ = "de_fo_summary"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    pcr_oi: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    pcr_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    total_oi: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    oi_change: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    fii_index_long: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fii_index_short: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fii_net_futures: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    fii_net_options: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    max_pain: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeRbiFxRate(Base):
    """RBI/FBIL daily INR reference exchange rates.

    Natural key: (rate_date, currency_pair).
    currency_pair examples: 'USD/INR', 'EUR/INR', 'GBP/INR', 'JPY/INR'.
    reference_rate stored with 4 decimal precision (e.g. 83.2145).
    """

    __tablename__ = "de_rbi_fx_rate"

    rate_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    currency_pair: Mapped[str] = mapped_column(sa.String(10), primary_key=True)
    reference_rate: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    source: Mapped[str] = mapped_column(
        sa.String(50), nullable=False, server_default=sa.text("'FBIL'")
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeFoBhavcopy(Base):
    """NSE F&O UDiFF Bhavcopy — daily futures and options contract data.

    Natural key: (trade_date, symbol, instrument_type, expiry_date, strike_price, option_type).
    For futures, strike_price=0 and option_type='--'.
    """

    __tablename__ = "de_fo_bhavcopy"
    __table_args__ = (
        sa.Index("ix_de_fo_bhavcopy_trade_date", "trade_date"),
        sa.Index("ix_de_fo_bhavcopy_symbol_expiry", "symbol", "expiry_date"),
    )

    trade_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    symbol: Mapped[str] = mapped_column(sa.String(60), primary_key=True)
    instrument_type: Mapped[str] = mapped_column(sa.String(10), primary_key=True)
    expiry_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    strike_price: Mapped[Decimal] = mapped_column(
        Numeric(18, 4), primary_key=True, nullable=False, server_default=sa.text("0")
    )
    option_type: Mapped[str] = mapped_column(
        sa.String(2), primary_key=True, nullable=False, server_default=sa.text("'--'")
    )

    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    settle_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    prev_close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    underlying_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    open_interest: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    change_in_oi: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    contracts_traded: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    turnover_lakh: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4), nullable=True)
    num_trades: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    source: Mapped[str] = mapped_column(
        sa.String(50), nullable=False, server_default=sa.text("'NSE'")
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


class DeGsecYield(Base):
    """Daily G-Sec (Government Securities) benchmark yield curve.

    Primary source: CCIL NDS-OM end-of-day snapshot.
    Fallback: RBI DBIE weekly series (best-effort; may be skipped).
    Natural key: (yield_date, tenor) where tenor is one of the standard
    benchmark buckets: '1Y','2Y','3Y','5Y','7Y','10Y','15Y','30Y','40Y'.
    """

    __tablename__ = "de_gsec_yield"

    yield_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    tenor: Mapped[str] = mapped_column(sa.String(10), primary_key=True)
    yield_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    security_name: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    source: Mapped[str] = mapped_column(
        sa.String(50), nullable=False, server_default=sa.text("'CCIL'")
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DeRbiPolicyRate(Base):
    """RBI policy rates — repo, reverse repo, MSF, bank rate, CRR, SLR.

    Low-frequency data: changes only at MPC meetings (~8 times/year).
    Natural key: (effective_date, rate_type). ON CONFLICT DO NOTHING preserves
    the first observation of a given rate_type on a given effective_date.
    """

    __tablename__ = "de_rbi_policy_rate"
    __table_args__ = (
        sa.CheckConstraint(
            "rate_type IN ('REPO','REVERSE_REPO','MSF','BANK_RATE','CRR','SLR')",
            name="chk_rbi_policy_rate_type",
        ),
    )

    effective_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    rate_type: Mapped[str] = mapped_column(sa.String(30), primary_key=True)
    rate_pct: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    source: Mapped[str] = mapped_column(
        sa.String(50), nullable=False, server_default=sa.text("'RBI'")
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
