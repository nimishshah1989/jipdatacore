"""SQLAlchemy 2.0 models for v2 technical indicator tables.

Five tables:
  - DeEquityTechnicalDailyV2    — full ~130 cols, PK (date, instrument_id)
  - DeEtfTechnicalDailyV2       — full ~130 cols, PK (date, ticker)
  - DeGlobalTechnicalDailyV2    — full ~130 cols, PK (date, ticker)
  - DeIndexTechnicalDaily       — subset without volume cols (Fix 12), PK (date, index_code)
  - DeMfTechnicalDaily          — strict single-price subset (Fix 13), PK (nav_date, mstar_id)

Generated columns use sa.Computed() with byte-identical SQL expressions (Fix 9).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeEquityTechnicalDailyV2(Base):
    """Daily technical indicators for equities — full v2 schema (~130 columns)."""

    __tablename__ = "de_equity_technical_daily_v2"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Price snapshot
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Overlap / Trend
    sma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    dema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    wma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kama_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    zlma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    alma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Momentum
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_9: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cci_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    mfi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_63: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tsi_13_25: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    williams_r_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cmo_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    trix_15: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    ultosc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Volatility
    bb_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bb_pct_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    atr_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    natr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    true_range: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hv_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Volume
    obv: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    ad: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    adosc_3_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    cmf_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    efi_13: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    eom_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kvo: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    pvt: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)

    # Trend strength
    adx_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    plus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    minus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_up: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_down: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_osc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    supertrend_10_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    supertrend_direction: Mapped[Optional[int]] = mapped_column(sa.SmallInteger, nullable=True)
    psar: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Statistics
    zscore_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    linreg_slope_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    linreg_r2_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    linreg_angle_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    skew_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    kurt_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Risk (empyrical)
    risk_sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_calmar_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_beta_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_alpha_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_omega: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_information_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Derived booleans — GENERATED STORED (Fix 9: byte-identical expressions)
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
    above_20ema: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > ema_20", persisted=True),
        nullable=True,
    )
    price_above_vwap: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > vwap", persisted=True),
        nullable=True,
    )
    rsi_overbought: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 > 70", persisted=True),
        nullable=True,
    )
    rsi_oversold: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 < 30", persisted=True),
        nullable=True,
    )
    macd_bullish: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("macd_line > macd_signal", persisted=True),
        nullable=True,
    )
    adx_strong_trend: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("adx_14 > 25", persisted=True),
        nullable=True,
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeEtfTechnicalDailyV2(Base):
    """Daily technical indicators for ETFs — full v2 schema (~130 columns)."""

    __tablename__ = "de_etf_technical_daily_v2"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(30),
        ForeignKey("de_etf_master.ticker", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Price snapshot
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Overlap / Trend
    sma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    dema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    wma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kama_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    zlma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    alma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Momentum
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_9: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cci_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    mfi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_63: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tsi_13_25: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    williams_r_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cmo_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    trix_15: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    ultosc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Volatility
    bb_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bb_pct_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    atr_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    natr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    true_range: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hv_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Volume
    obv: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    ad: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    adosc_3_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    cmf_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    efi_13: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    eom_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kvo: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    pvt: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)

    # Trend strength
    adx_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    plus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    minus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_up: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_down: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_osc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    supertrend_10_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    supertrend_direction: Mapped[Optional[int]] = mapped_column(sa.SmallInteger, nullable=True)
    psar: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Statistics
    zscore_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    linreg_slope_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    linreg_r2_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    linreg_angle_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    skew_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    kurt_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Risk (empyrical)
    risk_sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_calmar_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_beta_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_alpha_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_omega: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_information_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Derived booleans — GENERATED STORED (Fix 9: byte-identical expressions)
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
    above_20ema: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > ema_20", persisted=True),
        nullable=True,
    )
    price_above_vwap: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > vwap", persisted=True),
        nullable=True,
    )
    rsi_overbought: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 > 70", persisted=True),
        nullable=True,
    )
    rsi_oversold: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 < 30", persisted=True),
        nullable=True,
    )
    macd_bullish: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("macd_line > macd_signal", persisted=True),
        nullable=True,
    )
    adx_strong_trend: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("adx_14 > 25", persisted=True),
        nullable=True,
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeGlobalTechnicalDailyV2(Base):
    """Daily technical indicators for global instruments — full v2 schema (~130 columns)."""

    __tablename__ = "de_global_technical_daily_v2"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(30),
        ForeignKey("de_global_instrument_master.ticker", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Price snapshot
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Overlap / Trend
    sma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    dema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    wma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kama_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    zlma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    alma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Momentum
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_9: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cci_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    mfi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_63: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tsi_13_25: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    williams_r_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cmo_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    trix_15: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    ultosc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Volatility
    bb_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bb_pct_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    atr_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    natr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    true_range: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hv_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Volume
    obv: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    ad: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    adosc_3_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    cmf_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    efi_13: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    eom_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kvo: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    pvt: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)

    # Trend strength
    adx_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    plus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    minus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_up: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_down: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_osc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    supertrend_10_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    supertrend_direction: Mapped[Optional[int]] = mapped_column(sa.SmallInteger, nullable=True)
    psar: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Statistics
    zscore_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    linreg_slope_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    linreg_r2_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    linreg_angle_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    skew_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    kurt_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Risk (empyrical)
    risk_sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_calmar_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_beta_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_alpha_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_omega: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_information_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Derived booleans — GENERATED STORED (Fix 9: byte-identical expressions)
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
    above_20ema: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > ema_20", persisted=True),
        nullable=True,
    )
    price_above_vwap: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > vwap", persisted=True),
        nullable=True,
    )
    rsi_overbought: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 > 70", persisted=True),
        nullable=True,
    )
    rsi_oversold: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 < 30", persisted=True),
        nullable=True,
    )
    macd_bullish: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("macd_line > macd_signal", persisted=True),
        nullable=True,
    )
    adx_strong_trend: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("adx_14 > 25", persisted=True),
        nullable=True,
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeIndexTechnicalDaily(Base):
    """Daily technical indicators for indices — volume columns excluded (Fix 12).

    Omitted: obv, ad, adosc_3_10, cmf_20, efi_13, eom_14, kvo, pvt, vwap,
             price_above_vwap (references vwap), mfi_14.
    INDEX_SPEC.volume_col = None; indices have no aggregate volume time-series.
    """

    __tablename__ = "de_index_technical_daily"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    index_code: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_index_master.index_code", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Price snapshot
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Overlap / Trend (vwap excluded)
    sma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    dema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    wma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kama_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    zlma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    alma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Momentum (mfi_14 excluded)
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_9: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    stochastic_k: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    stochastic_d: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cci_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_63: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tsi_13_25: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    williams_r_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    cmo_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    trix_15: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    ultosc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)

    # Volatility
    bb_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bb_pct_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    atr_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    atr_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    natr_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    true_range: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    keltner_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    donchian_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hv_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Trend strength
    adx_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    plus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    minus_di: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_up: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_down: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    aroon_osc: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    supertrend_10_3: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    supertrend_direction: Mapped[Optional[int]] = mapped_column(sa.SmallInteger, nullable=True)
    psar: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Statistics
    zscore_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    linreg_slope_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    linreg_r2_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    linreg_angle_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    skew_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    kurt_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Risk (empyrical)
    risk_sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_calmar_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_beta_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_alpha_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_omega: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_information_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Derived booleans — price_above_vwap excluded (vwap column absent)
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
    above_20ema: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > ema_20", persisted=True),
        nullable=True,
    )
    rsi_overbought: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 > 70", persisted=True),
        nullable=True,
    )
    rsi_oversold: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 < 30", persisted=True),
        nullable=True,
    )
    macd_bullish: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("macd_line > macd_signal", persisted=True),
        nullable=True,
    )
    adx_strong_trend: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("adx_14 > 25", persisted=True),
        nullable=True,
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMfTechnicalDaily(Base):
    """Daily technical indicators for mutual funds — strict single-price subset (Fix 13).

    Omitted (require OHLC/volume/high-low):
      - Volume: obv, ad, adosc_3_10, cmf_20, efi_13, eom_14, kvo, pvt, vwap, mfi_14
      - ATR family: atr_7, atr_14, atr_21, natr_14, true_range
      - Keltner: keltner_upper, keltner_middle, keltner_lower
      - Donchian: donchian_upper, donchian_middle, donchian_lower
      - psar, supertrend_10_3, supertrend_direction
      - cci_20, williams_r_14, ultosc (use high/low — degenerate for single-price)
      - aroon_up, aroon_down, aroon_osc (position of high/low in window)
      - stochastic_k, stochastic_d (use high/low)
      - adx_14, plus_di, minus_di (directional movement — requires high/low)
      - price_above_vwap, adx_strong_trend (reference excluded columns)

    close_adj stores NAV for engine uniformity.
    PK: (nav_date, mstar_id) — nav_date used instead of date for domain clarity.
    """

    __tablename__ = "de_mf_technical_daily"

    nav_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    mstar_id: Mapped[str] = mapped_column(
        sa.String(20),
        ForeignKey("de_mf_master.mstar_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    # Price snapshot (stores NAV)
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Overlap / Trend — single-price
    sma_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sma_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_50: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_100: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    ema_200: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    dema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    tema_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    wma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    hma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    kama_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    zlma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    alma_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)

    # Momentum — single-price
    rsi_7: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_9: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    rsi_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    macd_line: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_signal: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    macd_histogram: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    roc_5: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_10: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_21: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_63: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    roc_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    tsi_13_25: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    cmo_14: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    trix_15: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Volatility — single-price (BBands + HV only)
    bb_upper: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_middle: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_lower: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    bb_width: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    bb_pct_b: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    hv_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_60: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    hv_252: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Statistics
    zscore_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    linreg_slope_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    linreg_r2_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    linreg_angle_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    skew_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    kurt_20: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Risk (empyrical)
    risk_sharpe_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_sortino_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_calmar_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_max_drawdown_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_beta_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_alpha_nifty: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_omega: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    risk_information_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)

    # Derived booleans — only reference columns present in this table
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
    above_20ema: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("close_adj > ema_20", persisted=True),
        nullable=True,
    )
    rsi_overbought: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 > 70", persisted=True),
        nullable=True,
    )
    rsi_oversold: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("rsi_14 < 30", persisted=True),
        nullable=True,
    )
    macd_bullish: Mapped[Optional[bool]] = mapped_column(
        sa.Boolean,
        sa.Computed("macd_line > macd_signal", persisted=True),
        nullable=True,
    )

    # Audit
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
