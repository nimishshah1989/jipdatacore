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
