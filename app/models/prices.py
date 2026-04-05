"""Price data tables — OHLCV, corporate actions, adjustments, MF NAV, indices, macro."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeEquityOhlcv(Base):
    """Equity OHLCV price data — parent partitioned table.

    Actual partitions are created via raw SQL in the Alembic migration.
    This model enables ORM-level queries against the parent.
    """

    __tablename__ = "de_equity_ohlcv"
    __table_args__ = (
        sa.CheckConstraint(
            "data_status IN ('raw','validated','quarantined')",
            name="chk_equity_ohlcv_data_status",
        ),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        primary_key=True,
        index=True,
    )
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    open_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    high_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    low_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    volume_adj: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    delivery_vol: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    delivery_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    trades: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    data_status: Mapped[str] = mapped_column(
        sa.String(20), nullable=False, server_default="raw"
    )
    source_file_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_source_files.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    pipeline_run_id: Mapped[Optional[int]] = mapped_column(
        sa.Integer,
        ForeignKey("de_pipeline_log.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
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


class DeCorporateActions(Base):
    """Corporate actions — dividends, splits, bonuses, rights, mergers."""

    __tablename__ = "de_corporate_actions"
    __table_args__ = (
        sa.CheckConstraint(
            "action_type IN ("
            "'dividend','split','bonus','rights','merger','demerger',"
            "'buyback','delisting','suspension','name_change','isin_change','other'"
            ")",
            name="chk_corp_action_type",
        ),
        sa.CheckConstraint(
            "dividend_type IN ('interim','final','special','none') OR dividend_type IS NULL",
            name="chk_corp_dividend_type",
        ),
        UniqueConstraint(
            "instrument_id", "ex_date", "action_type", "dividend_type",
            name="uq_corporate_actions",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    ex_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    action_type: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    dividend_type: Mapped[Optional[str]] = mapped_column(sa.String(10), nullable=True)
    ratio_from: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    ratio_to: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    cash_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    new_instrument_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    adj_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
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


class DeAdjustmentFactorsDaily(Base):
    """Cumulative adjustment factors per instrument per date."""

    __tablename__ = "de_adjustment_factors_daily"

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    cumulative_factor: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), nullable=False, server_default="1.0"
    )
    last_action_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_corporate_actions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
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


class DeRecomputeQueue(Base):
    """Queue for price recomputation jobs triggered by corporate actions."""

    __tablename__ = "de_recompute_queue"
    __table_args__ = (
        sa.CheckConstraint(
            "status IN ('pending','processing','complete','failed')",
            name="chk_recompute_queue_status",
        ),
        sa.CheckConstraint(
            "priority BETWEEN 1 AND 10",
            name="chk_recompute_queue_priority",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    from_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    trigger_action_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_corporate_actions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    priority: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=5)
    status: Mapped[str] = mapped_column(sa.String(20), nullable=False, default="pending")
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    enqueued_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    error_detail: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeDataAnomalies(Base):
    """Data quality anomalies detected by pipelines."""

    __tablename__ = "de_data_anomalies"
    __table_args__ = (
        sa.CheckConstraint(
            "entity_type IN ('equity','mf','index','macro','flow')",
            name="chk_data_anomaly_entity_type",
        ),
        sa.CheckConstraint(
            "anomaly_type IN ("
            "'price_spike','price_gap','zero_volume','missing_data','duplicate',"
            "'negative_value','stale_data','nav_deviation','dividend_anomaly',"
            "'split_mismatch','invalid_ratio','other'"
            ")",
            name="chk_data_anomaly_type",
        ),
        sa.CheckConstraint(
            "severity IN ('low','medium','high','critical')",
            name="chk_data_anomaly_severity",
        ),
        # Consistency: entity_type determines which reference column is required
        sa.CheckConstraint(
            """
            (entity_type = 'equity' AND instrument_id IS NOT NULL)
            OR (entity_type = 'mf' AND mstar_id IS NOT NULL)
            OR (entity_type IN ('macro','flow') AND ticker IS NOT NULL)
            OR (entity_type = 'index' AND (ticker IS NOT NULL OR instrument_id IS NOT NULL))
            """,
            name="chk_data_anomaly_entity_ref_consistency",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    pipeline_name: Mapped[str] = mapped_column(sa.String(100), nullable=False)
    business_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    entity_type: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    instrument_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    mstar_id: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    ticker: Mapped[Optional[str]] = mapped_column(sa.String(20), nullable=True)
    anomaly_type: Mapped[str] = mapped_column(sa.String(30), nullable=False)
    severity: Mapped[str] = mapped_column(sa.String(10), nullable=False)
    expected_range: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    actual_value: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    is_resolved: Mapped[bool] = mapped_column(sa.Boolean, default=False, nullable=False)
    resolved_by: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(
        sa.TIMESTAMP(timezone=True), nullable=True
    )
    resolution_note: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    detected_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
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


class DeMfNavDaily(Base):
    """MF NAV daily data — parent partitioned table.

    Actual partitions are created via raw SQL in the Alembic migration.
    """

    __tablename__ = "de_mf_nav_daily"
    __table_args__ = (
        sa.CheckConstraint(
            "data_status IN ('raw','validated','quarantined')",
            name="chk_mf_nav_data_status",
        ),
        sa.CheckConstraint("nav > 0", name="chk_mf_nav_positive"),
    )

    nav_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    mstar_id: Mapped[str] = mapped_column(
        sa.String(20),
        primary_key=True,
        index=True,
    )
    nav: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    nav_adj: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nav_change: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nav_change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_1d: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_1w: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_1m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_3m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_6m: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_1y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_3y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_5y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    return_10y: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    nav_52wk_high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nav_52wk_low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    data_status: Mapped[str] = mapped_column(
        sa.String(20), nullable=False, server_default="raw"
    )
    source_file_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_source_files.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    pipeline_run_id: Mapped[Optional[int]] = mapped_column(
        sa.Integer,
        ForeignKey("de_pipeline_log.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
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


class DeMfDividends(Base):
    """MF dividend payouts per fund per record date."""

    __tablename__ = "de_mf_dividends"
    __table_args__ = (
        sa.CheckConstraint("dividend_per_unit > 0", name="chk_mf_div_positive"),
        UniqueConstraint("mstar_id", "record_date", name="uq_mf_dividends"),
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
    record_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    dividend_per_unit: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    nav_before: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    nav_after: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    adj_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
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


class DeIndexPrices(Base):
    """Daily index OHLCV with valuation ratios."""

    __tablename__ = "de_index_prices"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    index_code: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_index_master.index_code", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    pe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    pb_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    div_yield: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeGlobalPrices(Base):
    """Daily prices for global instruments."""

    __tablename__ = "de_global_prices"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(20),
        ForeignKey("de_global_instrument_master.ticker", ondelete="CASCADE"),
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
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMacroValues(Base):
    """Daily macro indicator values."""

    __tablename__ = "de_macro_values"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    ticker: Mapped[str] = mapped_column(
        sa.String(20),
        ForeignKey("de_macro_master.ticker", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
