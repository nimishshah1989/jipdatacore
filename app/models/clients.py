"""Client portfolio tables — PII-safe client data, portfolios, transactions."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List, Optional

import sqlalchemy as sa
from sqlalchemy import ForeignKey, Numeric, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, INET
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeClients(Base):
    """Client master — PII encrypted at rest, hash columns for lookup."""

    __tablename__ = "de_clients"

    client_id: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(sa.String(500), nullable=True)
    # Encrypted PII columns
    email_enc: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    phone_enc: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    pan_enc: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    # Short hash for existence checks (8-char HMAC prefix)
    pan_hash: Mapped[Optional[str]] = mapped_column(sa.String(8), nullable=True)
    email_hash: Mapped[Optional[str]] = mapped_column(sa.String(8), nullable=True)
    phone_hash: Mapped[Optional[str]] = mapped_column(sa.String(8), nullable=True)
    hmac_version: Mapped[int] = mapped_column(sa.Integer, default=1, nullable=False)
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


class DeClientKeys(Base):
    """Per-client data encryption keys (DEKs), versioned."""

    __tablename__ = "de_client_keys"

    client_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_clients.client_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    key_version: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    encrypted_dek: Mapped[str] = mapped_column(sa.Text, nullable=False)
    kms_key_id: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
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


class DePiiAccessLog(Base):
    """Audit log for every PII field access."""

    __tablename__ = "de_pii_access_log"

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    accessed_by: Mapped[Optional[str]] = mapped_column(sa.String(100), nullable=True)
    client_id: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    fields_accessed: Mapped[Optional[List[str]]] = mapped_column(ARRAY(sa.Text), nullable=True)
    purpose: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    source_ip: Mapped[Optional[Any]] = mapped_column(INET, nullable=True)
    accessed_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )


class DePortfolios(Base):
    """Portfolio master per client."""

    __tablename__ = "de_portfolios"

    portfolio_id: Mapped[str] = mapped_column(sa.String(50), primary_key=True)
    client_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_clients.client_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    portfolio_name: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    inception_date: Mapped[Optional[date]] = mapped_column(sa.Date, nullable=True)
    strategy: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
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


class DePortfolioNav(Base):
    """Daily portfolio NAV and AUM."""

    __tablename__ = "de_portfolio_nav"
    __table_args__ = (
        sa.CheckConstraint("nav > 0", name="chk_portfolio_nav_positive"),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    nav: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False)
    aum_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    units: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DePortfolioTransactions(Base):
    """Portfolio transactions — buy, sell, dividend, etc."""

    __tablename__ = "de_portfolio_transactions"
    __table_args__ = (
        sa.CheckConstraint(
            "transaction_type IN ("
            "'buy','sell','dividend','interest','fee',"
            "'transfer_in','transfer_out','split','bonus'"
            ")",
            name="chk_portfolio_txn_type",
        ),
        UniqueConstraint(
            "portfolio_id", "trade_date", "instrument_id", "transaction_type", "source_ref",
            name="uq_portfolio_transactions",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    portfolio_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    trade_date: Mapped[date] = mapped_column(sa.Date, nullable=False)
    instrument_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    transaction_type: Mapped[str] = mapped_column(sa.String(20), nullable=False)
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    source_ref: Mapped[Optional[str]] = mapped_column(sa.String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DePortfolioHoldings(Base):
    """Daily portfolio holdings snapshot."""

    __tablename__ = "de_portfolio_holdings"
    __table_args__ = (
        sa.CheckConstraint(
            "weight_pct BETWEEN 0 AND 100",
            name="chk_portfolio_holdings_weight",
        ),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        sa.UUID(as_uuid=True),
        ForeignKey("de_instrument.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    symbol: Mapped[Optional[str]] = mapped_column(sa.String(50), nullable=True)
    quantity: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    avg_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    current_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    weight_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DePortfolioRiskMetrics(Base):
    """Daily portfolio risk metrics — Sharpe, Alpha, Beta, drawdown."""

    __tablename__ = "de_portfolio_risk_metrics"

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(
        sa.String(50),
        ForeignKey("de_portfolios.portfolio_id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    cagr: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    volatility: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    sharpe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    max_drawdown: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    alpha: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    beta: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
