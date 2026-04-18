"""Institutional and MF category flow tables."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import Numeric
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DeInstitutionalFlows(Base):
    """Daily FII/DII institutional flow data."""

    __tablename__ = "de_institutional_flows"
    __table_args__ = (
        sa.CheckConstraint(
            "category IN ('FII','DII','MF','Insurance','Banks','Corporates','Retail','Other')",
            name="chk_inst_flow_category",
        ),
        sa.CheckConstraint(
            "market_type IN ('equity','debt','hybrid','derivatives')",
            name="chk_inst_flow_market_type",
        ),
        sa.CheckConstraint("gross_buy >= 0", name="chk_inst_flow_gross_buy_positive"),
        sa.CheckConstraint("gross_sell >= 0", name="chk_inst_flow_gross_sell_positive"),
    )

    date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    category: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    market_type: Mapped[str] = mapped_column(sa.String(20), primary_key=True)
    gross_buy: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    gross_sell: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    # net_flow is GENERATED ALWAYS AS (gross_buy - gross_sell) STORED in the DB migration
    # Declared here as server-computed for ORM reads
    net_flow: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(18, 4),
        sa.Computed("gross_buy - gross_sell", persisted=True),
        nullable=True,
    )
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


class DeParticipantOi(Base):
    """Daily NSE participant-wise open interest (FII/DII/Pro/Client/TOTAL).

    Contract counts for futures (index/stock) and options (index/stock, call/put),
    split by long and short side, plus aggregate long/short totals.
    """

    __tablename__ = "de_participant_oi"
    __table_args__ = (
        sa.CheckConstraint(
            "client_type IN ('Client','DII','FII','Pro','TOTAL')",
            name="chk_participant_oi_client_type",
        ),
    )

    trade_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    client_type: Mapped[str] = mapped_column(sa.String(20), primary_key=True)

    future_index_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    future_index_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    future_stock_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    future_stock_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_index_call_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_index_put_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_index_call_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_index_put_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_stock_call_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_stock_put_long: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_stock_call_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    option_stock_put_short: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    total_long_contracts: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)
    total_short_contracts: Mapped[Optional[int]] = mapped_column(sa.BigInteger, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )


class DeMfCategoryFlows(Base):
    """Monthly MF category-level AUM, flows and SIP data."""

    __tablename__ = "de_mf_category_flows"

    month_date: Mapped[date] = mapped_column(sa.Date, primary_key=True)
    category: Mapped[str] = mapped_column(sa.String(200), primary_key=True)
    net_flow_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    gross_inflow_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    gross_outflow_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    aum_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sip_flow_cr: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 4), nullable=True)
    sip_accounts: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    folios: Mapped[Optional[int]] = mapped_column(sa.Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        sa.TIMESTAMP(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
        nullable=False,
    )
