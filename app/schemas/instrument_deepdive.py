"""Pydantic v2 response models for the instrument deepdive endpoint."""

from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel


class InstrumentInfo(BaseModel):
    symbol: str
    isin: Optional[str] = None
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    instrument_id: str
    listing_date: Optional[date] = None
    face_value: Optional[Any] = None


class Fundamentals(BaseModel):
    as_of_date: Optional[date] = None
    market_cap_cr: Optional[Any] = None
    pe_ratio: Optional[Any] = None
    pb_ratio: Optional[Any] = None
    peg_ratio: Optional[Any] = None
    ev_ebitda: Optional[Any] = None
    roe_pct: Optional[Any] = None
    roce_pct: Optional[Any] = None
    operating_margin_pct: Optional[Any] = None
    net_margin_pct: Optional[Any] = None
    debt_to_equity: Optional[Any] = None
    interest_coverage: Optional[Any] = None
    eps_ttm: Optional[Any] = None
    book_value: Optional[Any] = None
    dividend_per_share: Optional[Any] = None
    dividend_yield_pct: Optional[Any] = None
    promoter_holding_pct: Optional[Any] = None
    pledged_pct: Optional[Any] = None
    fii_holding_pct: Optional[Any] = None
    dii_holding_pct: Optional[Any] = None
    revenue_growth_yoy_pct: Optional[Any] = None
    profit_growth_yoy_pct: Optional[Any] = None
    high_52w: Optional[Any] = None
    low_52w: Optional[Any] = None


class PriceInfo(BaseModel):
    last_close: Optional[Any] = None
    last_date: Optional[date] = None
    change_1d_pct: Optional[Any] = None
    change_1w_pct: Optional[Any] = None
    change_1m_pct: Optional[Any] = None
    change_3m_pct: Optional[Any] = None
    change_1y_pct: Optional[Any] = None


class Technicals(BaseModel):
    as_of_date: Optional[date] = None
    sma_20: Optional[Any] = None
    sma_50: Optional[Any] = None
    sma_200: Optional[Any] = None
    ema_20: Optional[Any] = None
    ema_50: Optional[Any] = None
    rsi_14: Optional[Any] = None
    macd: Optional[Any] = None
    macd_signal: Optional[Any] = None
    bollinger_upper: Optional[Any] = None
    bollinger_lower: Optional[Any] = None
    atr_14: Optional[Any] = None
    adx_14: Optional[Any] = None
    above_50dma: Optional[bool] = None
    above_200dma: Optional[bool] = None


class RiskMetrics(BaseModel):
    sharpe_1y: Optional[Any] = None
    sharpe_3y: Optional[Any] = None
    sharpe_5y: Optional[Any] = None
    sortino_1y: Optional[Any] = None
    max_drawdown_1y: Optional[Any] = None
    beta_3y: Optional[Any] = None
    treynor_3y: Optional[Any] = None
    downside_risk_3y: Optional[Any] = None


class RelativeStrength(BaseModel):
    rs_vs_nifty: Optional[Any] = None
    rs_vs_sector: Optional[Any] = None
    rs_rank_overall: Optional[int] = None
    rs_trend: Optional[str] = None


class SectorPeer(BaseModel):
    symbol: str
    pe: Optional[Any] = None
    roe: Optional[Any] = None
    change_1y_pct: Optional[Any] = None


class NewsItem(BaseModel):
    headline: Optional[str] = None
    source: Optional[str] = None
    published_at: Optional[datetime] = None
    summary: Optional[str] = None
    url: Optional[str] = None


class DeepdiveMeta(BaseModel):
    data_as_of: datetime
    completeness_pct: int


class InstrumentDeepdiveResponse(BaseModel):
    instrument: InstrumentInfo
    fundamentals: Optional[Fundamentals] = None
    price: Optional[PriceInfo] = None
    technicals: Optional[Technicals] = None
    risk: Optional[RiskMetrics] = None
    relative_strength: Optional[RelativeStrength] = None
    sector_peers: list[SectorPeer] = []
    recent_news: list[NewsItem] = []
    meta: DeepdiveMeta
