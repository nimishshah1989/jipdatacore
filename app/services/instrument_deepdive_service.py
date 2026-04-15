"""Service layer for the instrument deepdive endpoint.

Queries 6 existing tables sequentially on a single async session.
"""

import uuid
from datetime import timedelta, timezone, datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.computed import DeRsDailySummary
from app.models.fundamentals import DeEquityFundamentals
from app.models.indicators_v2 import DeEquityTechnicalDailyV2
from app.models.instruments import DeInstrument
from app.models.prices import DeEquityOhlcv
from app.models.qualitative import DeQualDocuments, DeQualExtracts, DeQualSources
from app.schemas.instrument_deepdive import (
    DeepdiveMeta,
    Fundamentals,
    InstrumentDeepdiveResponse,
    InstrumentInfo,
    NewsItem,
    PriceInfo,
    RelativeStrength,
    RiskMetrics,
    SectorPeer,
    Technicals,
)

IST = timezone(timedelta(hours=5, minutes=30))


def _f(val: Any) -> Any:
    if val is None:
        return None
    return float(val)


async def _fetch_instrument(
    instrument_id: uuid.UUID, db: AsyncSession
) -> Optional[DeInstrument]:
    result = await db.execute(
        sa.select(DeInstrument).where(DeInstrument.id == instrument_id)
    )
    return result.scalar_one_or_none()


async def _fetch_fundamentals(
    instrument_id: uuid.UUID, db: AsyncSession
) -> Optional[Fundamentals]:
    result = await db.execute(
        sa.select(DeEquityFundamentals)
        .where(DeEquityFundamentals.instrument_id == instrument_id)
        .order_by(DeEquityFundamentals.as_of_date.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return Fundamentals(
        as_of_date=row.as_of_date,
        market_cap_cr=_f(row.market_cap_cr),
        pe_ratio=_f(row.pe_ratio),
        pb_ratio=_f(row.pb_ratio),
        peg_ratio=_f(row.peg_ratio),
        ev_ebitda=_f(row.ev_ebitda),
        roe_pct=_f(row.roe_pct),
        roce_pct=_f(row.roce_pct),
        operating_margin_pct=_f(row.operating_margin_pct),
        net_margin_pct=_f(row.net_margin_pct),
        debt_to_equity=_f(row.debt_to_equity),
        interest_coverage=_f(row.interest_coverage),
        eps_ttm=_f(row.eps_ttm),
        book_value=_f(row.book_value),
        dividend_per_share=_f(row.dividend_per_share),
        dividend_yield_pct=_f(row.dividend_yield_pct),
        promoter_holding_pct=_f(row.promoter_holding_pct),
        pledged_pct=_f(row.pledged_pct),
        fii_holding_pct=_f(row.fii_holding_pct),
        dii_holding_pct=_f(row.dii_holding_pct),
        revenue_growth_yoy_pct=_f(row.revenue_growth_yoy_pct),
        profit_growth_yoy_pct=_f(row.profit_growth_yoy_pct),
        high_52w=_f(row.high_52w),
        low_52w=_f(row.low_52w),
    )


async def _fetch_price(
    instrument_id: uuid.UUID, db: AsyncSession
) -> Optional[PriceInfo]:
    result = await db.execute(
        sa.select(DeEquityOhlcv.date, DeEquityOhlcv.close)
        .where(
            DeEquityOhlcv.instrument_id == instrument_id,
            DeEquityOhlcv.data_status == "validated",
        )
        .order_by(DeEquityOhlcv.date.desc())
        .limit(253)
    )
    rows = result.all()
    if not rows:
        return None

    last_date = rows[0].date
    last_close = rows[0].close

    def _pct(offset_days: int) -> Optional[float]:
        target = last_date - timedelta(days=offset_days)
        for r in rows:
            if r.date <= target:
                if r.close is None or r.close == 0:
                    return None
                return round(float((last_close - r.close) / r.close * 100), 2)
        return None

    change_1d = None
    if len(rows) > 1 and rows[1].close and rows[1].close != 0:
        change_1d = round(float((last_close - rows[1].close) / rows[1].close * 100), 2)

    return PriceInfo(
        last_close=_f(last_close),
        last_date=last_date,
        change_1d_pct=change_1d,
        change_1w_pct=_pct(7),
        change_1m_pct=_pct(30),
        change_3m_pct=_pct(90),
        change_1y_pct=_pct(365),
    )


async def _fetch_technicals_and_risk(
    instrument_id: uuid.UUID, db: AsyncSession
) -> tuple[Optional[Technicals], Optional[RiskMetrics]]:
    result = await db.execute(
        sa.select(DeEquityTechnicalDailyV2)
        .where(DeEquityTechnicalDailyV2.instrument_id == instrument_id)
        .order_by(DeEquityTechnicalDailyV2.date.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None, None

    technicals = Technicals(
        as_of_date=row.date,
        sma_20=_f(row.sma_20),
        sma_50=_f(row.sma_50),
        sma_200=_f(row.sma_200),
        ema_20=_f(row.ema_20),
        ema_50=_f(row.ema_50),
        rsi_14=_f(row.rsi_14),
        macd=_f(row.macd_line),
        macd_signal=_f(row.macd_signal),
        bollinger_upper=_f(row.bollinger_upper),
        bollinger_lower=_f(row.bollinger_lower),
        atr_14=_f(row.atr_14),
        adx_14=_f(row.adx_14),
        above_50dma=row.above_50dma,
        above_200dma=row.above_200dma,
    )
    risk = RiskMetrics(
        sharpe_1y=_f(row.sharpe_1y),
        sharpe_3y=_f(row.sharpe_3y),
        sharpe_5y=_f(row.sharpe_5y),
        sortino_1y=_f(row.sortino_1y),
        max_drawdown_1y=_f(row.max_drawdown_1y),
        beta_3y=_f(row.beta_3y),
        treynor_3y=_f(row.treynor_3y),
        downside_risk_3y=_f(row.downside_risk_3y),
    )
    return technicals, risk


async def _fetch_relative_strength(
    instrument_id: uuid.UUID, db: AsyncSession
) -> Optional[RelativeStrength]:
    max_date_result = await db.execute(
        sa.select(sa.func.max(DeRsDailySummary.date)).where(
            DeRsDailySummary.instrument_id == instrument_id
        )
    )
    max_date = max_date_result.scalar_one_or_none()
    if max_date is None:
        return None

    result = await db.execute(
        sa.select(DeRsDailySummary).where(
            DeRsDailySummary.instrument_id == instrument_id,
            DeRsDailySummary.date == max_date,
        )
    )
    rows = result.scalars().all()
    if not rows:
        return None

    rs_vs_nifty = None
    rs_vs_sector = None
    for r in rows:
        if r.vs_benchmark == "NIFTY_50":
            rs_vs_nifty = _f(r.rs_composite)
        elif r.vs_benchmark == "SECTOR":
            rs_vs_sector = _f(r.rs_composite)

    rank_result = await db.execute(
        sa.select(sa.func.count())
        .select_from(DeRsDailySummary)
        .where(
            DeRsDailySummary.date == max_date,
            DeRsDailySummary.vs_benchmark == "NIFTY_50",
            DeRsDailySummary.rs_composite > (rs_vs_nifty or 0),
        )
    )
    rank = rank_result.scalar_one() + 1

    prev_date = max_date - timedelta(days=30)
    prev_result = await db.execute(
        sa.select(DeRsDailySummary.rs_composite).where(
            DeRsDailySummary.instrument_id == instrument_id,
            DeRsDailySummary.vs_benchmark == "NIFTY_50",
            DeRsDailySummary.date <= prev_date,
        ).order_by(DeRsDailySummary.date.desc()).limit(1)
    )
    prev_rs = prev_result.scalar_one_or_none()
    trend = None
    if prev_rs is not None and rs_vs_nifty is not None:
        diff = rs_vs_nifty - float(prev_rs)
        if diff > 2:
            trend = "improving"
        elif diff < -2:
            trend = "declining"
        else:
            trend = "stable"

    return RelativeStrength(
        rs_vs_nifty=rs_vs_nifty,
        rs_vs_sector=rs_vs_sector,
        rs_rank_overall=rank,
        rs_trend=trend,
    )


async def _fetch_sector_peers(
    instrument_id: uuid.UUID, sector: Optional[str], db: AsyncSession
) -> list[SectorPeer]:
    if not sector:
        return []

    result = await db.execute(
        sa.select(
            DeInstrument.current_symbol,
            DeEquityFundamentals.pe_ratio,
            DeEquityFundamentals.roe_pct,
        )
        .select_from(DeEquityFundamentals)
        .join(DeInstrument, DeInstrument.id == DeEquityFundamentals.instrument_id)
        .where(
            sa.func.lower(DeInstrument.sector) == sector.lower(),
            DeInstrument.is_active.is_(True),
            DeInstrument.id != instrument_id,
        )
        .order_by(DeEquityFundamentals.market_cap_cr.desc().nulls_last())
        .distinct(DeInstrument.id)
        .limit(5)
    )
    rows = result.all()
    return [
        SectorPeer(
            symbol=r.current_symbol,
            pe=_f(r.pe_ratio),
            roe=_f(r.roe_pct),
            change_1y_pct=None,
        )
        for r in rows
    ]


async def _fetch_recent_news(
    symbol: str, db: AsyncSession
) -> list[NewsItem]:
    result = await db.execute(
        sa.select(
            DeQualDocuments.title,
            DeQualSources.source_name,
            DeQualDocuments.published_at,
            DeQualDocuments.summary,
            DeQualDocuments.source_url,
        )
        .select_from(DeQualExtracts)
        .join(DeQualDocuments, DeQualDocuments.id == DeQualExtracts.document_id)
        .join(DeQualSources, DeQualSources.id == DeQualDocuments.source_id)
        .where(sa.func.upper(DeQualExtracts.entity_ref) == symbol.upper())
        .order_by(DeQualDocuments.published_at.desc().nulls_last())
        .limit(5)
    )
    rows = result.all()
    return [
        NewsItem(
            headline=r.title,
            source=r.source_name,
            published_at=r.published_at,
            summary=r.summary,
            url=r.source_url,
        )
        for r in rows
    ]


async def get_instrument_deepdive(
    instrument_id: uuid.UUID, db: AsyncSession
) -> InstrumentDeepdiveResponse:
    instrument = await _fetch_instrument(instrument_id, db)
    if instrument is None:
        from fastapi import HTTPException, status
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="symbol not found",
        )

    symbol = instrument.current_symbol
    sector = instrument.sector

    fundamentals = await _fetch_fundamentals(instrument_id, db)
    price = await _fetch_price(instrument_id, db)
    technicals, risk = await _fetch_technicals_and_risk(instrument_id, db)
    rs = await _fetch_relative_strength(instrument_id, db)
    peers = await _fetch_sector_peers(instrument_id, sector, db)
    news = await _fetch_recent_news(symbol, db)

    sections_present = sum(
        1 for s in [fundamentals, price, technicals, risk, rs] if s is not None
    )
    sections_present += 1 if peers else 0
    sections_present += 1 if news else 0
    completeness = int(round(sections_present / 7 * 100))

    fv_result = await db.execute(
        sa.select(DeEquityFundamentals.face_value)
        .where(DeEquityFundamentals.instrument_id == instrument_id)
        .order_by(DeEquityFundamentals.as_of_date.desc())
        .limit(1)
    )
    face_value_row = fv_result.scalar_one_or_none()

    return InstrumentDeepdiveResponse(
        instrument=InstrumentInfo(
            symbol=symbol,
            isin=instrument.isin,
            name=instrument.company_name,
            sector=instrument.sector,
            industry=instrument.industry,
            instrument_id=str(instrument.id),
            listing_date=instrument.listing_date,
            face_value=_f(face_value_row),
        ),
        fundamentals=fundamentals,
        price=price,
        technicals=technicals,
        risk=risk,
        relative_strength=rs,
        sector_peers=peers,
        recent_news=news,
        meta=DeepdiveMeta(
            data_as_of=datetime.now(IST),
            completeness_pct=completeness,
        ),
    )
