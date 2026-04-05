"""Sector-level derived metrics — market-cap-weighted RS, sector breadth.

Formulas:
  Sector RS = market-cap-weighted average of constituent RS composites
  Sector breadth = % of constituents above 50DMA / 200DMA within a sector
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)

# Benchmark used for sector RS (primary)
PRIMARY_BENCHMARK = "NIFTY 50"

# Minimum constituents required to compute sector metrics
MIN_CONSTITUENTS = 3


def compute_weighted_sector_rs(
    constituent_rs: list[float],
    market_caps: list[float],
) -> Optional[Decimal]:
    """Compute market-cap-weighted sector RS.

    Formula:
        sector_rs = sum(rs_i * mcap_i) / sum(mcap_i)

    Args:
        constituent_rs: RS composite scores for each constituent.
        market_caps: Market-cap weight for each constituent (same order).

    Returns:
        Weighted average RS as Decimal, or None if no valid pairs.
    """
    if len(constituent_rs) != len(market_caps):
        return None

    weighted_sum = 0.0
    total_weight = 0.0

    for rs_val, mcap in zip(constituent_rs, market_caps):
        if mcap <= 0.0:
            continue
        weighted_sum += rs_val * mcap
        total_weight += mcap

    if total_weight == 0.0:
        return None

    result = weighted_sum / total_weight
    return Decimal(str(round(result, 4)))


def compute_sector_breadth(
    above_50dma: list[bool],
    above_200dma: list[bool],
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """Compute sector breadth — % of constituents above 50DMA and 200DMA.

    Args:
        above_50dma: Boolean per constituent indicating whether close > SMA50.
        above_200dma: Boolean per constituent indicating whether close > SMA200.

    Returns:
        Tuple of (pct_above_50dma, pct_above_200dma) as Decimal percentages.
        Returns (None, None) if lists are empty.
    """
    n = len(above_50dma)
    if n == 0 or len(above_200dma) != n:
        return None, None

    count_50 = sum(1 for v in above_50dma if v)
    count_200 = sum(1 for v in above_200dma if v)

    pct_50 = Decimal(str(round(count_50 / n * 100.0, 4)))
    pct_200 = Decimal(str(round(count_200 / n * 100.0, 4)))

    return pct_50, pct_200


async def compute_sector_metrics(
    session: AsyncSession,
    business_date: date,
    benchmark: str = PRIMARY_BENCHMARK,
) -> dict[str, dict]:
    """Compute sector RS and breadth for all sectors on a given date.

    Queries:
      - de_rs_scores for constituent RS composites
      - de_equity_technical_daily for above_50dma / above_200dma flags
      - de_market_cap_history for market-cap weights
      - de_instrument for sector labels

    Args:
        session: Async DB session.
        business_date: Date for which to compute sector metrics.
        benchmark: Benchmark to use for RS scores.

    Returns:
        Dict mapping sector name → dict with keys:
          sector_rs, pct_above_50dma, pct_above_200dma, constituent_count
    """
    logger.info(
        "sector_metrics_compute_start",
        business_date=business_date.isoformat(),
        benchmark=benchmark,
    )

    # Fetch all constituents with RS + technical flags + market cap
    query = sa.text("""
        SELECT
            i.sector,
            CAST(rs.rs_composite AS FLOAT) AS rs_composite,
            CAST(mch.market_cap_crore AS FLOAT) AS market_cap,
            COALESCE(etd.above_50dma, FALSE) AS above_50dma,
            COALESCE(etd.above_200dma, FALSE) AS above_200dma
        FROM de_rs_scores rs
        JOIN de_instrument i
            ON i.id::text = rs.entity_id
        LEFT JOIN de_equity_technical_daily etd
            ON etd.instrument_id::text = rs.entity_id
            AND etd.date = :bdate
        LEFT JOIN (
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                market_cap_crore
            FROM de_market_cap_history
            WHERE as_of_date <= :bdate
            ORDER BY instrument_id, as_of_date DESC
        ) mch ON mch.instrument_id::text = rs.entity_id
        WHERE rs.date = :bdate
          AND rs.vs_benchmark = :benchmark
          AND rs.entity_type = 'equity'
          AND i.sector IS NOT NULL
          AND rs.rs_composite IS NOT NULL
    """)

    rows = (
        await session.execute(
            query,
            {"bdate": business_date, "benchmark": benchmark},
        )
    ).fetchall()

    if not rows:
        logger.warning(
            "sector_metrics_no_data",
            business_date=business_date.isoformat(),
        )
        return {}

    # Aggregate per sector
    sector_data: dict[str, dict] = {}
    for row in rows:
        sector = row.sector
        if sector not in sector_data:
            sector_data[sector] = {
                "rs_list": [],
                "mcap_list": [],
                "above_50dma": [],
                "above_200dma": [],
            }
        sector_data[sector]["rs_list"].append(float(row.rs_composite))
        # Default to 1.0 market cap weight if missing (equal-weight fallback)
        mcap = float(row.market_cap) if row.market_cap is not None else 1.0
        sector_data[sector]["mcap_list"].append(mcap)
        sector_data[sector]["above_50dma"].append(bool(row.above_50dma))
        sector_data[sector]["above_200dma"].append(bool(row.above_200dma))

    results: dict[str, dict] = {}
    for sector, data in sector_data.items():
        n = len(data["rs_list"])
        if n < MIN_CONSTITUENTS:
            logger.debug(
                "sector_metrics_skip_insufficient_constituents",
                sector=sector,
                count=n,
                min_required=MIN_CONSTITUENTS,
            )
            continue

        sector_rs = compute_weighted_sector_rs(data["rs_list"], data["mcap_list"])
        pct_50, pct_200 = compute_sector_breadth(
            data["above_50dma"], data["above_200dma"]
        )

        results[sector] = {
            "sector_rs": sector_rs,
            "pct_above_50dma": pct_50,
            "pct_above_200dma": pct_200,
            "constituent_count": n,
        }

    logger.info(
        "sector_metrics_compute_complete",
        business_date=business_date.isoformat(),
        sectors_computed=len(results),
    )

    return results
