"""Fund derived metrics — holdings-weighted RS, manager alpha, risk metrics.

Formulas:
  derived_rs_composite = sum(rs_composite_i * weight_pct_i) / sum(weight_pct_i)
  manager_alpha        = nav_rs_composite - derived_rs_composite
  coverage_pct         = sum(weight_pct for mapped holdings) / 100
  Risk metrics use NAV daily returns; RF rate = 7% annual = 7/252 daily.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.computation.technicals import (
    compute_beta,
    compute_max_drawdown,
    compute_sharpe,
    compute_sortino,
    compute_volatility,
)
from app.logging import get_logger
from app.models.mf_derived import DeMfDerivedDaily

logger = get_logger(__name__)

# Annual risk-free rate for Sharpe/Sortino (7%)
RISK_FREE_ANNUAL = 0.07
# Daily risk-free rate
RISK_FREE_DAILY = RISK_FREE_ANNUAL / 252.0

# Lookback periods in trading days
LOOKBACK_1Y = 252
LOOKBACK_3Y = 756

# Benchmark for fund beta
NIFTY_50_SYMBOL = "NIFTY 50"

# Minimum NAV observations required for risk metrics
MIN_NAV_OBSERVATIONS_1Y = 200
MIN_NAV_OBSERVATIONS_3Y = 600

# Minimum coverage to compute derived RS
MIN_COVERAGE_PCT = Decimal("20.0")

# Batch size for DB upserts
BATCH_SIZE = 500


def compute_holdings_weighted_rs(
    rs_scores: list[float],
    weights: list[float],
) -> Optional[Decimal]:
    """Compute holdings-weighted RS composite for a fund.

    Formula: derived_rs = sum(rs_i * w_i) / sum(w_i)
    where w_i is weight_pct from de_mf_holdings.

    Args:
        rs_scores: RS composite scores for each mapped holding.
        weights: weight_pct values for each holding (same order).

    Returns:
        Holdings-weighted RS as Decimal, or None if no valid pairs.
    """
    if len(rs_scores) != len(weights):
        return None

    weighted_sum = 0.0
    total_weight = 0.0

    for rs_val, w in zip(rs_scores, weights):
        if w <= 0.0:
            continue
        weighted_sum += rs_val * w
        total_weight += w

    if total_weight == 0.0:
        return None

    result = weighted_sum / total_weight
    return Decimal(str(round(result, 4)))


def compute_coverage(
    total_weight_mapped: float,
) -> Decimal:
    """Compute holdings coverage percentage.

    Formula: coverage_pct = sum(weight_pct for mapped holdings) / 100

    Args:
        total_weight_mapped: Sum of weight_pct for holdings with instrument_id resolved.

    Returns:
        Coverage as Decimal percentage (0–100 range).
    """
    # weight_pct is stored in 0–100 scale in de_mf_holdings
    coverage = total_weight_mapped / 100.0 * 100.0
    # Cap at 100
    coverage = min(coverage, 100.0)
    return Decimal(str(round(coverage, 2)))


def compute_manager_alpha(
    nav_rs_composite: Optional[Decimal],
    derived_rs_composite: Optional[Decimal],
) -> Optional[Decimal]:
    """Compute manager alpha = nav_rs_composite - derived_rs_composite.

    Args:
        nav_rs_composite: Fund NAV RS composite score.
        derived_rs_composite: Holdings-weighted stock RS composite.

    Returns:
        Manager alpha as Decimal, or None if either input is None.
    """
    if nav_rs_composite is None or derived_rs_composite is None:
        return None
    alpha = float(nav_rs_composite) - float(derived_rs_composite)
    return Decimal(str(round(alpha, 4)))


def _daily_returns(prices: list[float]) -> list[float]:
    """Compute daily returns from a price series."""
    return [prices[i] / prices[i - 1] - 1.0 for i in range(1, len(prices))]


def compute_fund_risk_metrics(
    nav_prices: list[float],
    benchmark_prices: list[float],
) -> dict[str, Optional[Decimal]]:
    """Compute all risk metrics for a fund from NAV price series.

    Computes:
      - sharpe_1y: Sharpe ratio on last 252 trading days (RF=7% annual)
      - sharpe_3y: Sharpe ratio on last 756 trading days
      - sortino_1y: Sortino ratio on last 252 trading days
      - max_drawdown_1y: Max drawdown on last 252 prices
      - max_drawdown_3y: Max drawdown on last 756 prices
      - volatility_1y: Annualised volatility on last 252 prices
      - volatility_3y: Annualised volatility on last 756 prices
      - beta_vs_nifty: Beta vs NIFTY 50 on last 252 trading days

    Args:
        nav_prices: NAV price series (chronological, oldest first).
        benchmark_prices: Benchmark (NIFTY 50) price series (same length alignment).

    Returns:
        Dict with keys for each metric → Decimal or None.
    """
    metrics: dict[str, Optional[Decimal]] = {
        "sharpe_1y": None,
        "sharpe_3y": None,
        "sortino_1y": None,
        "max_drawdown_1y": None,
        "max_drawdown_3y": None,
        "volatility_1y": None,
        "volatility_3y": None,
        "beta_vs_nifty": None,
    }

    n = len(nav_prices)
    if n < 2:
        return metrics

    # 1-year window
    window_1y = nav_prices[-LOOKBACK_1Y:] if n >= LOOKBACK_1Y else nav_prices
    if len(window_1y) >= MIN_NAV_OBSERVATIONS_1Y:
        returns_1y = _daily_returns(window_1y)
        metrics["sharpe_1y"] = compute_sharpe(
            returns_1y,
            risk_free_rate=RISK_FREE_DAILY,
            trading_days=252,
        )
        metrics["sortino_1y"] = compute_sortino(
            returns_1y,
            target_return=RISK_FREE_DAILY,
            trading_days=252,
        )
        metrics["max_drawdown_1y"] = compute_max_drawdown(window_1y)
        metrics["volatility_1y"] = compute_volatility(window_1y, trading_days=252)

    # 3-year window
    window_3y = nav_prices[-LOOKBACK_3Y:] if n >= LOOKBACK_3Y else nav_prices
    if len(window_3y) >= MIN_NAV_OBSERVATIONS_3Y:
        returns_3y = _daily_returns(window_3y)
        metrics["sharpe_3y"] = compute_sharpe(
            returns_3y,
            risk_free_rate=RISK_FREE_DAILY,
            trading_days=252,
        )
        metrics["max_drawdown_3y"] = compute_max_drawdown(window_3y)
        metrics["volatility_3y"] = compute_volatility(window_3y, trading_days=252)

    # Beta vs NIFTY 50 (1-year window, aligned)
    n_bench = len(benchmark_prices)
    if n_bench >= 2 and n >= 2:
        # Align to same length suffix
        align_n = min(n, n_bench, LOOKBACK_1Y)
        nav_window = nav_prices[-align_n:]
        bench_window = benchmark_prices[-align_n:]
        if len(nav_window) >= MIN_NAV_OBSERVATIONS_1Y and len(bench_window) >= MIN_NAV_OBSERVATIONS_1Y:
            nav_rets = _daily_returns(nav_window)
            bench_rets = _daily_returns(bench_window)
            # Align lengths after returns computation
            min_len = min(len(nav_rets), len(bench_rets))
            metrics["beta_vs_nifty"] = compute_beta(
                nav_rets[:min_len], bench_rets[:min_len]
            )

    return metrics


async def compute_fund_derived_metrics(
    session: AsyncSession,
    business_date: date,
    benchmark: str = NIFTY_50_SYMBOL,
) -> int:
    """Compute and persist derived metrics for all funds on a given date.

    Steps:
      1. Fetch all active mstar_ids with NAV data on business_date
      2. For each fund, fetch holdings + RS scores for derived_rs
      3. Fetch NAV history for risk metrics
      4. Fetch NAV RS composite from de_rs_scores
      5. Upsert to de_mf_derived_daily ON CONFLICT (nav_date, mstar_id) DO UPDATE

    Args:
        session: Async DB session.
        business_date: Date for which to compute metrics.
        benchmark: Benchmark symbol for NAV RS lookup.

    Returns:
        Number of rows upserted.
    """
    logger.info(
        "fund_derived_metrics_compute_start",
        business_date=business_date.isoformat(),
        benchmark=benchmark,
    )

    # Fetch all mstar_ids with NAV on this date
    fund_query = sa.text("""
        SELECT DISTINCT mstar_id
        FROM de_mf_nav_daily
        WHERE nav_date = :bdate
          AND data_status = 'validated'
    """)
    fund_rows = (
        await session.execute(fund_query, {"bdate": business_date})
    ).fetchall()

    if not fund_rows:
        logger.warning(
            "fund_derived_no_nav_data",
            business_date=business_date.isoformat(),
        )
        return 0

    mstar_ids = [r.mstar_id for r in fund_rows]

    # Fetch holdings with RS for derived RS computation
    # Use most recent holdings as_of_date <= business_date
    holdings_query = sa.text("""
        SELECT
            h.mstar_id,
            h.weight_pct,
            h.is_mapped,
            CAST(rs.rs_composite AS FLOAT) AS rs_composite
        FROM de_mf_holdings h
        LEFT JOIN de_rs_scores rs
            ON rs.entity_id = h.instrument_id::text
            AND rs.date = :bdate
            AND rs.vs_benchmark = :benchmark
            AND rs.entity_type = 'equity'
        WHERE h.mstar_id = ANY(:mstar_ids)
          AND h.as_of_date = (
              SELECT MAX(as_of_date)
              FROM de_mf_holdings h2
              WHERE h2.mstar_id = h.mstar_id
                AND h2.as_of_date <= :bdate
          )
    """)

    holdings_rows = (
        await session.execute(
            holdings_query,
            {
                "bdate": business_date,
                "benchmark": benchmark,
                "mstar_ids": mstar_ids,
            },
        )
    ).fetchall()

    # Group holdings by fund
    fund_holdings: dict[str, dict] = {}
    for row in holdings_rows:
        mid = row.mstar_id
        if mid not in fund_holdings:
            fund_holdings[mid] = {"rs_scores": [], "weights": [], "mapped_weight": 0.0}
        w = float(row.weight_pct) if row.weight_pct is not None else 0.0
        if row.is_mapped and row.rs_composite is not None:
            fund_holdings[mid]["rs_scores"].append(float(row.rs_composite))
            fund_holdings[mid]["weights"].append(w)
        if row.is_mapped and w > 0.0:
            fund_holdings[mid]["mapped_weight"] += w

    # Fetch NAV RS composites
    nav_rs_query = sa.text("""
        SELECT entity_id AS mstar_id, CAST(rs_composite AS FLOAT) AS rs_composite
        FROM de_rs_scores
        WHERE date = :bdate
          AND vs_benchmark = :benchmark
          AND entity_type = 'mf'
          AND entity_id = ANY(:mstar_ids)
    """)

    nav_rs_rows = (
        await session.execute(
            nav_rs_query,
            {"bdate": business_date, "benchmark": benchmark, "mstar_ids": mstar_ids},
        )
    ).fetchall()

    nav_rs_map: dict[str, Optional[float]] = {
        r.mstar_id: r.rs_composite for r in nav_rs_rows
    }

    # Fetch NAV history for risk metrics — limit to 3yr + buffer (LOOKBACK_3Y=756 trading days)
    import datetime as dt
    nav_start_date = business_date - dt.timedelta(days=1100)  # ~756 trading days with buffer

    nav_history_query = sa.text("""
        SELECT mstar_id, nav_date, CAST(nav AS FLOAT) AS nav
        FROM de_mf_nav_daily
        WHERE mstar_id = ANY(:mstar_ids)
          AND nav_date <= :bdate
          AND nav_date >= :start_date
          AND data_status = 'validated'
          AND nav IS NOT NULL
        ORDER BY mstar_id, nav_date
    """)

    nav_history_rows = (
        await session.execute(
            nav_history_query,
            {"mstar_ids": mstar_ids, "bdate": business_date, "start_date": nav_start_date},
        )
    ).fetchall()

    nav_history: dict[str, list[float]] = {}
    for row in nav_history_rows:
        mid = row.mstar_id
        if mid not in nav_history:
            nav_history[mid] = []
        nav_history[mid].append(float(row.nav))

    # Fetch NIFTY 50 benchmark price history for beta.
    # Query de_index_prices directly by index_code to avoid the UUID/VARCHAR
    # type mismatch that occurs when joining de_index_price_daily (a view where
    # instrument_id is VARCHAR) against de_instrument.id (UUID).
    bench_query = sa.text("""
        SELECT date, CAST(close AS FLOAT) AS close_adj
        FROM de_index_prices
        WHERE index_code = :benchmark
          AND date <= :bdate
          AND date >= :start_date
          AND close IS NOT NULL
        ORDER BY date
    """)

    bench_rows = (
        await session.execute(
            bench_query,
            {"benchmark": benchmark, "bdate": business_date, "start_date": nav_start_date},
        )
    ).fetchall()

    benchmark_prices_list: list[float] = [float(r.close_adj) for r in bench_rows]

    # Build upsert rows
    upsert_rows: list[dict] = []

    for mstar_id in mstar_ids:
        holdings_data = fund_holdings.get(mstar_id, {})
        rs_scores = holdings_data.get("rs_scores", [])
        weights = holdings_data.get("weights", [])
        mapped_weight = holdings_data.get("mapped_weight", 0.0)

        # Derived RS (holdings-weighted)
        derived_rs: Optional[Decimal] = None
        coverage: Optional[Decimal] = None

        if rs_scores:
            derived_rs = compute_holdings_weighted_rs(rs_scores, weights)

        coverage = compute_coverage(mapped_weight)

        # NAV RS composite
        nav_rs_raw = nav_rs_map.get(mstar_id)
        nav_rs: Optional[Decimal] = (
            Decimal(str(round(nav_rs_raw, 4))) if nav_rs_raw is not None else None
        )

        # Manager alpha
        alpha = compute_manager_alpha(nav_rs, derived_rs)

        # Skip risk metrics if coverage too low
        nav_prices = nav_history.get(mstar_id, [])
        risk_metrics = compute_fund_risk_metrics(nav_prices, benchmark_prices_list)

        upsert_rows.append(
            {
                "nav_date": business_date,
                "mstar_id": mstar_id,
                "derived_rs_composite": derived_rs,
                "nav_rs_composite": nav_rs,
                "manager_alpha": alpha,
                "coverage_pct": coverage,
                "sharpe_1y": risk_metrics["sharpe_1y"],
                "sharpe_3y": risk_metrics["sharpe_3y"],
                "sortino_1y": risk_metrics["sortino_1y"],
                "max_drawdown_1y": risk_metrics["max_drawdown_1y"],
                "max_drawdown_3y": risk_metrics["max_drawdown_3y"],
                "volatility_1y": risk_metrics["volatility_1y"],
                "volatility_3y": risk_metrics["volatility_3y"],
                "beta_vs_nifty": risk_metrics["beta_vs_nifty"],
            }
        )

    if not upsert_rows:
        return 0

    # Batch upsert
    total_upserted = 0

    for offset in range(0, len(upsert_rows), BATCH_SIZE):
        batch = upsert_rows[offset : offset + BATCH_SIZE]
        stmt = pg_insert(DeMfDerivedDaily).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["nav_date", "mstar_id"],
            set_={
                "derived_rs_composite": stmt.excluded.derived_rs_composite,
                "nav_rs_composite": stmt.excluded.nav_rs_composite,
                "manager_alpha": stmt.excluded.manager_alpha,
                "coverage_pct": stmt.excluded.coverage_pct,
                "sharpe_1y": stmt.excluded.sharpe_1y,
                "sharpe_3y": stmt.excluded.sharpe_3y,
                "sortino_1y": stmt.excluded.sortino_1y,
                "max_drawdown_1y": stmt.excluded.max_drawdown_1y,
                "max_drawdown_3y": stmt.excluded.max_drawdown_3y,
                "volatility_1y": stmt.excluded.volatility_1y,
                "volatility_3y": stmt.excluded.volatility_3y,
                "beta_vs_nifty": stmt.excluded.beta_vs_nifty,
                "updated_at": sa.func.now(),
            },
        )
        await session.execute(stmt)
        total_upserted += len(batch)

    await session.flush()

    logger.info(
        "fund_derived_metrics_compute_complete",
        business_date=business_date.isoformat(),
        rows_upserted=total_upserted,
    )

    return total_upserted
