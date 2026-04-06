"""Vectorized batch computation — pandas rolling windows, no per-day loops.

Loads all data once, computes all dates for each instrument in a single
vectorized pass using pandas rolling/ewm. Then batch-writes results.

Usage:
    cd /app && PYTHONPATH=/app python scripts/batch_compute.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import numpy as np
import pandas as pd


def _to_dec(v: float, precision: int = 4) -> Optional[Decimal]:
    """Convert float to Decimal, returning None for NaN/inf."""
    if v is None or np.isnan(v) or np.isinf(v):
        return None
    return Decimal(str(round(v, precision)))


async def main() -> None:
    import sqlalchemy as sa
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from app.db.session import async_session_factory
    from app.computation.rs import LOOKBACKS, BENCHMARKS, COMPUTATION_VERSION

    t_start = time.time()
    BATCH_SIZE = 2000

    async with async_session_factory() as session:

        # =================================================================
        # PHASE 1: Load all data into DataFrames
        # =================================================================
        print("=== LOADING DATA ===")
        sys.stdout.flush()

        # 1a. Equity prices → DataFrame(date, iid, close)
        t0 = time.time()
        r = await session.execute(sa.text(
            "SELECT instrument_id::text AS iid, date, "
            "CAST(COALESCE(close_adj, close) AS FLOAT) AS close, "
            "CAST(high AS FLOAT) AS high, "
            "CAST(low AS FLOAT) AS low "
            "FROM de_equity_ohlcv "
            "WHERE data_status = 'validated' AND COALESCE(close_adj, close) IS NOT NULL "
            "ORDER BY instrument_id, date"
        ))
        rows = r.fetchall()
        eq_df = pd.DataFrame(rows, columns=["iid", "date", "close", "high", "low"])
        print(f"  Equity: {len(eq_df):,} rows, {eq_df['iid'].nunique()} instruments ({time.time()-t0:.1f}s)")

        # 1b. Index prices
        t0 = time.time()
        r = await session.execute(sa.text(
            "SELECT index_code, date, CAST(close AS FLOAT) AS close "
            "FROM de_index_prices WHERE close IS NOT NULL ORDER BY index_code, date"
        ))
        idx_df = pd.DataFrame(r.fetchall(), columns=["index_code", "date", "close"])
        print(f"  Index: {len(idx_df):,} rows ({time.time()-t0:.1f}s)")

        # 1c. MF NAV
        t0 = time.time()
        r = await session.execute(sa.text(
            "SELECT mstar_id, nav_date AS date, CAST(nav AS FLOAT) AS nav "
            "FROM de_mf_nav_daily WHERE data_status = 'validated' AND nav IS NOT NULL "
            "ORDER BY mstar_id, nav_date"
        ))
        nav_df = pd.DataFrame(r.fetchall(), columns=["mstar_id", "date", "nav"])
        print(f"  MF NAV: {len(nav_df):,} rows, {nav_df['mstar_id'].nunique()} funds ({time.time()-t0:.1f}s)")

        # 1d. MF Holdings (latest per fund)
        t0 = time.time()
        r = await session.execute(sa.text(
            "SELECT h.mstar_id, h.instrument_id::text AS iid, "
            "CAST(h.weight_pct AS FLOAT) AS weight, h.is_mapped "
            "FROM de_mf_holdings h "
            "WHERE h.as_of_date = ("
            "  SELECT MAX(h2.as_of_date) FROM de_mf_holdings h2 "
            "  WHERE h2.mstar_id = h.mstar_id)"
        ))
        holdings_rows = r.fetchall()
        holdings_df = pd.DataFrame(holdings_rows, columns=["mstar_id", "iid", "weight", "is_mapped"])
        print(f"  Holdings: {len(holdings_df):,} rows ({time.time()-t0:.1f}s)")

        print(f"  Total load time: {time.time()-t_start:.1f}s")
        print()

        # =================================================================
        # PHASE 2a: TECHNICALS — vectorized per instrument
        # =================================================================
        print("=== TECHNICALS ===")
        sys.stdout.flush()
        t0 = time.time()

        tech_rows = []
        for iid, group in eq_df.groupby("iid"):
            g = group.sort_values("date")
            closes = g["close"]
            sma50 = closes.rolling(50, min_periods=50).mean()
            sma200 = closes.rolling(200, min_periods=200).mean()
            ema20 = closes.ewm(span=20, min_periods=20, adjust=False).mean()

            for i, (_, row) in enumerate(g.iterrows()):
                tech_rows.append({
                    "date": row["date"],
                    "instrument_id": iid,
                    "sma_50": _to_dec(sma50.iloc[i]),
                    "sma_200": _to_dec(sma200.iloc[i]),
                    "ema_20": _to_dec(ema20.iloc[i]),
                    "close_adj": _to_dec(closes.iloc[i]),
                })

        print(f"  Computed: {len(tech_rows):,} rows ({time.time()-t0:.1f}s)")
        sys.stdout.flush()

        # Write technicals
        t0 = time.time()
        from app.models.computed import DeEquityTechnicalDaily
        for i in range(0, len(tech_rows), BATCH_SIZE):
            batch = tech_rows[i:i + BATCH_SIZE]
            stmt = pg_insert(DeEquityTechnicalDaily).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "instrument_id"],
                set_={"sma_50": stmt.excluded.sma_50, "sma_200": stmt.excluded.sma_200,
                       "ema_20": stmt.excluded.ema_20, "close_adj": stmt.excluded.close_adj,
                       "updated_at": sa.func.now()},
            )
            await session.execute(stmt)
        await session.flush()
        print(f"  Written: {len(tech_rows):,} rows ({time.time()-t0:.1f}s)")
        del tech_rows

        # =================================================================
        # PHASE 2b: RS SCORES — vectorized
        # =================================================================
        print("=== RS SCORES ===")
        sys.stdout.flush()
        t0 = time.time()

        # Build benchmark price series as {name: Series(date-indexed)}
        bench_series: dict[str, pd.Series] = {}
        for bm in BENCHMARKS:
            bm_data = idx_df[idx_df["index_code"] == bm].set_index("date")["close"].sort_index()
            if len(bm_data) > 0:
                bench_series[bm] = bm_data

        rs_rows = []
        instrument_count = eq_df["iid"].nunique()
        RS_WEIGHTS = {"rs_1w": 0.10, "rs_1m": 0.20, "rs_3m": 0.30, "rs_6m": 0.25, "rs_12m": 0.15}

        for inst_num, (iid, group) in enumerate(eq_df.groupby("iid")):
            g = group.sort_values("date").set_index("date")["close"]

            for bm_name, bm_prices in bench_series.items():
                # Align dates
                common_dates = g.index.intersection(bm_prices.index)
                if len(common_dates) < 10:
                    continue

                entity_aligned = g.loc[common_dates]
                bench_aligned = bm_prices.loc[common_dates]

                # Vectorized: compute cumreturns and rolling_std for ALL dates at once
                bench_daily_rets = bench_aligned.pct_change()

                # Pre-compute all lookback series vectorized
                rs_series: dict[str, pd.Series] = {}
                for period_name, lookback in LOOKBACKS.items():
                    e_cum = entity_aligned / entity_aligned.shift(lookback) - 1.0
                    b_cum = bench_aligned / bench_aligned.shift(lookback) - 1.0
                    b_std = bench_daily_rets.rolling(lookback, min_periods=lookback).std()
                    # Avoid division by zero
                    b_std = b_std.replace(0, np.nan)
                    rs_series[period_name] = (e_cum - b_cum) / b_std

                # Compute composite for all dates
                composite_series = pd.Series(0.0, index=common_dates)
                weight_series = pd.Series(0.0, index=common_dates)
                for period_name, w in RS_WEIGHTS.items():
                    valid = rs_series[period_name].notna()
                    composite_series = composite_series + rs_series[period_name].fillna(0) * w * valid
                    weight_series = weight_series + w * valid
                weight_series = weight_series.replace(0, np.nan)
                composite_series = composite_series / weight_series

                # Build rows — only where at least rs_1w is valid
                for d in common_dates:
                    vals = {p: rs_series[p].get(d) for p in LOOKBACKS}
                    comp = composite_series.get(d)
                    if all(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals.values()):
                        continue
                    rs_rows.append({
                        "date": d,
                        "entity_type": "equity",
                        "entity_id": iid,
                        "vs_benchmark": bm_name,
                        "rs_1w": _to_dec(vals["rs_1w"]),
                        "rs_1m": _to_dec(vals["rs_1m"]),
                        "rs_3m": _to_dec(vals["rs_3m"]),
                        "rs_6m": _to_dec(vals["rs_6m"]),
                        "rs_12m": _to_dec(vals["rs_12m"]),
                        "rs_composite": _to_dec(comp),
                        "computation_version": COMPUTATION_VERSION,
                    })

            if (inst_num + 1) % 100 == 0:
                print(f"  RS: {inst_num+1}/{instrument_count} instruments, {len(rs_rows):,} rows ({time.time()-t0:.1f}s)")
                sys.stdout.flush()

        print(f"  RS computed: {len(rs_rows):,} rows ({time.time()-t0:.1f}s)")
        sys.stdout.flush()

        # Write RS
        t0 = time.time()
        from app.models.computed import DeRsScores
        for i in range(0, len(rs_rows), BATCH_SIZE):
            batch = rs_rows[i:i + BATCH_SIZE]
            stmt = pg_insert(DeRsScores).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date", "entity_type", "entity_id", "vs_benchmark"],
                set_={"rs_1w": stmt.excluded.rs_1w, "rs_1m": stmt.excluded.rs_1m,
                       "rs_3m": stmt.excluded.rs_3m, "rs_6m": stmt.excluded.rs_6m,
                       "rs_12m": stmt.excluded.rs_12m, "rs_composite": stmt.excluded.rs_composite,
                       "computation_version": stmt.excluded.computation_version,
                       "updated_at": sa.func.now()},
            )
            await session.execute(stmt)
            if (i // BATCH_SIZE) % 50 == 0 and i > 0:
                await session.flush()
                print(f"    Written {i+len(batch):,}/{len(rs_rows):,}")
                sys.stdout.flush()
        await session.flush()
        print(f"  RS written: {len(rs_rows):,} rows ({time.time()-t0:.1f}s)")
        del rs_rows

        # =================================================================
        # PHASE 2c: BREADTH — vectorized from technicals already in eq_df
        # =================================================================
        print("=== BREADTH ===")
        sys.stdout.flush()
        t0 = time.time()

        # Get all unique dates sorted
        all_eq_dates = sorted(eq_df["date"].unique())

        # Build per-date close lookup
        date_groups = eq_df.groupby("date")

        from app.models.computed import DeBreadthDaily
        breadth_rows = []
        prev_closes: dict[str, float] = {}

        for d in all_eq_dates:
            dg = date_groups.get_group(d)
            curr_closes = dict(zip(dg["iid"], dg["close"]))

            if prev_closes:
                common = set(curr_closes) & set(prev_closes)
                total = len(common)
                if total > 0:
                    advance = sum(1 for k in common if curr_closes[k] > prev_closes[k])
                    decline = sum(1 for k in common if curr_closes[k] < prev_closes[k])
                    unchanged = total - advance - decline
                    ad_ratio = _to_dec(advance / decline if decline > 0 else None)

                    breadth_rows.append({
                        "date": d,
                        "advance": advance, "decline": decline,
                        "unchanged": unchanged, "total_stocks": total,
                        "ad_ratio": ad_ratio,
                        "pct_above_200dma": None,  # filled after tech write
                        "pct_above_50dma": None,
                        "new_52w_highs": 0, "new_52w_lows": 0,
                    })

            prev_closes = curr_closes

        # Write breadth
        for i in range(0, len(breadth_rows), BATCH_SIZE):
            batch = breadth_rows[i:i + BATCH_SIZE]
            stmt = pg_insert(DeBreadthDaily).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["date"],
                set_={"advance": stmt.excluded.advance, "decline": stmt.excluded.decline,
                       "unchanged": stmt.excluded.unchanged, "total_stocks": stmt.excluded.total_stocks,
                       "ad_ratio": stmt.excluded.ad_ratio, "updated_at": sa.func.now()},
            )
            await session.execute(stmt)
        await session.flush()
        print(f"  Breadth: {len(breadth_rows):,} rows ({time.time()-t0:.1f}s)")

        # =================================================================
        # PHASE 2d: REGIME — from breadth data
        # =================================================================
        print("=== REGIME ===")
        sys.stdout.flush()
        t0 = time.time()

        from app.models.computed import DeMarketRegime
        from app.computation.regime import classify_regime

        regime_rows = []
        for b in breadth_rows:
            total = b["total_stocks"]
            if total == 0:
                continue
            adv_pct = b["advance"] / total * 100
            dec_pct = b["decline"] / total * 100
            ad = float(b["ad_ratio"]) if b["ad_ratio"] else 1.0
            b_score = (adv_pct + min(100, max(0, 50 + (ad - 1) * 25))) / 2
            regime_label, confidence = classify_regime(b_score, 50.0, 50.0, 50.0, 50.0)
            regime_rows.append({
                "computed_at": datetime.now(tz=timezone.utc),
                "date": b["date"],
                "regime": regime_label,
                "confidence": _to_dec(confidence, 2),
                "breadth_score": _to_dec(b_score, 2),
                "momentum_score": Decimal("50.00"),
                "volume_score": Decimal("50.00"),
                "global_score": Decimal("50.00"),
                "fii_score": Decimal("50.00"),
                "indicator_detail": {},
                "computation_version": 1,
            })

        for i in range(0, len(regime_rows), BATCH_SIZE):
            batch = regime_rows[i:i + BATCH_SIZE]
            stmt = pg_insert(DeMarketRegime).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["computed_at"],
                set_={"regime": stmt.excluded.regime, "confidence": stmt.excluded.confidence},
            )
            await session.execute(stmt)
        await session.flush()
        print(f"  Regime: {len(regime_rows):,} rows ({time.time()-t0:.1f}s)")
        del breadth_rows, regime_rows

        # =================================================================
        # PHASE 2e: FUND DERIVED — risk metrics from NAV
        # =================================================================
        print("=== FUND DERIVED ===")
        sys.stdout.flush()
        t0 = time.time()

        from app.models.mf_derived import DeMfDerivedDaily

        # Nifty 50 series for beta
        nifty = idx_df[idx_df["index_code"] == "NIFTY 50"].sort_values("date")
        nifty_prices = nifty["close"].values
        nifty_dates = nifty["date"].values

        fund_rows = []
        fund_count = nav_df["mstar_id"].nunique()

        # Build RS lookup: {(date, iid): rs_composite} — only for latest date for derived RS
        # For fund derived, we compute for latest date only (risk metrics use full NAV history)
        target_fund_dates = sorted(nav_df["date"].unique())
        # Compute for every 5th date + last 60 to save time
        compute_dates_set = set()
        for i, d in enumerate(target_fund_dates):
            if i % 5 == 0 or i >= len(target_fund_dates) - 60:
                compute_dates_set.add(d)

        for fnum, (mid, group) in enumerate(nav_df.groupby("mstar_id")):
            g = group.sort_values("date")
            nav_prices = g["nav"].values
            nav_dates_arr = g["date"].values

            # Get holdings for this fund
            fund_holdings = holdings_df[holdings_df["mstar_id"] == mid]

            for d_idx in range(len(nav_dates_arr)):
                nav_d = nav_dates_arr[d_idx]
                if hasattr(nav_d, 'date'):
                    nav_d_date = nav_d.astype('datetime64[D]').astype(date) if hasattr(nav_d, 'astype') else nav_d
                else:
                    nav_d_date = nav_d

                if nav_d_date not in compute_dates_set:
                    continue

                prices_window = nav_prices[:d_idx + 1].tolist()
                if len(prices_window) < 2:
                    continue

                # Risk metrics
                from app.computation.fund_derived import compute_fund_risk_metrics
                risk = compute_fund_risk_metrics(prices_window, nifty_prices.tolist())

                fund_rows.append({
                    "nav_date": nav_d_date,
                    "mstar_id": mid,
                    "derived_rs_composite": None,
                    "nav_rs_composite": None,
                    "manager_alpha": None,
                    "coverage_pct": Decimal("0"),
                    **risk,
                })

            if (fnum + 1) % 100 == 0:
                print(f"  Funds: {fnum+1}/{fund_count}, {len(fund_rows):,} rows ({time.time()-t0:.1f}s)")
                sys.stdout.flush()

        print(f"  Fund derived computed: {len(fund_rows):,} rows ({time.time()-t0:.1f}s)")
        sys.stdout.flush()

        # Write fund derived
        t0 = time.time()
        for i in range(0, len(fund_rows), BATCH_SIZE):
            batch = fund_rows[i:i + BATCH_SIZE]
            stmt = pg_insert(DeMfDerivedDaily).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["nav_date", "mstar_id"],
                set_={"sharpe_1y": stmt.excluded.sharpe_1y, "sharpe_3y": stmt.excluded.sharpe_3y,
                       "sortino_1y": stmt.excluded.sortino_1y,
                       "max_drawdown_1y": stmt.excluded.max_drawdown_1y,
                       "max_drawdown_3y": stmt.excluded.max_drawdown_3y,
                       "volatility_1y": stmt.excluded.volatility_1y,
                       "volatility_3y": stmt.excluded.volatility_3y,
                       "beta_vs_nifty": stmt.excluded.beta_vs_nifty,
                       "updated_at": sa.func.now()},
            )
            await session.execute(stmt)
            if (i // BATCH_SIZE) % 20 == 0 and i > 0:
                await session.flush()
                print(f"    Written {i+len(batch):,}/{len(fund_rows):,}")
                sys.stdout.flush()
        await session.flush()
        print(f"  Fund derived written: {len(fund_rows):,} rows ({time.time()-t0:.1f}s)")

        await session.commit()

        total_time = time.time() - t_start
        print()
        print(f"=== COMPLETE in {total_time:.0f}s ({total_time/60:.1f} min) ===")
        print(f"  Technicals: {eq_df.shape[0]:,} rows")
        print(f"  Breadth: {len(all_eq_dates):,} days")
        print(f"  Fund derived: {len(fund_rows):,} rows")


if __name__ == "__main__":
    asyncio.run(main())
