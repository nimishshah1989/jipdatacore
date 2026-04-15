"""GAP-10 fast path — recompute de_mf_derived_daily risk columns via vectorized
matrix rolling (replaces the old hand-rolled MF risk formulas).

Loads NAV series as a wide DataFrame, computes sharpe/sortino/drawdown/
volatility/beta/treynor/information_ratio column-wise, then UPDATEs
de_mf_derived_daily. Non-risk columns (manager_alpha, *_rs_composite,
coverage_pct) are left untouched.

Usage (inside the data-engine docker container):
    python /app/scripts/fill_mf_derived_risk.py
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import List

import asyncpg
import numpy as np
import pandas as pd

WINDOWS = {"1y": 252, "3y": 756, "5y": 1260}
ANNUALIZER = float(np.sqrt(252.0))
RISK_FREE_DAILY = 0.065 / 252.0
DECIMAL_CLAMP = 999_999.9999

# Target columns in de_mf_derived_daily (legacy shape)
RISK_COLS = [
    "sharpe_1y", "sharpe_3y", "sharpe_5y",
    "sortino_1y", "sortino_3y", "sortino_5y",
    "max_drawdown_1y", "max_drawdown_3y", "max_drawdown_5y",
    "volatility_1y", "volatility_3y",          # annualized
    "stddev_1y", "stddev_3y", "stddev_5y",     # daily
    "beta_vs_nifty",                            # single, 3y window
    "information_ratio",                        # single, 3y window
    "treynor_ratio",                            # single, 3y window
]


def _dsn() -> str:
    return os.environ["DATABASE_URL"].replace(
        "postgresql+asyncpg://", "postgresql://"
    )


async def _load_nav_wide(conn: asyncpg.Connection,
                         since_days: int = 2500) -> pd.DataFrame:
    rows = await conn.fetch(
        """
        SELECT nav_date, mstar_id, nav
        FROM de_mf_nav_daily
        WHERE nav_date >= CURRENT_DATE - make_interval(days => $1)
          AND nav IS NOT NULL AND nav > 0
        ORDER BY nav_date
        """,
        since_days,
    )
    if not rows:
        return pd.DataFrame()
    long = pd.DataFrame(
        [(r["nav_date"], r["mstar_id"], float(r["nav"])) for r in rows],
        columns=["date", "mstar_id", "nav"],
    )
    wide = long.pivot_table(index="date", columns="mstar_id", values="nav",
                            aggfunc="last")
    wide.index = pd.to_datetime(wide.index)
    return wide.sort_index().astype(float)


async def _load_nifty(conn: asyncpg.Connection,
                      since_days: int = 2500) -> pd.Series:
    rows = await conn.fetch(
        """
        SELECT date, close FROM de_index_prices
        WHERE index_code = 'NIFTY 50'
          AND date >= CURRENT_DATE - make_interval(days => $1)
        ORDER BY date
        """,
        since_days,
    )
    if not rows:
        raise RuntimeError("nifty50 has no rows")
    return pd.Series(
        {pd.Timestamp(r["date"]): float(r["close"]) for r in rows}
    ).sort_index()


def _compute(wide_nav: pd.DataFrame, nifty: pd.Series) -> pd.DataFrame:
    returns = wide_nav.pct_change()
    bench = nifty.reindex(wide_nav.index).pct_change()

    blocks: dict[str, pd.DataFrame] = {}

    for win_name, win in WINDOWS.items():
        minp = max(20, win // 4)
        mean_r = returns.rolling(win, min_periods=minp).mean()
        std_r = returns.rolling(win, min_periods=minp).std()
        neg = returns.where(returns < 0, 0.0)
        downside_std = neg.rolling(win, min_periods=minp).std()

        sharpe = (mean_r - RISK_FREE_DAILY) / std_r * ANNUALIZER
        sortino = (mean_r - RISK_FREE_DAILY) / downside_std * ANNUALIZER

        cum = (1.0 + returns.fillna(0.0)).cumprod()
        running_max = cum.rolling(win, min_periods=minp).max()
        drawdown = (cum / running_max) - 1.0
        max_dd = drawdown.rolling(win, min_periods=minp).min()

        blocks[f"sharpe_{win_name}"] = sharpe
        blocks[f"sortino_{win_name}"] = sortino
        blocks[f"max_drawdown_{win_name}"] = max_dd
        blocks[f"stddev_{win_name}"] = std_r
        if win_name in ("1y", "3y"):
            blocks[f"volatility_{win_name}"] = std_r * ANNUALIZER

        if win_name == "3y":
            # Single-value columns all use the 3y rolling window.
            cov = returns.rolling(win, min_periods=minp).cov(bench)
            var_b = bench.rolling(win, min_periods=minp).var()
            beta = cov.div(var_b, axis=0)
            blocks["beta_vs_nifty"] = beta

            bench_repl = pd.DataFrame(
                np.broadcast_to(bench.values[:, None], returns.shape),
                index=returns.index, columns=returns.columns,
            )
            active = returns - bench_repl
            tracking_err = active.rolling(win, min_periods=minp).std()
            active_mean = active.rolling(win, min_periods=minp).mean()
            blocks["information_ratio"] = (active_mean / tracking_err) * ANNUALIZER

            blocks["treynor_ratio"] = (
                (mean_r - RISK_FREE_DAILY).div(beta, axis=0) * 252.0
            )

    tidy_frames: List[pd.DataFrame] = []
    for name, mat in blocks.items():
        s = mat.stack().rename(name).dropna()
        s.index = s.index.set_names(["date", "mstar_id"])
        tidy_frames.append(s.to_frame())

    merged = pd.concat(tidy_frames, axis=1).reset_index()
    merged["date"] = merged["date"].dt.date

    for c in RISK_COLS:
        if c not in merged.columns:
            merged[c] = np.nan

    merged[RISK_COLS] = (
        merged[RISK_COLS].replace([np.inf, -np.inf], np.nan)
        .clip(-DECIMAL_CLAMP, DECIMAL_CLAMP)
    )
    return merged[["date", "mstar_id"] + RISK_COLS]


async def _update(conn: asyncpg.Connection,
                  df: pd.DataFrame,
                  batch_size: int = 100_000) -> int:
    if df.empty:
        return 0

    cols_def = ", ".join(f"{c} double precision" for c in RISK_COLS)
    set_clause = ", ".join(f"{c} = s.{c}" for c in RISK_COLS)

    total = 0
    n = len(df)
    for start in range(0, n, batch_size):
        chunk = df.iloc[start:start + batch_size]
        records = []
        for row in chunk.itertuples(index=False):
            records.append((
                row.mstar_id, row.date,
                *[None if pd.isna(getattr(row, c)) else float(getattr(row, c))
                  for c in RISK_COLS],
            ))

        await conn.execute("DROP TABLE IF EXISTS _mf_risk_stg")
        await conn.execute(
            f"CREATE TEMP TABLE _mf_risk_stg "
            f"(mstar_id text, nav_date date, {cols_def})"
        )
        await conn.copy_records_to_table(
            "_mf_risk_stg", records=records,
            columns=["mstar_id", "nav_date"] + RISK_COLS,
        )
        r = await conn.execute(
            f"""
            UPDATE de_mf_derived_daily t SET {set_clause}
            FROM _mf_risk_stg s
            WHERE t.nav_date = s.nav_date
              AND t.mstar_id = s.mstar_id
            """
        )
        await conn.execute("DROP TABLE IF EXISTS _mf_risk_stg")
        total += len(chunk)
        print(f"  batch {start // batch_size + 1}: pg={r} cumulative "
              f"{total:,}/{n:,}", flush=True)
    return total


async def _upsert_missing(conn: asyncpg.Connection,
                          df: pd.DataFrame,
                          batch_size: int = 100_000) -> int:
    """For (mstar_id, nav_date) pairs that don't exist yet in de_mf_derived_daily,
    INSERT them. UPDATE path only touches existing rows."""
    if df.empty:
        return 0

    cols_def = ", ".join(f"{c} double precision" for c in RISK_COLS)
    set_clause_cols = ", ".join(RISK_COLS)
    set_clause_vals = ", ".join(f"s.{c}" for c in RISK_COLS)

    total = 0
    n = len(df)
    for start in range(0, n, batch_size):
        chunk = df.iloc[start:start + batch_size]
        records = []
        for row in chunk.itertuples(index=False):
            records.append((
                row.mstar_id, row.date,
                *[None if pd.isna(getattr(row, c)) else float(getattr(row, c))
                  for c in RISK_COLS],
            ))

        await conn.execute("DROP TABLE IF EXISTS _mf_risk_stg")
        await conn.execute(
            f"CREATE TEMP TABLE _mf_risk_stg "
            f"(mstar_id text, nav_date date, {cols_def})"
        )
        await conn.copy_records_to_table(
            "_mf_risk_stg", records=records,
            columns=["mstar_id", "nav_date"] + RISK_COLS,
        )
        r = await conn.execute(
            f"""
            INSERT INTO de_mf_derived_daily
              (mstar_id, nav_date, {set_clause_cols}, created_at, updated_at)
            SELECT s.mstar_id, s.nav_date, {set_clause_vals}, now(), now()
            FROM _mf_risk_stg s
            ON CONFLICT (mstar_id, nav_date) DO UPDATE SET
              {', '.join(f"{c} = EXCLUDED.{c}" for c in RISK_COLS)},
              updated_at = now()
            """
        )
        await conn.execute("DROP TABLE IF EXISTS _mf_risk_stg")
        total += len(chunk)
        print(f"  upsert batch {start // batch_size + 1}: pg={r} "
              f"cumulative {total:,}/{n:,}", flush=True)
    return total


async def main():
    print("[mf_risk] connecting…", flush=True)
    conn = await asyncpg.connect(_dsn())
    try:
        t0 = time.perf_counter()
        print("[mf_risk] loading NAV wide…", flush=True)
        wide = await _load_nav_wide(conn)
        print(f"[mf_risk] NAV shape = {wide.shape}", flush=True)
        if wide.empty:
            return

        nifty = await _load_nifty(conn)
        print(f"[mf_risk] nifty rows = {len(nifty)}", flush=True)

        tc = time.perf_counter()
        df = _compute(wide, nifty)
        print(f"[mf_risk] computed {len(df):,} tidy rows in "
              f"{time.perf_counter() - tc:.1f}s", flush=True)

        tw = time.perf_counter()
        n = await _upsert_missing(conn, df)
        print(f"[mf_risk] wrote {n:,} rows in "
              f"{time.perf_counter() - tw:.1f}s", flush=True)
        print(f"[mf_risk] total {time.perf_counter() - t0:.1f}s", flush=True)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
