"""GAP-07 fast path — matrix-wide backfill of multi-window risk columns.

Pure asyncpg (no SQLAlchemy) to keep the COPY+UPDATE semantics transparent.
Loads close prices into a wide DataFrame, computes rolling risk column-wise
with numpy, stacks to long, and UPDATEs the target table via COPY staging.

Usage (from inside the data-engine docker container):
    python -m scripts.fill_new_risk_columns --asset equity
    python -m scripts.fill_new_risk_columns --asset all
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from typing import Iterable, List

import asyncpg
import numpy as np
import pandas as pd

WINDOWS = {"1y": 252, "3y": 756, "5y": 1260}
ANNUALIZER = float(np.sqrt(252.0))
RISK_FREE_DAILY = 0.065 / 252.0

DECIMAL_CLAMP = 999_999.9999
RISK_COLS_ALL = [
    "sharpe_1y", "sharpe_3y", "sharpe_5y",
    "sortino_1y", "sortino_3y", "sortino_5y",
    "max_drawdown_1y", "max_drawdown_3y", "max_drawdown_5y",
    "beta_3y", "beta_5y",
    "treynor_1y", "treynor_3y", "treynor_5y",
    "downside_risk_1y", "downside_risk_3y", "downside_risk_5y",
]


@dataclass(frozen=True)
class AssetSpec:
    name: str
    source_table: str
    target_table: str
    id_col: str
    id_cast: str
    source_close: str
    benchmark: str


SPECS = {
    "equity": AssetSpec("equity", "de_equity_ohlcv", "de_equity_technical_daily",
                        "instrument_id", "uuid", "close_adj", "nifty50"),
    "etf": AssetSpec("etf", "de_etf_ohlcv", "de_etf_technical_daily",
                     "ticker", "text", "close", "nifty50"),
    "global": AssetSpec("global", "de_global_prices", "de_global_technical_daily",
                        "ticker", "text", "close", "nifty50"),
    "index": AssetSpec("index", "de_index_prices", "de_index_technical_daily",
                       "index_code", "text", "close", "nifty50"),
}


def _asyncpg_dsn() -> str:
    url = os.environ["DATABASE_URL"]
    # asyncpg takes the bare postgresql://... form, not the sqlalchemy +asyncpg one
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def _load_wide(conn: asyncpg.Connection, spec: AssetSpec,
                     since_days: int = 2500) -> pd.DataFrame:
    rows = await conn.fetch(
        f"""
        SELECT date, {spec.id_col} AS iid, {spec.source_close} AS px
        FROM {spec.source_table}
        WHERE date >= CURRENT_DATE - make_interval(days => $1)
          AND {spec.source_close} IS NOT NULL
        ORDER BY date
        """,
        since_days,
    )
    if not rows:
        return pd.DataFrame()
    long = pd.DataFrame(
        [(r["date"], str(r["iid"]), float(r["px"])) for r in rows],
        columns=["date", "iid", "px"],
    )
    wide = long.pivot_table(index="date", columns="iid", values="px",
                            aggfunc="last")
    wide.index = pd.to_datetime(wide.index)
    return wide.sort_index().astype(float)


async def _load_benchmark(conn: asyncpg.Connection, which: str,
                          since_days: int = 2500) -> pd.Series:
    if which == "nifty50":
        rows = await conn.fetch(
            """
            SELECT date, close FROM de_index_prices
            WHERE index_code = 'NIFTY 50'
              AND date >= CURRENT_DATE - make_interval(days => $1)
            ORDER BY date
            """,
            since_days,
        )
    elif which == "spy":
        rows = await conn.fetch(
            """
            SELECT date, close FROM de_global_prices
            WHERE ticker = 'SPY'
              AND date >= CURRENT_DATE - make_interval(days => $1)
            ORDER BY date
            """,
            since_days,
        )
    else:
        raise ValueError(which)
    if not rows:
        raise RuntimeError(f"benchmark {which} has no rows")
    s = pd.Series(
        {pd.Timestamp(r["date"]): float(r["close"]) for r in rows}
    )
    return s.sort_index()


def _compute(wide_close: pd.DataFrame,
             bench_close: pd.Series) -> pd.DataFrame:
    returns = wide_close.pct_change()
    bench = bench_close.reindex(wide_close.index).pct_change()

    blocks: dict[str, pd.DataFrame] = {}

    for win_name, win in WINDOWS.items():
        minp = max(20, win // 4)
        mean_r = returns.rolling(win, min_periods=minp).mean()
        std_r = returns.rolling(win, min_periods=minp).std()
        neg = returns.where(returns < 0, 0.0)
        downside_std = neg.rolling(win, min_periods=minp).std()

        sharpe = (mean_r - RISK_FREE_DAILY) / std_r * ANNUALIZER
        sortino = (mean_r - RISK_FREE_DAILY) / downside_std * ANNUALIZER
        downside_risk = downside_std * ANNUALIZER

        cum = (1.0 + returns.fillna(0.0)).cumprod()
        running_max = cum.rolling(win, min_periods=minp).max()
        drawdown = (cum / running_max) - 1.0
        max_dd = drawdown.rolling(win, min_periods=minp).min()

        # Rolling beta column-wise. cov(x, y) with a Series y broadcasts.
        cov = returns.rolling(win, min_periods=minp).cov(bench)
        var_b = bench.rolling(win, min_periods=minp).var()
        beta = cov.div(var_b, axis=0)

        treynor = (mean_r - RISK_FREE_DAILY).div(beta, axis=0) * 252.0

        blocks[f"sharpe_{win_name}"] = sharpe
        blocks[f"sortino_{win_name}"] = sortino
        blocks[f"downside_risk_{win_name}"] = downside_risk
        blocks[f"max_drawdown_{win_name}"] = max_dd
        blocks[f"treynor_{win_name}"] = treynor
        if win_name in ("3y", "5y"):
            blocks[f"beta_{win_name}"] = beta

    # Stack each block to long and outer-join on (date, iid).
    tidy_frames: List[pd.DataFrame] = []
    for name, mat in blocks.items():
        s = mat.stack().rename(name).dropna()
        s.index = s.index.set_names(["date", "iid"])
        tidy_frames.append(s.to_frame())

    merged = pd.concat(tidy_frames, axis=1).reset_index()
    merged["date"] = merged["date"].dt.date

    num = [c for c in RISK_COLS_ALL if c in merged.columns]
    merged[num] = merged[num].replace([np.inf, -np.inf], np.nan)
    merged[num] = merged[num].clip(-DECIMAL_CLAMP, DECIMAL_CLAMP)

    # Ensure every expected col exists so UPDATE SET doesn't choke.
    for c in RISK_COLS_ALL:
        if c not in merged.columns:
            merged[c] = np.nan

    return merged[["date", "iid"] + RISK_COLS_ALL]


async def _update(conn: asyncpg.Connection, spec: AssetSpec,
                  df: pd.DataFrame, batch_size: int = 100_000) -> int:
    if df.empty:
        return 0

    cols_def = ", ".join(f"{c} double precision" for c in RISK_COLS_ALL)
    set_clause = ", ".join(f"{c} = s.{c}" for c in RISK_COLS_ALL)

    total = 0
    n = len(df)
    for start in range(0, n, batch_size):
        chunk = df.iloc[start:start + batch_size]
        records = []
        for row in chunk.itertuples(index=False):
            records.append((
                str(row.iid),
                row.date,
                *[None if pd.isna(getattr(row, c)) else float(getattr(row, c))
                  for c in RISK_COLS_ALL],
            ))

        await conn.execute("DROP TABLE IF EXISTS _risk_stg")
        await conn.execute(
            f"CREATE TEMP TABLE _risk_stg (iid text, date date, {cols_def})"
        )
        await conn.copy_records_to_table(
            "_risk_stg",
            records=records,
            columns=["iid", "date"] + RISK_COLS_ALL,
        )
        r = await conn.execute(
            f"""
            UPDATE {spec.target_table} t SET {set_clause}
            FROM _risk_stg s
            WHERE t.date = s.date
              AND t.{spec.id_col} = s.iid::{spec.id_cast}
            """
        )
        await conn.execute("DROP TABLE IF EXISTS _risk_stg")
        total += len(chunk)
        print(f"  [{spec.name}] batch {start // batch_size + 1}: "
              f"updated (pg: {r}) — cumulative {total:,}/{n:,}")
    return total


async def run_asset(asset: str):
    spec = SPECS[asset]
    print(f"[{asset}] connecting…", flush=True)
    conn = await asyncpg.connect(_asyncpg_dsn())
    try:
        t0 = time.perf_counter()
        print(f"[{asset}] loading close prices from {spec.source_table}…",
              flush=True)
        wide = await _load_wide(conn, spec)
        print(f"[{asset}] wide shape = {wide.shape}", flush=True)
        if wide.empty:
            return
        bench = await _load_benchmark(conn, spec.benchmark)
        print(f"[{asset}] benchmark {spec.benchmark} rows = {len(bench)}",
              flush=True)

        tc = time.perf_counter()
        df = _compute(wide, bench)
        print(f"[{asset}] computed {len(df):,} tidy rows in "
              f"{time.perf_counter() - tc:.1f}s", flush=True)

        tw = time.perf_counter()
        n = await _update(conn, spec, df)
        print(f"[{asset}] wrote {n:,} rows in "
              f"{time.perf_counter() - tw:.1f}s", flush=True)
        print(f"[{asset}] total {time.perf_counter() - t0:.1f}s", flush=True)
    finally:
        await conn.close()


async def main(assets: Iterable[str]):
    for a in assets:
        await run_asset(a)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", choices=list(SPECS) + ["all"], required=True)
    args = ap.parse_args()
    targets = list(SPECS) if args.asset == "all" else [args.asset]
    asyncio.run(main(targets))
