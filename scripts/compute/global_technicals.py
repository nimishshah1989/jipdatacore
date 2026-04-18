"""Compute technical indicators for global instruments.

Reads from de_global_prices, writes to de_global_technical_daily.
Uses the same vectorized pandas approach as etf_technicals.py.

Usage:
    python -m scripts.compute.global_technicals
    python -m scripts.compute.global_technicals --start-date 2010-01-01
    python -m scripts.compute.global_technicals --filter-date 2024-01-01
"""

from __future__ import annotations

import argparse
import gc
import os
import time

import numpy as np
import pandas as pd
import psycopg2

from scripts.compute.db import get_sync_url

# Columns that map to de_global_technical_daily (excluding computed generated columns)
ETF_INDICATOR_COLS = [
    "close_adj",
    "sma_50",
    "sma_200",
    "ema_10",
    "ema_20",
    "ema_50",
    "ema_200",
    "rsi_14",
    "rsi_7",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "roc_5",
    "roc_21",
    "volatility_20d",
    "volatility_60d",
    "bollinger_upper",
    "bollinger_lower",
    "adx_14",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS de_global_technical_daily (
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL REFERENCES de_global_instrument_master(ticker) ON DELETE CASCADE,
    close NUMERIC(18,4),
    sma_50 NUMERIC(18,4),
    sma_200 NUMERIC(18,4),
    ema_10 NUMERIC(18,4),
    ema_20 NUMERIC(18,4),
    ema_50 NUMERIC(18,4),
    ema_200 NUMERIC(18,4),
    rsi_14 NUMERIC(8,4),
    rsi_7 NUMERIC(8,4),
    macd_line NUMERIC(18,4),
    macd_signal NUMERIC(18,4),
    macd_histogram NUMERIC(18,4),
    roc_5 NUMERIC(10,4),
    roc_21 NUMERIC(10,4),
    volatility_20d NUMERIC(10,4),
    volatility_60d NUMERIC(10,4),
    bollinger_upper NUMERIC(18,4),
    bollinger_lower NUMERIC(18,4),
    relative_volume NUMERIC(10,4),
    adx_14 NUMERIC(8,4),
    above_50dma BOOLEAN GENERATED ALWAYS AS (close > sma_50) STORED,
    above_200dma BOOLEAN GENERATED ALWAYS AS (close > sma_200) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    PRIMARY KEY (date, ticker)
)
"""

CREATE_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_global_technical_daily_ticker "
    "ON de_global_technical_daily(ticker)"
)


def compute_global_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all technical indicators for global instruments.

    Input DataFrame must have columns: [ticker, date, close, volume, high, low]
    Groups by 'ticker'.
    """
    g = df.groupby("ticker")

    # SMA
    df["sma_50"] = g["close"].transform(lambda x: x.rolling(50).mean())
    df["sma_200"] = g["close"].transform(lambda x: x.rolling(200).mean())

    # EMAs
    for span, col in [(10, "ema_10"), (20, "ema_20"), (50, "ema_50"), (200, "ema_200")]:
        df[col] = g["close"].transform(lambda x: x.ewm(span=span, adjust=False).mean())

    # MACD (12/26/9)
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("ticker")["macd_line"].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )
    df["macd_histogram"] = df["macd_line"] - df["macd_signal"]
    del ema12, ema26

    # RSI (Wilder's smoothing)
    delta = g["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    for period, col in [(14, "rsi_14"), (7, "rsi_7")]:
        ag = gain.groupby(df["ticker"]).transform(
            lambda x: x.ewm(alpha=1 / period, adjust=False).mean()
        )
        al = loss.groupby(df["ticker"]).transform(
            lambda x: x.ewm(alpha=1 / period, adjust=False).mean()
        )
        rs = ag / al.replace(0, np.nan)
        df[col] = 100 - (100 / (1 + rs))
    del delta, gain, loss

    # ROC (Rate of Change)
    for n, col in [(5, "roc_5"), (21, "roc_21")]:
        df[col] = g["close"].transform(lambda x: (x / x.shift(n) - 1) * 100)

    # Volatility (annualized)
    dr = g["close"].transform(lambda x: x.pct_change())
    df["volatility_20d"] = dr.groupby(df["ticker"]).transform(
        lambda x: x.rolling(20).std() * np.sqrt(252) * 100
    )
    df["volatility_60d"] = dr.groupby(df["ticker"]).transform(
        lambda x: x.rolling(60).std() * np.sqrt(252) * 100
    )
    del dr

    # Bollinger Bands (20-day SMA ± 2*std)
    sma20 = g["close"].transform(lambda x: x.rolling(20).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20).std())
    df["bollinger_upper"] = sma20 + 2 * std20
    df["bollinger_lower"] = sma20 - 2 * std20
    del sma20, std20

    # Relative Volume (today / 20-day avg)
    df["relative_volume"] = df.groupby("ticker")["volume"].transform(
        lambda x: x / x.rolling(20).mean().replace(0, np.nan)
    )

    # ADX 14
    g2 = df.groupby("ticker")
    prev_h = g2["high"].shift(1)
    prev_l = g2["low"].shift(1)
    prev_c = g2["close"].shift(1)
    tr = pd.concat(
        [df["high"] - df["low"], (df["high"] - prev_c).abs(), (df["low"] - prev_c).abs()],
        axis=1,
    ).max(axis=1)
    plus_dm = np.where(
        (df["high"] - prev_h) > (prev_l - df["low"]),
        np.maximum(df["high"] - prev_h, 0),
        0,
    )
    minus_dm = np.where(
        (prev_l - df["low"]) > (df["high"] - prev_h),
        np.maximum(prev_l - df["low"], 0),
        0,
    )
    atr14 = pd.Series(tr, index=df.index).groupby(df["ticker"]).transform(
        lambda x: x.ewm(alpha=1 / 14, adjust=False).mean()
    )
    plus_dm14 = pd.Series(plus_dm, index=df.index).groupby(df["ticker"]).transform(
        lambda x: x.ewm(alpha=1 / 14, adjust=False).mean()
    )
    minus_dm14 = pd.Series(minus_dm, index=df.index).groupby(df["ticker"]).transform(
        lambda x: x.ewm(alpha=1 / 14, adjust=False).mean()
    )
    plus_di = (plus_dm14 / atr14.replace(0, np.nan)) * 100
    minus_di = (minus_dm14 / atr14.replace(0, np.nan)) * 100
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    df["adx_14"] = dx.groupby(df["ticker"]).transform(
        lambda x: x.ewm(alpha=1 / 14, adjust=False).mean()
    )
    del tr, plus_dm, minus_dm, atr14, plus_dm14, minus_dm14, plus_di, minus_di, dx
    del prev_h, prev_l, prev_c

    gc.collect()
    return df


def write_global_technicals_via_staging(
    conn, df: pd.DataFrame, filter_date: str = None
) -> int:
    """Write global technicals via COPY + staging table + INSERT ON CONFLICT.

    Uses ticker (string) as the key column, matching de_global_prices schema.
    """
    cur = conn.cursor()

    if filter_date:
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] >= pd.Timestamp(filter_date)].copy()
        df["date"] = df["date"].dt.date

    df = df.rename(columns={"close": "close_adj"})
    write_cols = ["ticker", "date"] + ETF_INDICATOR_COLS
    out = df[write_cols].copy()

    csv_path = "/tmp/global_tech_staging.csv"
    out.to_csv(csv_path, index=False, header=False, na_rep="\\N")
    del out
    gc.collect()

    col_defs = "ticker VARCHAR(20), date DATE, " + ", ".join(
        [f"{c} DOUBLE PRECISION" for c in ETF_INDICATOR_COLS]
    )
    cur.execute("DROP TABLE IF EXISTS tmp_global_tech_staging")
    cur.execute(f"CREATE TEMP TABLE tmp_global_tech_staging ({col_defs})")

    with open(csv_path) as f:
        col_list = "ticker,date," + ",".join(ETF_INDICATOR_COLS)
        cur.copy_expert(
            f"COPY tmp_global_tech_staging ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
            f,
        )

    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in ETF_INDICATOR_COLS])
    indicator_insert_cols = ", ".join(ETF_INDICATOR_COLS)
    indicator_vals = ", ".join([f"s.{c}" for c in ETF_INDICATOR_COLS])

    cur.execute(
        f"""
        INSERT INTO de_global_technical_daily (ticker, date, {indicator_insert_cols})
        SELECT s.ticker, s.date, {indicator_vals} FROM tmp_global_tech_staging s
        ON CONFLICT (date, ticker) DO UPDATE SET
            {set_clause},
            updated_at = NOW()
        """
    )
    updated = cur.rowcount

    cur.execute("DROP TABLE IF EXISTS tmp_global_tech_staging")
    os.remove(csv_path)

    return updated


def ensure_table(conn) -> None:
    """Create de_global_technical_daily if it does not exist."""
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cur.execute(CREATE_INDEX_SQL)
    conn.commit()
    print("  Table de_global_technical_daily ready", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute global technical indicators")
    parser.add_argument(
        "--start-date",
        default="2010-01-01",
        help="Load prices from this date (need lookback buffer)",
    )
    parser.add_argument(
        "--filter-date",
        default=None,
        help="Only write results from this date onwards",
    )
    args = parser.parse_args()

    t0 = time.time()
    conn = psycopg2.connect(get_sync_url())
    conn.autocommit = True

    # Ensure table exists
    print("Ensuring de_global_technical_daily table exists...", flush=True)
    ensure_table(conn)

    # Load global prices
    print("Loading global prices...", flush=True)
    df = pd.read_sql(
        f"SELECT ticker, date, close::float AS close, COALESCE(volume, 0)::float AS volume, "
        f"COALESCE(high, close)::float AS high, COALESCE(low, close)::float AS low "
        f"FROM de_global_prices WHERE date >= '{args.start_date}' AND close IS NOT NULL "
        f"ORDER BY ticker, date",
        conn,
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    print(
        f"  {len(df):,} rows, {df['ticker'].nunique()} instruments ({time.time()-t0:.1f}s)",
        flush=True,
    )

    if df.empty:
        print("No data found. Has yfinance_pipeline.py been run?", flush=True)
        conn.close()
        return

    # Compute
    print("Computing indicators...", flush=True)
    t1 = time.time()
    df = compute_global_indicators(df)
    print(f"  Indicators computed in {time.time()-t1:.1f}s", flush=True)

    # Write
    print("Writing to de_global_technical_daily via staging...", flush=True)
    t2 = time.time()
    conn.autocommit = False
    updated = write_global_technicals_via_staging(conn, df, args.filter_date)
    conn.commit()
    print(f"  Upserted {updated:,} rows in {time.time()-t2:.1f}s", flush=True)

    conn.close()
    print(f"\nDone in {time.time()-t0:.0f}s ({(time.time()-t0)/60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()
