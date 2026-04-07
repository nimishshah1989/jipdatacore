"""Step 2: Compute EMA, RSI, MACD, ADX, Bollinger, ROC, volatility via vectorized pandas.

Approach:
1. Load price data from RDS into pandas DataFrame (~300MB for 10 years)
2. Compute all indicators using groupby().transform() — vectorized, no loops
3. Write via CSV → COPY into staging table → UPDATE JOIN (13x faster than row-by-row)

Usage:
    python -m scripts.compute.technicals_pandas
    python -m scripts.compute.technicals_pandas --start-date 2025-01-01
"""

import argparse
import gc
import io
import time

import numpy as np
import pandas as pd
import psycopg2

from scripts.compute.db import get_sync_url

# Indicator columns computed by this module
INDICATOR_COLS = [
    "ema_10", "ema_20", "ema_21", "ema_50", "ema_200",
    "rsi_14", "rsi_7", "rsi_9", "rsi_21",
    "macd_line", "macd_signal", "macd_histogram",
    "roc_5", "roc_10", "roc_21", "roc_63",
    "volatility_20d", "volatility_60d",
    "bollinger_upper", "bollinger_lower",
    "relative_volume",
]

# Additional risk columns
RISK_COLS = [
    "beta_nifty", "sharpe_1y", "sortino_1y", "max_drawdown_1y",
    "calmar_ratio", "obv", "mfi_14", "adx_14", "plus_di", "minus_di",
    "delivery_vs_avg",
]


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all technical indicators on a DataFrame with columns [iid, date, close, volume, high, low, del_pct].

    Uses groupby().transform() with ewm/rolling — fully vectorized, no loops.
    """
    g = df.groupby("iid")

    # EMAs
    for span, col in [(10, "ema_10"), (20, "ema_20"), (21, "ema_21"), (50, "ema_50"), (200, "ema_200")]:
        df[col] = g["close"].transform(lambda x: x.ewm(span=span, adjust=False).mean())

    # MACD (12/26/9)
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_line"] = ema12 - ema26
    df["macd_signal"] = df.groupby("iid")["macd_line"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_histogram"] = df["macd_line"] - df["macd_signal"]
    del ema12, ema26

    # RSI (Wilder's smoothing: ewm(alpha=1/period))
    delta = g["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    for period, col in [(14, "rsi_14"), (7, "rsi_7"), (9, "rsi_9"), (21, "rsi_21")]:
        ag = gain.groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / period, adjust=False).mean())
        al = loss.groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / period, adjust=False).mean())
        rs = ag / al.replace(0, np.nan)
        df[col] = 100 - (100 / (1 + rs))
    del delta, gain, loss

    # ROC (Rate of Change)
    for n, col in [(5, "roc_5"), (10, "roc_10"), (21, "roc_21"), (63, "roc_63")]:
        df[col] = g["close"].transform(lambda x: (x / x.shift(n) - 1) * 100)

    # Volatility (annualized daily return std)
    dr = g["close"].transform(lambda x: x.pct_change())
    df["volatility_20d"] = dr.groupby(df["iid"]).transform(lambda x: x.rolling(20).std() * np.sqrt(252) * 100)
    df["volatility_60d"] = dr.groupby(df["iid"]).transform(lambda x: x.rolling(60).std() * np.sqrt(252) * 100)
    del dr

    # Bollinger Bands (20-day SMA ± 2*std)
    sma20 = g["close"].transform(lambda x: x.rolling(20).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20).std())
    df["bollinger_upper"] = sma20 + 2 * std20
    df["bollinger_lower"] = sma20 - 2 * std20
    del sma20, std20

    # Relative Volume (today / 20-day avg)
    df["relative_volume"] = df.groupby("iid")["volume"].transform(
        lambda x: x / x.rolling(20).mean().replace(0, np.nan)
    )

    gc.collect()
    return df


def compute_risk_indicators(df: pd.DataFrame, nifty_df: pd.DataFrame) -> pd.DataFrame:
    """Compute Beta, Sharpe, Sortino, MaxDD, Calmar, OBV, MFI, ADX, delivery."""
    g = df.groupby("iid")
    RF = 0.07 / 252  # daily risk-free rate

    # Daily returns
    df["ret"] = g["close"].transform(lambda x: x.pct_change())

    # Sharpe 1Y
    avg_ret = df.groupby("iid")["ret"].transform(lambda x: x.rolling(252, min_periods=200).mean())
    std_ret = df.groupby("iid")["ret"].transform(lambda x: x.rolling(252, min_periods=200).std())
    df["sharpe_1y"] = ((avg_ret - RF) / std_ret.replace(0, np.nan)) * np.sqrt(252)

    # Sortino 1Y
    downside = df["ret"].clip(upper=0)
    down_std = downside.groupby(df["iid"]).transform(lambda x: x.rolling(252, min_periods=200).std())
    df["sortino_1y"] = ((avg_ret - RF) / down_std.replace(0, np.nan)) * np.sqrt(252)
    del avg_ret, std_ret, downside, down_std

    # Max Drawdown 1Y
    rolling_peak = g["close"].transform(lambda x: x.rolling(252, min_periods=1).max())
    drawdown = (df["close"] - rolling_peak) / rolling_peak
    df["max_drawdown_1y"] = drawdown.groupby(df["iid"]).transform(lambda x: x.rolling(252, min_periods=200).min()) * 100
    del rolling_peak, drawdown

    # Calmar
    ann_ret = g["ret"].transform(lambda x: x.rolling(252, min_periods=200).mean()) * 252 * 100
    df["calmar_ratio"] = ann_ret / df["max_drawdown_1y"].abs().replace(0, np.nan)
    del ann_ret

    # Beta vs NIFTY 50
    nifty_df = nifty_df.set_index("date")
    nifty_df["nret"] = nifty_df["nclose"].pct_change()
    df = df.merge(nifty_df[["nret"]], left_on="date", right_index=True, how="left")

    def calc_beta(group):
        cov = group["ret"].rolling(252, min_periods=200).cov(group["nret"])
        var = group["nret"].rolling(252, min_periods=200).var()
        return cov / var.replace(0, np.nan)

    df["beta_nifty"] = df.groupby("iid").apply(calc_beta).reset_index(level=0, drop=True)

    # OBV
    prev_close = g["close"].shift(1)
    sign = np.where(df["close"] > prev_close, 1, np.where(df["close"] < prev_close, -1, 0))
    df["obv"] = (df["volume"] * sign).groupby(df["iid"]).cumsum().astype("Int64")
    del prev_close, sign

    # MFI 14
    tp = (df["high"] + df["low"] + df["close"]) / 3
    raw_mf = tp * df["volume"]
    tp_prev = tp.groupby(df["iid"]).shift(1)
    pos_mf = np.where(tp > tp_prev, raw_mf, 0)
    neg_mf = np.where(tp < tp_prev, raw_mf, 0)
    pos_sum = pd.Series(pos_mf, index=df.index).groupby(df["iid"]).transform(lambda x: x.rolling(14).sum())
    neg_sum = pd.Series(neg_mf, index=df.index).groupby(df["iid"]).transform(lambda x: x.rolling(14).sum())
    mfr = pos_sum / neg_sum.replace(0, np.nan)
    df["mfi_14"] = 100 - (100 / (1 + mfr))
    del tp, raw_mf, tp_prev, pos_mf, neg_mf, pos_sum, neg_sum, mfr

    # ADX 14
    prev_h = g["high"].shift(1)
    prev_l = g["low"].shift(1)
    prev_c = g["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - prev_c).abs(), (df["low"] - prev_c).abs()], axis=1).max(axis=1)
    plus_dm = np.where((df["high"] - prev_h) > (prev_l - df["low"]), np.maximum(df["high"] - prev_h, 0), 0)
    minus_dm = np.where((prev_l - df["low"]) > (df["high"] - prev_h), np.maximum(prev_l - df["low"], 0), 0)
    atr14 = pd.Series(tr, index=df.index).groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / 14, adjust=False).mean())
    plus_dm14 = pd.Series(plus_dm, index=df.index).groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / 14, adjust=False).mean())
    minus_dm14 = pd.Series(minus_dm, index=df.index).groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / 14, adjust=False).mean())
    df["plus_di"] = (plus_dm14 / atr14.replace(0, np.nan)) * 100
    df["minus_di"] = (minus_dm14 / atr14.replace(0, np.nan)) * 100
    dx = ((df["plus_di"] - df["minus_di"]).abs() / (df["plus_di"] + df["minus_di"]).replace(0, np.nan)) * 100
    df["adx_14"] = dx.groupby(df["iid"]).transform(lambda x: x.ewm(alpha=1 / 14, adjust=False).mean())
    del tr, plus_dm, minus_dm, atr14, plus_dm14, minus_dm14, dx, prev_h, prev_l, prev_c

    # Delivery vs 20d avg
    if "del_pct" in df.columns:
        df["delivery_vs_avg"] = df.groupby("iid")["del_pct"].transform(lambda x: x / x.rolling(20).mean().replace(0, np.nan))

    df.drop(columns=["ret", "nret"], errors="ignore", inplace=True)
    gc.collect()
    return df


def write_via_staging(conn, df: pd.DataFrame, cols: list[str], filter_date: str = None) -> int:
    """Write computed columns via COPY into staging table + UPDATE JOIN.

    This is 13x faster than row-by-row UPDATE for million-row tables.
    """
    cur = conn.cursor()

    if filter_date:
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] >= pd.Timestamp(filter_date)].copy()
        df["date"] = df["date"].dt.date

    out = df[["iid", "date"] + cols].copy()
    out.rename(columns={"iid": "instrument_id"}, inplace=True)
    csv_path = "/tmp/tech_staging.csv"
    out.to_csv(csv_path, index=False, header=False, na_rep="\\N")
    del out
    gc.collect()

    col_defs = ", ".join([f"{c} DOUBLE PRECISION" for c in cols])
    cur.execute("DROP TABLE IF EXISTS tmp_tech_staging")
    cur.execute(f"CREATE TEMP TABLE tmp_tech_staging (instrument_id UUID, date DATE, {col_defs})")

    with open(csv_path) as f:
        col_list = "instrument_id,date," + ",".join(cols)
        cur.copy_expert(f"COPY tmp_tech_staging ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')", f)

    set_clause = ", ".join([f"{c} = s.{c}" for c in cols])
    cur.execute(f"UPDATE de_equity_technical_daily t SET {set_clause} FROM tmp_tech_staging s WHERE t.instrument_id = s.instrument_id AND t.date = s.date")
    updated = cur.rowcount

    cur.execute("DROP TABLE IF EXISTS tmp_tech_staging")
    import os
    os.remove(csv_path)

    return updated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2016-04-01", help="Load prices from this date (need lookback buffer)")
    parser.add_argument("--filter-date", default=None, help="Only write results from this date onwards")
    parser.add_argument("--skip-risk", action="store_true", help="Skip Beta/Sharpe/Sortino/MaxDD/ADX/MFI/OBV")
    args = parser.parse_args()

    t0 = time.time()
    conn = psycopg2.connect(get_sync_url())
    conn.autocommit = True

    # Load
    print("Loading prices...", flush=True)
    df = pd.read_sql(
        f"SELECT instrument_id::text AS iid, date, COALESCE(close_adj,close)::float AS close, "
        f"COALESCE(volume,0)::float AS volume, high::float, low::float, delivery_pct::float AS del_pct "
        f"FROM de_equity_ohlcv WHERE date >= '{args.start_date}' AND COALESCE(close_adj,close) IS NOT NULL "
        f"ORDER BY instrument_id, date",
        conn,
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["iid", "date"]).reset_index(drop=True)
    print(f"  {len(df):,} rows, {df['iid'].nunique()} instruments ({time.time()-t0:.1f}s)", flush=True)

    # Compute indicators
    print("Computing indicators...", flush=True)
    t1 = time.time()
    df = compute_indicators(df)
    print(f"  Indicators: {time.time()-t1:.1f}s", flush=True)

    # Compute risk indicators
    if not args.skip_risk:
        print("Computing risk indicators...", flush=True)
        t2 = time.time()
        nifty = pd.read_sql(
            "SELECT date, close::float AS nclose FROM de_index_prices WHERE index_code = 'NIFTY 50' AND close IS NOT NULL ORDER BY date",
            conn,
        )
        nifty["date"] = pd.to_datetime(nifty["date"])
        df = compute_risk_indicators(df, nifty)
        print(f"  Risk: {time.time()-t2:.1f}s", flush=True)

    # Write
    print("Writing via staging table...", flush=True)
    t3 = time.time()
    all_cols = INDICATOR_COLS + (RISK_COLS if not args.skip_risk else [])
    # Filter to only columns that exist in df
    write_cols = [c for c in all_cols if c in df.columns]
    updated = write_via_staging(conn, df, write_cols, args.filter_date)
    print(f"  Updated: {updated:,} rows ({time.time()-t3:.1f}s)", flush=True)

    conn.close()
    print(f"\nDone in {time.time()-t0:.0f}s ({(time.time()-t0)/60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()
