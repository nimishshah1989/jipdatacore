"""Risk and annualized volatility metrics via empyrical-reloaded.

These indicators are computed from the daily-return series of each
instrument and cannot be expressed in strategy.yaml (pandas-ta-classic
does not emit them). The engine calls :func:`compute_risk_series` after
the pandas-ta strategy runs, so that risk + HV columns are merged into
the same DataFrame and land in the same upsert.

Functions:
- ``compute_hv_series``: annualized historical volatility at 20/60/252
  day rolling windows, expressed as percent.
- ``compute_risk_series``: rolling risk metrics across 1y/3y/5y windows
  (Sharpe, Sortino, Calmar, max drawdown, beta, alpha, omega,
  information ratio, treynor, downside risk).

Implementation notes:
- 6 of 8 core risk metrics use empyrical.roll_* (vectorized, O(N)):
  sharpe, sortino, max_drawdown, beta, alpha, information_ratio.
- 2 of 8 (calmar, omega) have no roll_* variant in empyrical-reloaded
  and use vectorized pandas rolling instead.
- treynor and downside_risk are computed manually (no empyrical variant).
- empyrical.roll_* returns an array of length (N - window), aligned to
  the TAIL of the input. First (window - 1) rows remain NaN.
- Trading-day annualization factor is 252 (Indian market convention,
  matching NSE calendar).
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

# Trading days per year, Indian market convention
TRADING_DAYS_PER_YEAR = 252

# Allow up to 5 missing trading days within a window
_MIN_OBS_RATIO = (TRADING_DAYS_PER_YEAR - 5) / TRADING_DAYS_PER_YEAR

DEFAULT_WINDOWS: list[tuple[str, int]] = [
    ("1y", 252),
    ("3y", 756),
    ("5y", 1260),
]


def compute_hv_series(
    close: pd.Series,
    *,
    extra_windows: list[tuple[str, int]] | None = None,
) -> pd.DataFrame:
    """Annualized historical volatility from log returns.

    HV_N = stdev(log_return over N days) * sqrt(252) * 100
    Output expressed as percent (so 18.5 means 18.5% annualized vol).

    Args:
        close: closing price series (float/Decimal-compatible), DatetimeIndex required.
        extra_windows: additional (label, days) windows to compute volatility for.
            Produces columns named ``volatility_{label}``.

    Returns:
        DataFrame with columns volatility_20d, volatility_60d, hv_252,
        plus any extra_windows columns.
    """
    safe_close = close.clip(lower=1e-9).astype(float)
    log_ret = np.log(safe_close / safe_close.shift(1))
    annualizer = np.sqrt(TRADING_DAYS_PER_YEAR) * 100
    out = pd.DataFrame(index=close.index)
    _col_names = {20: "volatility_20d", 60: "volatility_60d", 252: "hv_252"}
    for window in (20, 60, 252):
        out[_col_names[window]] = (
            log_ret.rolling(window=window, min_periods=window).std() * annualizer
        )
    if extra_windows:
        for label, days in extra_windows:
            out[f"volatility_{label}"] = (
                log_ret.rolling(window=days, min_periods=days).std() * annualizer
            )
    return out


def _compute_window_risk(
    returns: pd.Series,
    returns_filled: pd.Series,
    n: int,
    window: int,
    label: str,
    bench_returns: pd.Series | None,
) -> dict[str, pd.Series]:
    """Compute all risk metrics for a single rolling window. Returns column_name→Series."""
    import empyrical

    idx = returns.index
    cols: dict[str, pd.Series] = {}
    nan_series = pd.Series(np.nan, index=idx)
    min_obs = max(int(window * _MIN_OBS_RATIO), window - 5)

    def _align(arr: np.ndarray) -> np.ndarray:
        pad = np.full(n - len(arr), np.nan)
        return np.concatenate([pad, arr])

    # -- Sharpe
    try:
        arr = empyrical.roll_sharpe_ratio(returns, window=window, annualization=TRADING_DAYS_PER_YEAR)
        cols[f"sharpe_{label}"] = pd.Series(_align(np.asarray(arr, dtype=float)), index=idx)
    except Exception:
        cols[f"sharpe_{label}"] = nan_series.copy()

    # -- Sortino
    try:
        arr = empyrical.roll_sortino_ratio(returns, window=window, annualization=TRADING_DAYS_PER_YEAR)
        cols[f"sortino_{label}"] = pd.Series(_align(np.asarray(arr, dtype=float)), index=idx)
    except Exception:
        cols[f"sortino_{label}"] = nan_series.copy()

    # -- Max drawdown
    try:
        arr = empyrical.roll_max_drawdown(returns, window=window)
        mdd_aligned = _align(np.asarray(arr, dtype=float))
        cols[f"max_drawdown_{label}"] = pd.Series(mdd_aligned, index=idx)
    except Exception:
        mdd_aligned = np.full(n, np.nan)
        cols[f"max_drawdown_{label}"] = nan_series.copy()

    # -- Calmar (vectorized via pandas rolling)
    log_ret = np.log1p(returns_filled)
    rolling_log_sum = log_ret.rolling(window=window, min_periods=window).sum()
    rolling_cum_ret = np.expm1(rolling_log_sum)
    if window == TRADING_DAYS_PER_YEAR:
        annualized_return = rolling_cum_ret
    else:
        annualized_return = np.power(1.0 + rolling_cum_ret, TRADING_DAYS_PER_YEAR / window) - 1.0

    mdd_series = cols[f"max_drawdown_{label}"]
    calmar_arr = annualized_return / np.abs(mdd_series)
    cols[f"calmar_{label}"] = calmar_arr

    # -- Omega (required_return=0)
    pos = returns_filled.clip(lower=0.0)
    neg = returns_filled.clip(upper=0.0)
    rolling_pos = pos.rolling(window=window, min_periods=window).sum()
    rolling_neg = neg.rolling(window=window, min_periods=window).sum()
    with np.errstate(divide="ignore", invalid="ignore"):
        omega_arr = rolling_pos / np.abs(rolling_neg)
    cols[f"omega_{label}"] = omega_arr

    # -- Downside risk: stdev of negative returns × sqrt(annualization)
    neg_returns = returns_filled.clip(upper=0.0)
    neg_sq = neg_returns ** 2
    rolling_mean_neg_sq = neg_sq.rolling(window=window, min_periods=min_obs).mean()
    downside = np.sqrt(rolling_mean_neg_sq) * np.sqrt(TRADING_DAYS_PER_YEAR)
    cols[f"downside_risk_{label}"] = downside

    # -- Benchmark-dependent metrics
    if bench_returns is not None:
        # Beta
        try:
            arr = empyrical.roll_beta(returns, bench_returns, window=window)
            beta_series = pd.Series(_align(np.asarray(arr, dtype=float)), index=idx)
            cols[f"beta_{label}"] = beta_series
        except Exception:
            beta_series = nan_series.copy()
            cols[f"beta_{label}"] = beta_series

        # Alpha (from roll_alpha_beta)
        try:
            ab_arr = empyrical.roll_alpha_beta(
                returns, bench_returns, window=window, annualization=TRADING_DAYS_PER_YEAR
            )
            ab = np.asarray(ab_arr, dtype=float)
            if ab.ndim == 2 and ab.shape[1] == 2:
                cols[f"alpha_{label}"] = pd.Series(_align(ab[:, 0]), index=idx)
                if cols[f"beta_{label}"].isna().all():
                    cols[f"beta_{label}"] = pd.Series(_align(ab[:, 1]), index=idx)
                    beta_series = cols[f"beta_{label}"]
            else:
                cols[f"alpha_{label}"] = nan_series.copy()
        except Exception:
            cols[f"alpha_{label}"] = nan_series.copy()

        # Information ratio
        active = (returns - bench_returns).fillna(0.0)
        roll_mean = active.rolling(window=window, min_periods=min_obs).mean()
        roll_std = active.rolling(window=window, min_periods=min_obs).std(ddof=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            ir_arr = roll_mean / roll_std
        cols[f"information_ratio_{label}"] = ir_arr

        # Treynor = annualized_return / beta
        with np.errstate(divide="ignore", invalid="ignore"):
            treynor = annualized_return / beta_series
        cols[f"treynor_{label}"] = treynor
    else:
        cols[f"beta_{label}"] = nan_series.copy()
        cols[f"alpha_{label}"] = nan_series.copy()
        cols[f"information_ratio_{label}"] = nan_series.copy()
        cols[f"treynor_{label}"] = nan_series.copy()

    return cols


# Column name mapping from internal multi-window names to schema names.
# The 1y window uses legacy v1-compatible names.
_1Y_RENAME = {
    "sharpe_1y": "sharpe_1y",
    "sortino_1y": "sortino_1y",
    "calmar_1y": "calmar_ratio",
    "max_drawdown_1y": "max_drawdown_1y",
    "beta_1y": "beta_nifty",
    "alpha_1y": "risk_alpha_nifty",
    "omega_1y": "risk_omega",
    "information_ratio_1y": "risk_information_ratio",
    "treynor_1y": "treynor_1y",
    "downside_risk_1y": "downside_risk_1y",
}


def compute_risk_series(
    close: pd.Series,
    benchmark_close: Optional[pd.Series] = None,
    *,
    windows: list[tuple[str, int]] | None = None,
) -> pd.DataFrame:
    """Rolling multi-window risk metrics via empyrical-reloaded.

    Args:
        close: instrument closing price series, DatetimeIndex required.
        benchmark_close: benchmark closing price series (for beta/alpha/
            information ratio/treynor). If None, those columns are all NaN.
        windows: list of (label, days) tuples. Defaults to 1y/3y/5y.

    Returns:
        DataFrame with columns for each window's risk metrics.
    """
    if windows is None:
        windows = DEFAULT_WINDOWS

    n = len(close)
    returns = close.pct_change().astype(float)
    returns_filled = returns.fillna(0.0)

    bench_returns: pd.Series | None = None
    if benchmark_close is not None:
        bench_aligned = benchmark_close.reindex(close.index).astype(float)
        bench_returns = bench_aligned.pct_change().astype(float)

    all_cols: dict[str, pd.Series] = {}

    for label, window in windows:
        if n <= window:
            # Create NaN columns for this window
            nan_s = pd.Series(np.nan, index=close.index)
            for prefix in ("sharpe", "sortino", "calmar", "max_drawdown",
                           "omega", "downside_risk", "beta", "alpha",
                           "information_ratio", "treynor"):
                all_cols[f"{prefix}_{label}"] = nan_s.copy()
            continue

        cols = _compute_window_risk(
            returns, returns_filled, n, window, label, bench_returns
        )
        all_cols.update(cols)

    out = pd.DataFrame(all_cols, index=close.index)

    # Rename 1y columns to legacy schema names
    rename_map = {k: v for k, v in _1Y_RENAME.items() if k in out.columns and k != v}
    out = out.rename(columns=rename_map)

    out = out.replace([np.inf, -np.inf], np.nan)
    return out
