"""Risk and annualized volatility metrics via empyrical-reloaded.

These indicators are computed from the daily-return series of each
instrument and cannot be expressed in strategy.yaml (pandas-ta-classic
does not emit them). The engine calls :func:`compute_risk_series` after
the pandas-ta strategy runs, so that risk + HV columns are merged into
the same DataFrame and land in the same upsert.

Functions:
- ``compute_hv_series``: annualized historical volatility at 20/60/252
  day rolling windows, expressed as percent.
- ``compute_risk_series``: rolling 1-year risk metrics (Sharpe, Sortino,
  Calmar, max drawdown, beta, alpha, omega, information ratio).

Implementation notes:
- 6 of 8 risk metrics use empyrical.roll_* (vectorized, O(N)):
  sharpe, sortino, max_drawdown, beta, alpha, information_ratio.
- 2 of 8 (calmar, omega) have no roll_* variant in empyrical-reloaded
  and fall back to a per-row loop.
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

# Allow up to 5 missing trading days within the 252-day window
_MIN_OBSERVATIONS = TRADING_DAYS_PER_YEAR - 5


def compute_hv_series(close: pd.Series) -> pd.DataFrame:
    """Annualized historical volatility from log returns.

    HV_N = stdev(log_return over N days) * sqrt(252) * 100
    Output expressed as percent (so 18.5 means 18.5% annualized vol).

    Args:
        close: closing price series (float/Decimal-compatible), DatetimeIndex required.

    Returns:
        DataFrame with columns volatility_20d, volatility_60d, hv_252.
        Rows before window fills are NaN (standard pandas rolling).
    """
    # Clip to positive values to avoid log(0) or log(negative)
    safe_close = close.clip(lower=1e-9).astype(float)
    log_ret = np.log(safe_close / safe_close.shift(1))
    annualizer = np.sqrt(TRADING_DAYS_PER_YEAR) * 100
    out = pd.DataFrame(index=close.index)
    # Column naming matches v1: volatility_20d / volatility_60d / hv_252.
    # hv_252 keeps the hv_ prefix because v1 had no 252-day column.
    _col_names = {20: "volatility_20d", 60: "volatility_60d", 252: "hv_252"}
    for window in (20, 60, 252):
        out[_col_names[window]] = (
            log_ret.rolling(window=window, min_periods=window).std() * annualizer
        )
    return out


def compute_risk_series(
    close: pd.Series,
    benchmark_close: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """Rolling 1-year risk metrics via empyrical-reloaded.

    6 of 8 metrics use vectorized empyrical.roll_* functions.
    2 metrics (calmar, omega) fall back to a per-row loop because
    empyrical-reloaded has no roll_calmar_ratio or roll_omega_ratio.

    empyrical.roll_* returns an array of length (N - window), aligned
    to the tail of the series. This function pads the front with NaN
    to produce a Series aligned to ``close.index``.

    Args:
        close: instrument closing price series, DatetimeIndex required.
        benchmark_close: benchmark closing price series (for beta/alpha/
            information ratio). If None, those columns are all NaN.

    Returns:
        DataFrame with columns:
          sharpe_1y, sortino_1y, calmar_ratio,
          max_drawdown_1y, beta_nifty, risk_alpha_nifty,
          risk_omega, risk_information_ratio
    """
    import empyrical

    n = len(close)
    window = TRADING_DAYS_PER_YEAR

    # Build output DataFrame; default NaN
    out = pd.DataFrame(
        np.nan,
        index=close.index,
        columns=[
            "sharpe_1y",
            "sortino_1y",
            "calmar_ratio",
            "max_drawdown_1y",
            "beta_nifty",
            "risk_alpha_nifty",
            "risk_omega",
            "risk_information_ratio",
        ],
    )

    if n <= window:
        # Insufficient history for any 1-year window
        return out

    returns = close.pct_change().astype(float)

    # -------------------------------------------------------------------
    # Vectorized metrics via empyrical.roll_*
    # roll_* output length = N - window; we pad the front with (window-1)
    # NaN values so the result aligns to close.index.
    # -------------------------------------------------------------------

    def _align(arr: np.ndarray) -> np.ndarray:
        """Pad front of roll_* output to match close.index length."""
        pad = np.full(n - len(arr), np.nan)
        return np.concatenate([pad, arr])

    try:
        sharpe_arr = empyrical.roll_sharpe_ratio(
            returns, window=window, annualization=window
        )
        out["sharpe_1y"] = _align(np.asarray(sharpe_arr, dtype=float))
    except Exception:
        pass

    try:
        sortino_arr = empyrical.roll_sortino_ratio(
            returns, window=window, annualization=window
        )
        out["sortino_1y"] = _align(np.asarray(sortino_arr, dtype=float))
    except Exception:
        pass

    try:
        mdd_arr = empyrical.roll_max_drawdown(returns, window=window)
        out["max_drawdown_1y"] = _align(np.asarray(mdd_arr, dtype=float))
    except Exception:
        pass

    # -------------------------------------------------------------------
    # Per-row loop for calmar and omega (no vectorized variant)
    # Only iterates over rows i >= window where returns.iloc[i] is valid
    # -------------------------------------------------------------------
    for i in range(window, n):
        window_ret = returns.iloc[i - window + 1 : i + 1].dropna()
        if len(window_ret) < _MIN_OBSERVATIONS:
            continue
        try:
            out.iloc[i, out.columns.get_loc("calmar_ratio")] = float(
                empyrical.calmar_ratio(window_ret, annualization=window)
            )
        except Exception:
            pass
        try:
            out.iloc[i, out.columns.get_loc("risk_omega")] = float(
                empyrical.omega_ratio(window_ret)
            )
        except Exception:
            pass

    # -------------------------------------------------------------------
    # Benchmark-dependent metrics (beta, alpha, information_ratio)
    # -------------------------------------------------------------------
    if benchmark_close is not None:
        bench_aligned = benchmark_close.reindex(close.index).astype(float)
        bench_returns = bench_aligned.pct_change().astype(float)

        try:
            beta_arr = empyrical.roll_beta(returns, bench_returns, window=window)
            out["beta_nifty"] = _align(np.asarray(beta_arr, dtype=float))
        except Exception:
            pass

        try:
            ab_arr = empyrical.roll_alpha_beta(
                returns, bench_returns, window=window, annualization=window
            )
            ab = np.asarray(ab_arr, dtype=float)
            # roll_alpha_beta returns shape (N-window, 2): col 0 = alpha, col 1 = beta
            if ab.ndim == 2 and ab.shape[1] == 2:
                out["risk_alpha_nifty"] = _align(ab[:, 0])
                # beta already computed above; only overwrite if prior failed
                if out["beta_nifty"].isna().all():
                    out["beta_nifty"] = _align(ab[:, 1])
        except Exception:
            pass

        # Information ratio = excess_sharpe (active return / tracking error)
        try:
            # empyrical.excess_sharpe is not vectorized; use per-row loop
            for i in range(window, n):
                wr = returns.iloc[i - window + 1 : i + 1].dropna()
                br = bench_returns.iloc[i - window + 1 : i + 1].dropna()
                common = wr.index.intersection(br.index)
                if len(common) < _MIN_OBSERVATIONS:
                    continue
                try:
                    out.iloc[i, out.columns.get_loc("risk_information_ratio")] = float(
                        empyrical.excess_sharpe(wr.loc[common], br.loc[common])
                    )
                except Exception:
                    pass
        except Exception:
            pass

    # Replace inf/-inf with NaN before returning
    out = out.replace([np.inf, -np.inf], np.nan)
    return out
