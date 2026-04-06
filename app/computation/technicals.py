"""Technical indicator computations — pure functions operating on numpy arrays.

All financial output values are returned as Decimal(str(round(result, 4))).
numpy/pandas operations use float internally; conversion happens at boundary.
"""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Optional

import numpy as np

from app.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Moving Averages
# ---------------------------------------------------------------------------


def compute_ema(prices: list[float], period: int) -> list[Optional[Decimal]]:
    """Compute Exponential Moving Average.

    Formula: k = 2/(period+1), EMA[i] = price[i]*k + EMA[i-1]*(1-k)
    First valid EMA seeded with SMA of first `period` values.

    Args:
        prices: List of close prices (chronological order, oldest first).
        period: EMA period.

    Returns:
        List of Decimal EMA values; None for indices before first valid EMA.
    """
    if not prices or period <= 0:
        return [None] * len(prices)

    n = len(prices)
    k = 2.0 / (period + 1)
    result: list[Optional[Decimal]] = [None] * n

    if n < period:
        return result

    # Seed with SMA of first `period` values
    seed = sum(prices[:period]) / period
    result[period - 1] = Decimal(str(round(seed, 4)))

    ema_prev = seed
    for i in range(period, n):
        ema_val = prices[i] * k + ema_prev * (1 - k)
        result[i] = Decimal(str(round(ema_val, 4)))
        ema_prev = ema_val

    return result


def compute_sma(prices: list[float], period: int) -> list[Optional[Decimal]]:
    """Compute Simple Moving Average using incremental update.

    Formula: SMA_today = SMA_yesterday + (close_today - close_N_days_ago) / N

    Args:
        prices: List of close prices (chronological order, oldest first).
        period: SMA period.

    Returns:
        List of Decimal SMA values; None for indices before first valid SMA.
    """
    if not prices or period <= 0:
        return [None] * len(prices)

    n = len(prices)
    result: list[Optional[Decimal]] = [None] * n

    if n < period:
        return result

    # Seed first SMA
    running_sum = sum(prices[:period])
    result[period - 1] = Decimal(str(round(running_sum / period, 4)))

    for i in range(period, n):
        running_sum += prices[i] - prices[i - period]
        result[i] = Decimal(str(round(running_sum / period, 4)))

    return result


# ---------------------------------------------------------------------------
# RSI (Wilder's smoothing)
# ---------------------------------------------------------------------------


def compute_rsi_wilder(prices: list[float], period: int = 14) -> list[Optional[Decimal]]:
    """Compute RSI using Wilder's smoothing method.

    Formula:
        avg_gain[i] = (avg_gain[i-1]*(period-1) + gain[i]) / period
        RSI = 100 - 100/(1+RS)  where RS = avg_gain / avg_loss

    Args:
        prices: List of close prices (chronological order, oldest first).
        period: RSI period (default 14).

    Returns:
        List of Decimal RSI values; None for indices before first valid RSI.
    """
    if not prices or period <= 0:
        return [None] * len(prices)

    n = len(prices)
    result: list[Optional[Decimal]] = [None] * n

    if n < period + 1:
        return result

    # Compute price changes
    changes = [prices[i] - prices[i - 1] for i in range(1, n)]
    gains = [max(c, 0.0) for c in changes]
    losses = [max(-c, 0.0) for c in changes]

    # Seed: simple average of first `period` gains/losses
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # First RSI at index `period`
    if avg_loss == 0.0:
        rsi_val = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_val = 100.0 - 100.0 / (1.0 + rs)
    result[period] = Decimal(str(round(rsi_val, 4)))

    for i in range(period, n - 1):
        gain = gains[i]
        loss = losses[i]
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0.0:
            rsi_val = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_val = 100.0 - 100.0 / (1.0 + rs)
        result[i + 1] = Decimal(str(round(rsi_val, 4)))

    return result


# ---------------------------------------------------------------------------
# MACD
# ---------------------------------------------------------------------------


def compute_macd(
    prices: list[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[list[Optional[Decimal]], list[Optional[Decimal]], list[Optional[Decimal]]]:
    """Compute MACD line, signal line, and histogram.

    Formula:
        MACD line = EMA(fast) - EMA(slow)
        Signal line = EMA(9) of MACD line
        Histogram = MACD line - signal line

    Args:
        prices: List of close prices.
        fast: Fast EMA period (default 12).
        slow: Slow EMA period (default 26).
        signal: Signal EMA period (default 9).

    Returns:
        Tuple of (macd_line, signal_line, histogram) as Decimal lists.
    """
    n = len(prices)
    ema_fast = compute_ema(prices, fast)
    ema_slow = compute_ema(prices, slow)

    # MACD line: defined only where both EMAs are defined
    macd_line: list[Optional[float]] = [None] * n
    for i in range(n):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = float(ema_fast[i]) - float(ema_slow[i])  # type: ignore[arg-type]

    # Signal line: EMA(signal) of macd_line — only on valid MACD values
    valid_indices = [i for i, v in enumerate(macd_line) if v is not None]
    signal_line: list[Optional[Decimal]] = [None] * n
    histogram: list[Optional[Decimal]] = [None] * n

    if len(valid_indices) >= signal:
        macd_values = [macd_line[i] for i in valid_indices]  # type: ignore[misc]
        signal_emas = compute_ema(macd_values, signal)  # type: ignore[arg-type]

        for j, idx in enumerate(valid_indices):
            if signal_emas[j] is not None:
                macd_dec = Decimal(str(round(macd_line[idx], 4)))  # type: ignore[arg-type]
                sig_dec = signal_emas[j]
                signal_line[idx] = sig_dec
                histogram[idx] = Decimal(str(round(float(macd_dec) - float(sig_dec), 4)))  # type: ignore[arg-type]

    macd_dec_line: list[Optional[Decimal]] = [
        Decimal(str(round(v, 4))) if v is not None else None for v in macd_line
    ]

    return macd_dec_line, signal_line, histogram


# ---------------------------------------------------------------------------
# ADX (Average Directional Index)
# ---------------------------------------------------------------------------


def compute_adx(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int = 14,
) -> tuple[list[Optional[Decimal]], list[Optional[Decimal]], list[Optional[Decimal]]]:
    """Compute ADX, +DI, and -DI using Wilder's method.

    Formula:
        +DM = max(high - prev_high, 0) if > (-DM), else 0
        -DM = max(prev_low - low, 0) if > (+DM), else 0
        TR = max(high-low, |high-prev_close|, |low-prev_close|)
        +DI = 100 * Wilder_smooth(+DM) / Wilder_smooth(TR)
        -DI = 100 * Wilder_smooth(-DM) / Wilder_smooth(TR)
        DX = 100 * |+DI - -DI| / (+DI + -DI)
        ADX = Wilder_smooth(DX, period)

    Args:
        highs: List of high prices.
        lows: List of low prices.
        closes: List of close prices.
        period: ADX period (default 14).

    Returns:
        Tuple of (adx, plus_di, minus_di) as Decimal lists.
    """
    n = len(closes)
    if n < period + 1 or not (len(highs) == len(lows) == n):
        return [None] * n, [None] * n, [None] * n

    # Compute raw DM and TR arrays (length n-1, starting from index 1)
    plus_dm = []
    minus_dm = []
    tr_list = []

    for i in range(1, n):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]

        pdm = up_move if (up_move > down_move and up_move > 0) else 0.0
        mdm = down_move if (down_move > up_move and down_move > 0) else 0.0

        plus_dm.append(pdm)
        minus_dm.append(mdm)

        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        tr_list.append(tr)

    m = len(tr_list)  # = n - 1

    if m < period:
        return [None] * n, [None] * n, [None] * n

    # Wilder smooth: seed = sum of first `period` values
    smooth_tr = sum(tr_list[:period])
    smooth_pdm = sum(plus_dm[:period])
    smooth_mdm = sum(minus_dm[:period])

    adx_arr: list[Optional[float]] = [None] * m
    plus_di_arr: list[Optional[float]] = [None] * m
    minus_di_arr: list[Optional[float]] = [None] * m

    def _di_val(smoothed_dm: float, smoothed_tr: float) -> float:
        return 100.0 * smoothed_dm / smoothed_tr if smoothed_tr != 0 else 0.0

    def _dx(pdi: float, mdi: float) -> float:
        denom = pdi + mdi
        return 100.0 * abs(pdi - mdi) / denom if denom != 0 else 0.0

    # First values at index period-1
    pdi = _di_val(smooth_pdm, smooth_tr)
    mdi = _di_val(smooth_mdm, smooth_tr)
    plus_di_arr[period - 1] = pdi
    minus_di_arr[period - 1] = mdi

    dx_values: list[float] = [_dx(pdi, mdi)]

    for i in range(period, m):
        smooth_tr = smooth_tr - smooth_tr / period + tr_list[i]
        smooth_pdm = smooth_pdm - smooth_pdm / period + plus_dm[i]
        smooth_mdm = smooth_mdm - smooth_mdm / period + minus_dm[i]

        pdi = _di_val(smooth_pdm, smooth_tr)
        mdi = _di_val(smooth_mdm, smooth_tr)
        plus_di_arr[i] = pdi
        minus_di_arr[i] = mdi
        dx_values.append(_dx(pdi, mdi))

    # ADX = Wilder smooth of DX over period
    # dx_values has length (m - period + 1)
    n_dx = len(dx_values)
    if n_dx >= period:
        smooth_dx = sum(dx_values[:period]) / period
        # First ADX value maps to index (period - 1 + period - 1) = 2*period - 2 in m-space
        adx_start_m = 2 * period - 2
        if adx_start_m < m:
            adx_arr[adx_start_m] = smooth_dx

        for j in range(period, n_dx):
            smooth_dx = (smooth_dx * (period - 1) + dx_values[j]) / period
            m_idx = period - 1 + j
            if m_idx < m:
                adx_arr[m_idx] = smooth_dx

    # Shift results: m-arrays are offset by 1 from n-arrays (index 0 in m = index 1 in n)
    adx_result: list[Optional[Decimal]] = [None] * n
    plus_di_result: list[Optional[Decimal]] = [None] * n
    minus_di_result: list[Optional[Decimal]] = [None] * n

    for i in range(m):
        n_idx = i + 1
        if adx_arr[i] is not None:
            adx_result[n_idx] = Decimal(str(round(adx_arr[i], 4)))  # type: ignore[arg-type]
        if plus_di_arr[i] is not None:
            plus_di_result[n_idx] = Decimal(str(round(plus_di_arr[i], 4)))  # type: ignore[arg-type]
        if minus_di_arr[i] is not None:
            minus_di_result[n_idx] = Decimal(str(round(minus_di_arr[i], 4)))  # type: ignore[arg-type]

    return adx_result, plus_di_result, minus_di_result


# ---------------------------------------------------------------------------
# MFI (Money Flow Index)
# ---------------------------------------------------------------------------


def compute_mfi(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
    period: int = 14,
) -> list[Optional[Decimal]]:
    """Compute Money Flow Index.

    Formula:
        typical_price = (high + low + close) / 3
        raw_money_flow = typical_price * volume
        positive_flow = sum of raw_mf where typical_price > prev_typical_price (period)
        negative_flow = sum of raw_mf where typical_price <= prev_typical_price (period)
        MFI = 100 - 100 / (1 + money_flow_ratio)

    Args:
        highs, lows, closes, volumes: OHLCV arrays.
        period: MFI period (default 14).

    Returns:
        List of Decimal MFI values.
    """
    n = len(closes)
    if n != len(highs) or n != len(lows) or n != len(volumes):
        return [None] * n

    result: list[Optional[Decimal]] = [None] * n

    if n < period + 1:
        return result

    # Compute typical prices and raw money flows
    tp = [(highs[i] + lows[i] + closes[i]) / 3.0 for i in range(n)]
    rmf = [tp[i] * volumes[i] for i in range(n)]

    for i in range(period, n):
        pos_flow = 0.0
        neg_flow = 0.0
        for j in range(i - period + 1, i + 1):
            if j == 0:
                continue
            if tp[j] > tp[j - 1]:
                pos_flow += rmf[j]
            else:
                neg_flow += rmf[j]

        if neg_flow == 0.0:
            # All flow is positive — skip this day per convention
            continue
        else:
            mf_ratio = pos_flow / neg_flow
            mfi_val = 100.0 - 100.0 / (1.0 + mf_ratio)

        result[i] = Decimal(str(round(mfi_val, 4)))

    return result


# ---------------------------------------------------------------------------
# Bollinger Bands
# ---------------------------------------------------------------------------


def compute_bollinger(
    prices: list[float],
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[list[Optional[Decimal]], list[Optional[Decimal]], list[Optional[Decimal]]]:
    """Compute Bollinger Bands.

    Formula:
        middle = SMA(period)
        upper = middle + num_std * std(prices[-period:])
        lower = middle - num_std * std(prices[-period:])

    Args:
        prices: List of close prices.
        period: Period for SMA and standard deviation (default 20).
        num_std: Number of standard deviations (default 2).

    Returns:
        Tuple of (upper, middle, lower) as Decimal lists.
    """
    n = len(prices)
    upper: list[Optional[Decimal]] = [None] * n
    middle: list[Optional[Decimal]] = [None] * n
    lower: list[Optional[Decimal]] = [None] * n

    if n < period:
        return upper, middle, lower

    sma_vals = compute_sma(prices, period)

    for i in range(period - 1, n):
        if sma_vals[i] is None:
            continue
        mid = float(sma_vals[i])  # type: ignore[arg-type]
        window = prices[i - period + 1 : i + 1]
        std = float(np.std(window, ddof=0))
        u = mid + num_std * std
        lo = mid - num_std * std
        upper[i] = Decimal(str(round(u, 4)))
        middle[i] = sma_vals[i]
        lower[i] = Decimal(str(round(lo, 4)))

    return upper, middle, lower


# ---------------------------------------------------------------------------
# Rate of Change
# ---------------------------------------------------------------------------


def compute_roc(prices: list[float], period: int = 10) -> list[Optional[Decimal]]:
    """Compute Rate of Change (percentage).

    Formula: ROC = ((price[i] - price[i-period]) / price[i-period]) * 100

    Args:
        prices: List of close prices.
        period: Look-back period (default 10).

    Returns:
        List of Decimal ROC values.
    """
    n = len(prices)
    result: list[Optional[Decimal]] = [None] * n

    for i in range(period, n):
        prev = prices[i - period]
        if prev == 0.0:
            continue
        roc_val = ((prices[i] - prev) / prev) * 100.0
        result[i] = Decimal(str(round(roc_val, 4)))

    return result


# ---------------------------------------------------------------------------
# Volatility (annualised)
# ---------------------------------------------------------------------------


def compute_volatility(prices: list[float], trading_days: int = 252) -> Optional[Decimal]:
    """Compute annualised historical volatility.

    Formula: std(daily_returns) * sqrt(trading_days) * 100

    Args:
        prices: List of close prices (at least 2 values).
        trading_days: Number of trading days per year (default 252).

    Returns:
        Annualised volatility as Decimal percentage, or None if insufficient data.
    """
    if len(prices) < 2:
        return None

    returns = [(prices[i] / prices[i - 1]) - 1.0 for i in range(1, len(prices))]
    arr = np.array(returns, dtype=float)
    std = float(np.std(arr, ddof=1))
    vol = std * math.sqrt(trading_days) * 100.0
    return Decimal(str(round(vol, 4)))


# ---------------------------------------------------------------------------
# Beta
# ---------------------------------------------------------------------------


def compute_beta(
    asset_returns: list[float],
    benchmark_returns: list[float],
) -> Optional[Decimal]:
    """Compute beta of asset vs benchmark.

    Formula: beta = Cov(asset, benchmark) / Var(benchmark)

    Args:
        asset_returns: Daily returns of the asset.
        benchmark_returns: Daily returns of the benchmark (same length).

    Returns:
        Beta as Decimal, or None if insufficient data.
    """
    if len(asset_returns) != len(benchmark_returns) or len(asset_returns) < 2:
        return None

    asset_arr = np.array(asset_returns, dtype=float)
    bench_arr = np.array(benchmark_returns, dtype=float)

    bench_var = float(np.var(bench_arr, ddof=1))
    if bench_var == 0.0:
        return None

    cov = float(np.cov(asset_arr, bench_arr, ddof=1)[0][1])
    beta = cov / bench_var
    return Decimal(str(round(beta, 4)))


# ---------------------------------------------------------------------------
# Sharpe Ratio
# ---------------------------------------------------------------------------


def compute_sharpe(
    returns: list[float],
    risk_free_rate: float = 0.0,
    trading_days: int = 252,
) -> Optional[Decimal]:
    """Compute annualised Sharpe Ratio.

    Formula: Sharpe = (mean_return - risk_free_rate) / std_return * sqrt(trading_days)

    Args:
        returns: List of daily returns.
        risk_free_rate: Daily risk-free rate (default 0.0).
        trading_days: Trading days per year (default 252).

    Returns:
        Sharpe ratio as Decimal, or None if insufficient data.
    """
    if len(returns) < 2:
        return None

    arr = np.array(returns, dtype=float)
    mean_ret = float(np.mean(arr)) - risk_free_rate
    std_ret = float(np.std(arr, ddof=1))

    if std_ret == 0.0:
        return None

    sharpe = (mean_ret / std_ret) * math.sqrt(trading_days)
    return Decimal(str(round(sharpe, 4)))


# ---------------------------------------------------------------------------
# Sortino Ratio
# ---------------------------------------------------------------------------


def compute_sortino(
    returns: list[float],
    target_return: float = 0.0,
    trading_days: int = 252,
) -> Optional[Decimal]:
    """Compute annualised Sortino Ratio.

    Formula: Sortino = (mean_return - target) / downside_deviation * sqrt(trading_days)
    Downside deviation uses only returns below target_return.

    Args:
        returns: List of daily returns.
        target_return: Target/minimum acceptable return (default 0.0).
        trading_days: Trading days per year (default 252).

    Returns:
        Sortino ratio as Decimal, or None if insufficient data.
    """
    if len(returns) < 2:
        return None

    arr = np.array(returns, dtype=float)
    mean_ret = float(np.mean(arr)) - target_return
    downside = arr[arr < target_return] - target_return

    if len(downside) == 0:
        return None

    downside_std = float(np.sqrt(np.mean(downside**2)))

    if downside_std == 0.0:
        return None

    sortino = (mean_ret / downside_std) * math.sqrt(trading_days)
    return Decimal(str(round(sortino, 4)))


# ---------------------------------------------------------------------------
# Maximum Drawdown
# ---------------------------------------------------------------------------


def compute_max_drawdown(prices: list[float]) -> Optional[Decimal]:
    """Compute maximum drawdown percentage.

    Formula: max drawdown = min((price[i] - peak[i]) / peak[i]) * 100
    where peak[i] = max(prices[0..i])

    Args:
        prices: List of close prices.

    Returns:
        Max drawdown as Decimal percentage (negative value), or None.
    """
    if len(prices) < 2:
        return None

    peak = prices[0]
    max_dd = 0.0

    for p in prices[1:]:
        if p > peak:
            peak = p
        if peak > 0:
            dd = (p - peak) / peak
            if dd < max_dd:
                max_dd = dd

    return Decimal(str(round(max_dd * 100.0, 4)))


# ---------------------------------------------------------------------------
# On-Balance Volume
# ---------------------------------------------------------------------------


def compute_obv(closes: list[float], volumes: list[float]) -> list[Optional[Decimal]]:
    """Compute On-Balance Volume.

    Formula:
        OBV[0] = volume[0]
        OBV[i] = OBV[i-1] + volume[i] if close[i] > close[i-1]
        OBV[i] = OBV[i-1] - volume[i] if close[i] < close[i-1]
        OBV[i] = OBV[i-1] if close[i] == close[i-1]

    Args:
        closes: List of close prices.
        volumes: List of volumes (same length).

    Returns:
        List of Decimal OBV values.
    """
    n = len(closes)
    if n == 0 or n != len(volumes):
        return []

    result: list[Optional[Decimal]] = [None] * n
    obv = volumes[0]
    result[0] = Decimal(str(round(obv, 4)))

    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            obv += volumes[i]
        elif closes[i] < closes[i - 1]:
            obv -= volumes[i]
        result[i] = Decimal(str(round(obv, 4)))

    return result


# ---------------------------------------------------------------------------
# Relative Volume
# ---------------------------------------------------------------------------


def compute_relative_volume(
    volumes: list[float],
    period: int = 20,
) -> list[Optional[Decimal]]:
    """Compute relative volume (current volume / average volume over period).

    Formula: RelVol[i] = volume[i] / SMA(volume, period)[i]

    Args:
        volumes: List of volume values.
        period: Look-back period for average volume (default 20).

    Returns:
        List of Decimal relative volume values; None before period.
    """
    n = len(volumes)
    result: list[Optional[Decimal]] = [None] * n

    if n < period:
        return result

    sma_vols = compute_sma(volumes, period)

    for i in range(period - 1, n):
        if sma_vols[i] is None or float(sma_vols[i]) == 0.0:  # type: ignore[arg-type]
            continue
        rel_vol = volumes[i] / float(sma_vols[i])  # type: ignore[arg-type]
        result[i] = Decimal(str(round(rel_vol, 4)))

    return result
