# JIP Data Engine — Calculation Methodology

> Complete reference for every computed metric, its formula, data sources, and output table/column.

---

## Table of Contents

1. [Technical Indicators (Stock-Level)](#1-technical-indicators)
2. [Relative Strength (RS) Scores](#2-relative-strength-rs-scores)
3. [Market Breadth](#3-market-breadth)
4. [Market Regime](#4-market-regime)
5. [Mutual Fund Risk Metrics](#5-mutual-fund-risk-metrics)
6. [Mutual Fund Derived Metrics](#6-mutual-fund-derived-metrics)
7. [Sector Metrics](#7-sector-metrics)
8. [NAV Returns & Range](#8-nav-returns--range)
9. [Corporate Action Adjustments](#9-corporate-action-adjustments)
10. [Data Sources Summary](#10-data-sources-summary)

---

## 1. Technical Indicators

**Output table:** `de_equity_technical_daily`
**Primary key:** `(date, instrument_id)`
**Input table:** `de_equity_ohlcv` (columns: date, instrument_id, close, close_adj, open, high, low, volume, delivery_pct)
**Computation method:** SQL window functions for SMA; vectorized pandas groupby().transform() for EMA/RSI/MACD/ADX

### 1.1 Simple Moving Averages (SMA)

| Column | Formula | Method |
|--------|---------|--------|
| `sma_50` | AVG(close_adj) over last 50 trading days | SQL: `AVG(close_adj) OVER (PARTITION BY instrument_id ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)` |
| `sma_200` | AVG(close_adj) over last 200 trading days | Same window, 199 preceding |
| `above_50dma` | close_adj > sma_50 | PostgreSQL GENERATED ALWAYS column |
| `above_200dma` | close_adj > sma_200 | PostgreSQL GENERATED ALWAYS column |

**Null handling:** SMA50 is NULL for first 49 rows per instrument. SMA200 is NULL for first 199 rows.

### 1.2 Exponential Moving Averages (EMA)

| Column | Formula | Smoothing Factor |
|--------|---------|-----------------|
| `ema_10` | EMA(close, span=10) | α = 2/(10+1) = 0.1818 |
| `ema_20` | EMA(close, span=20) | α = 2/(20+1) = 0.0952 |
| `ema_21` | EMA(close, span=21) | α = 2/(21+1) = 0.0909 |
| `ema_50` | EMA(close, span=50) | α = 2/(50+1) = 0.0392 |
| `ema_200` | EMA(close, span=200) | α = 2/(200+1) = 0.00995 |

**Recursive formula:**
```
EMA_today = α × Price_today + (1 - α) × EMA_yesterday
where α = 2 / (span + 1)
```

**Implementation:** `pandas.groupby('instrument_id')['close'].transform(lambda x: x.ewm(span=N, adjust=False).mean())`

### 1.3 RSI (Relative Strength Index)

| Column | Period | Smoothing |
|--------|--------|-----------|
| `rsi_7` | 7 | Wilder's (α = 1/7) |
| `rsi_9` | 9 | Wilder's (α = 1/9) |
| `rsi_14` | 14 | Wilder's (α = 1/14) |
| `rsi_21` | 21 | Wilder's (α = 1/21) |

**Formula:**
```
delta = close_today - close_yesterday
gain = max(delta, 0)
loss = max(-delta, 0)
avg_gain = EWM(gain, alpha=1/period)
avg_loss = EWM(loss, alpha=1/period)
RS = avg_gain / avg_loss
RSI = 100 - (100 / (1 + RS))
```

**Implementation:** Wilder's smoothing via `pandas.ewm(alpha=1/14, adjust=False).mean()` — NOT standard EMA.

### 1.4 MACD (Moving Average Convergence Divergence)

| Column | Formula |
|--------|---------|
| `macd_line` | EMA(close, 12) - EMA(close, 26) |
| `macd_signal` | EMA(macd_line, 9) |
| `macd_histogram` | macd_line - macd_signal |

**Parameters:** Standard 12/26/9.

### 1.5 Rate of Change (ROC)

| Column | Formula |
|--------|---------|
| `roc_5` | (close / close_5_days_ago - 1) × 100 |
| `roc_10` | (close / close_10_days_ago - 1) × 100 |
| `roc_21` | (close / close_21_days_ago - 1) × 100 |
| `roc_63` | (close / close_63_days_ago - 1) × 100 |

**Implementation:** `(x / x.shift(N) - 1) * 100`

### 1.6 Volatility

| Column | Formula |
|--------|---------|
| `volatility_20d` | STDDEV(daily_return, 20 days) × √252 × 100 |
| `volatility_60d` | STDDEV(daily_return, 60 days) × √252 × 100 |

**Daily return:** `close_today / close_yesterday - 1`
**Annualization:** Multiply by √252 (trading days per year)
**Unit:** Percentage (e.g., 15.0 = 15% annualized volatility)

### 1.7 Bollinger Bands

| Column | Formula |
|--------|---------|
| `bollinger_upper` | SMA(close, 20) + 2 × STDDEV(close, 20) |
| `bollinger_lower` | SMA(close, 20) - 2 × STDDEV(close, 20) |

### 1.8 Relative Volume

| Column | Formula |
|--------|---------|
| `relative_volume` | volume_today / AVG(volume, 20 days) |

**Interpretation:** >1.0 means above-average volume. >2.0 is significant.

### 1.9 Beta vs NIFTY 50

| Column | Formula |
|--------|---------|
| `beta_nifty` | REGR_SLOPE(stock_daily_return, nifty_daily_return) over 252 days |

**Input:** Stock daily returns and NIFTY 50 daily returns, aligned by date.
**Window:** Rolling 252 trading days (1 year).
**Implementation (SQL):** `REGR_SLOPE(stock_ret, nifty_ret) OVER (PARTITION BY instrument_id ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)`

### 1.10 Sharpe Ratio (Stock-Level)

| Column | Formula |
|--------|---------|
| `sharpe_1y` | (AVG(daily_return, 252d) - RF) / STDDEV(daily_return, 252d) × √252 |

**Risk-free rate:** RF = 7% annual = 0.07/252 daily = 0.0002778
**Minimum observations:** 200 trading days required, else NULL.

### 1.11 Sortino Ratio (Stock-Level)

| Column | Formula |
|--------|---------|
| `sortino_1y` | (AVG(daily_return, 252d) - RF) / STDDEV(downside_returns, 252d) × √252 |

**Downside returns:** Only negative daily returns (gains excluded from denominator).

### 1.12 Max Drawdown (Stock-Level)

| Column | Formula |
|--------|---------|
| `max_drawdown_1y` | MIN((close - rolling_peak) / rolling_peak) over 252 days × 100 |

**Rolling peak:** MAX(close) over the lookback window.
**Unit:** Percentage (always negative, e.g., -15.0 = 15% drawdown).

### 1.13 Calmar Ratio

| Column | Formula |
|--------|---------|
| `calmar_ratio` | Annualized return / |Max Drawdown| |

### 1.14 OBV (On-Balance Volume)

| Column | Formula |
|--------|---------|
| `obv` | Cumulative SUM of: +volume if close > prev_close, -volume if close < prev_close, 0 if equal |

### 1.15 MFI (Money Flow Index)

| Column | Formula |
|--------|---------|
| `mfi_14` | 100 - (100 / (1 + Money Flow Ratio)) |

```
Typical Price = (High + Low + Close) / 3
Raw Money Flow = Typical Price × Volume
Positive MF = SUM(Raw MF where TP > prev_TP, 14 days)
Negative MF = SUM(Raw MF where TP < prev_TP, 14 days)
Money Flow Ratio = Positive MF / Negative MF
```

### 1.16 ADX (Average Directional Index)

| Column | Formula |
|--------|---------|
| `adx_14` | EWM(DX, alpha=1/14) |
| `plus_di` | (+DM14 / ATR14) × 100 |
| `minus_di` | (-DM14 / ATR14) × 100 |

```
True Range = MAX(H-L, |H-prev_C|, |L-prev_C|)
+DM = H - prev_H (if positive and > prev_L - L, else 0)
-DM = prev_L - L (if positive and > H - prev_H, else 0)
ATR14 = EWM(TR, alpha=1/14)
+DM14 = EWM(+DM, alpha=1/14)
-DM14 = EWM(-DM, alpha=1/14)
DX = |(+DI - -DI)| / (+DI + -DI) × 100
ADX = EWM(DX, alpha=1/14)
```

### 1.17 Delivery Analysis

| Column | Formula |
|--------|---------|
| `delivery_vs_avg` | delivery_pct_today / AVG(delivery_pct, 20 days) |

**Note:** delivery_pct only available for 2025-2026 (96-100% coverage). Earlier dates will be NULL.

---

## 2. Relative Strength (RS) Scores

**Output table:** `de_rs_scores`
**Primary key:** `(date, entity_type, entity_id, vs_benchmark)`
**Entity types:** `equity` (stocks), `mf` (mutual funds), `sector`
**Benchmarks:** NIFTY 50, NIFTY 500, NIFTY MIDCAP 100

### 2.1 RS Score per Lookback

| Column | Lookback (trading days) | Weight in Composite |
|--------|------------------------|---------------------|
| `rs_1w` | 5 | 10% |
| `rs_1m` | 21 | 20% |
| `rs_3m` | 63 | 30% |
| `rs_6m` | 126 | 25% |
| `rs_12m` | 252 | 15% |

**Formula:**
```
cumreturn_entity_N = close_today / close_N_days_ago - 1
cumreturn_bench_N = bench_close_today / bench_close_N_days_ago - 1
rolling_std_bench_N = STDDEV(bench_daily_returns, N days)

RS_N = (cumreturn_entity_N - cumreturn_bench_N) / rolling_std_bench_N
```

### 2.2 RS Composite

```
rs_composite = rs_1w × 0.10 + rs_1m × 0.20 + rs_3m × 0.30 + rs_6m × 0.25 + rs_12m × 0.15
```

If any lookback is NULL (insufficient history), the composite uses available components normalized by their total weight.

### 2.3 Data Sources

| Entity Type | Price Source | Price Column |
|------------|-------------|--------------|
| equity | `de_equity_ohlcv` | COALESCE(close_adj, close) |
| mf | `de_mf_nav_daily` | nav |
| sector | Aggregated from equity RS | AVG(rs_composite) grouped by sector |

**Benchmark source:** `de_index_prices` (column: close, key: index_code)

**Implementation:** Pure SQL with window functions (LAG, STDDEV) — zero Python memory.

---

## 3. Market Breadth

**Output table:** `de_breadth_daily`
**Primary key:** `date`
**Input:** `de_equity_ohlcv`, `de_equity_technical_daily`

| Column | Formula |
|--------|---------|
| `advance` | COUNT of stocks where close_today > close_yesterday |
| `decline` | COUNT of stocks where close_today < close_yesterday |
| `unchanged` | COUNT of stocks where close_today = close_yesterday |
| `total_stocks` | advance + decline + unchanged |
| `ad_ratio` | advance / decline |
| `pct_above_200dma` | (COUNT where above_200dma = TRUE / total) × 100 |
| `pct_above_50dma` | (COUNT where above_50dma = TRUE / total) × 100 |

---

## 4. Market Regime

**Output table:** `de_market_regime`
**Primary key:** `computed_at`
**Input:** `de_breadth_daily`

### 4.1 Breadth Score

```
adv_pct = advance / total_stocks × 100
ad_normalized = CLAMP(50 + (ad_ratio - 1) × 25, 0, 100)
breadth_score = adv_pct × 0.5 + ad_normalized × 0.5
```

### 4.2 Classification

| Regime | Condition |
|--------|-----------|
| BULL | breadth_score >= 60 |
| BEAR | breadth_score <= 40 |
| SIDEWAYS | 40 < breadth_score < 60 |

### 4.3 Confidence Score

```
confidence = breadth_score × 0.30 + momentum_score × 0.25 + volume_score × 0.15 + global_score × 0.15 + fii_score × 0.15
```

**Note:** Currently momentum/volume/global/fii scores default to 50 (neutral). These require FII/DII flow data and global market data to be fully populated.

---

## 5. Mutual Fund Risk Metrics

**Output table:** `de_mf_derived_daily`
**Primary key:** `(nav_date, mstar_id)`
**Input:** `de_mf_nav_daily` (nav), `de_index_prices` (NIFTY 50 close)

### 5.1 Sharpe Ratio

| Column | Window | Min Observations |
|--------|--------|-----------------|
| `sharpe_1y` | 252 days | 200 |
| `sharpe_3y` | 756 days | 600 |
| `sharpe_5y` | 1260 days | 1000 |

```
daily_return = nav_today / nav_yesterday - 1
Sharpe = (AVG(daily_return) - RF) / STDDEV(daily_return) × √252
RF = 0.07 / 252 (7% annual risk-free rate)
```

### 5.2 Sortino Ratio

| Column | Window |
|--------|--------|
| `sortino_1y` | 252 days |
| `sortino_3y` | 756 days |
| `sortino_5y` | 1260 days |

```
Sortino = (AVG(daily_return) - RF) / STDDEV(negative_returns_only) × √252
```

### 5.3 Standard Deviation (Annualized)

| Column | Window |
|--------|--------|
| `stddev_1y` | 252 days |
| `stddev_3y` | 756 days |
| `stddev_5y` | 1260 days |

```
StdDev = STDDEV(daily_return) × √252 × 100
Unit: percentage
```

### 5.4 Max Drawdown

| Column | Window |
|--------|--------|
| `max_drawdown_1y` | 252 days |
| `max_drawdown_3y` | 756 days |
| `max_drawdown_5y` | 1260 days |

```
running_peak = MAX(nav) over window
drawdown = (nav - running_peak) / running_peak
max_drawdown = MIN(drawdown) over window × 100
Unit: percentage (always negative)
```

### 5.5 Volatility

| Column | Window |
|--------|--------|
| `volatility_1y` | 252 days |
| `volatility_3y` | 756 days |

Same formula as StdDev (annualized daily return standard deviation × 100).

### 5.6 Beta vs NIFTY 50

| Column | Formula |
|--------|---------|
| `beta_vs_nifty` | REGR_SLOPE(fund_daily_return, nifty_daily_return) over 252 days |

**Date alignment:** Fund returns and NIFTY returns joined on matching dates only.

### 5.7 Treynor Ratio

| Column | Formula |
|--------|---------|
| `treynor_ratio` | Sharpe_1Y × Volatility_1Y / 100 / Beta |

---

## 6. Mutual Fund Derived Metrics

**Output table:** `de_mf_derived_daily`
**Input:** `de_mf_holdings`, `de_rs_scores`, `de_instrument`

### 6.1 Holdings-Weighted RS (Derived RS)

| Column | Formula |
|--------|---------|
| `derived_rs_composite` | SUM(weight_pct × stock_rs_composite) / SUM(weight_pct) |

**Source:** `de_mf_holdings.weight_pct` × `de_rs_scores.rs_composite` (entity_type='equity', vs_benchmark='NIFTY 50')
**Coverage:** `coverage_pct` = SUM(weight_pct where RS exists) / SUM(all weight_pct) × 100

### 6.2 NAV RS

| Column | Formula |
|--------|---------|
| `nav_rs_composite` | RS composite from `de_rs_scores` where entity_type='mf' |

This is the fund's NAV-based RS — treating the fund's NAV as a price series and computing RS vs benchmark.

### 6.3 Manager Alpha

| Column | Formula |
|--------|---------|
| `manager_alpha` | nav_rs_composite - derived_rs_composite |

**Interpretation:**
- Positive alpha: Fund manager's active bets add value beyond what the holdings would predict
- Negative alpha: Portfolio construction destroys value vs passive equivalent

---

## 7. Sector Metrics

### 7.1 Sector RS

**Output table:** `de_rs_scores` (entity_type = 'sector')

| Column | Formula |
|--------|---------|
| `rs_composite` | AVG(stock_rs_composite) for all stocks in that sector |

**Sector assignment source:** `de_instrument.sector` (mapped from NSE index constituents + yfinance)

### 7.2 Fund Sector Exposure

**Output table:** `de_mf_sector_exposure`
**Primary key:** `(mstar_id, sector)`

| Column | Formula |
|--------|---------|
| `weight_pct` | SUM(holding_weight_pct) for stocks in that sector within the fund |
| `stock_count` | COUNT(DISTINCT instrument_id) in that sector |

**Source:** `de_mf_holdings` JOIN `de_instrument` ON instrument_id, grouped by sector.

---

## 8. NAV Returns & Range

**Output table:** `de_mf_nav_daily`
**Input:** Same table (self-referencing via LAG)

### 8.1 Returns

| Column | Lookback | Formula |
|--------|----------|---------|
| `return_1d` | 1 day | (nav / LAG(nav, 1) - 1) × 100 |
| `return_1w` | 5 days | (nav / LAG(nav, 5) - 1) × 100 |
| `return_1m` | 21 days | (nav / LAG(nav, 21) - 1) × 100 |
| `return_3m` | 63 days | (nav / LAG(nav, 63) - 1) × 100 |
| `return_6m` | 126 days | (nav / LAG(nav, 126) - 1) × 100 |
| `return_1y` | 252 days | (nav / LAG(nav, 252) - 1) × 100 |
| `return_3y` | 756 days | (nav / LAG(nav, 756) - 1) × 100 |
| `return_5y` | 1260 days | (nav / LAG(nav, 1260) - 1) × 100 |
| `return_10y` | 2520 days | (nav / LAG(nav, 2520) - 1) × 100 |

**Unit:** Percentage. NULL if insufficient history.

### 8.2 52-Week Range

| Column | Formula |
|--------|---------|
| `nav_52wk_high` | MAX(nav) over last 252 trading days |
| `nav_52wk_low` | MIN(nav) over last 252 trading days |

---

## 9. Corporate Action Adjustments

**Source table:** `de_corporate_actions` (columns: instrument_id, ex_date, action_type, ratio_from, ratio_to, adj_factor)
**Target table:** `de_equity_ohlcv` (columns: close_adj, open_adj, high_adj, low_adj)

### 9.1 Adjustment Factor Computation

| Action Type | Formula |
|-------------|---------|
| **Split** (e.g., 1:10) | adj_factor = ratio_from / ratio_to = 1/10 = 0.1 |
| **Bonus** (e.g., 1:1) | adj_factor = ratio_from / (ratio_from + ratio_to) = 1/2 = 0.5 |

### 9.2 Cumulative Adjustment

```
For each instrument, sorted by ex_date DESC (most recent first):
  cumulative_factor starts at 1.0
  For each corporate action:
    cumulative_factor *= adj_factor
    All prices BEFORE ex_date: close_adj = close × cumulative_factor
  Prices ON or AFTER the last action: close_adj = close (no adjustment)
```

### 9.3 Instruments Without Corporate Actions

```
close_adj = close (identical — no adjustment needed)
```

---

## 10. Data Sources Summary

| Source Table | Key Columns Used | Used By |
|-------------|-----------------|---------|
| `de_equity_ohlcv` | close, close_adj, high, low, volume, delivery_pct | Technicals, RS, Breadth |
| `de_index_prices` | close, index_code | RS (benchmark), Beta |
| `de_mf_nav_daily` | nav, mstar_id, nav_date | MF RS, Fund Metrics, Returns |
| `de_mf_holdings` | mstar_id, instrument_id, weight_pct, is_mapped | Derived RS, Sector Exposure |
| `de_instrument` | id, sector, current_symbol | Sector aggregation, Symbol mapping |
| `de_corporate_actions` | instrument_id, ex_date, ratio_from, ratio_to | close_adj computation |

### Constants

| Constant | Value | Used In |
|----------|-------|---------|
| Risk-free rate (annual) | 7.0% | Sharpe, Sortino |
| Risk-free rate (daily) | 0.07/252 = 0.0002778 | Sharpe, Sortino |
| Trading days per year | 252 | Annualization factor |
| √252 | 15.8745 | Volatility, Sharpe annualization |

### Minimum Observation Thresholds

| Metric | Minimum Days | Reason |
|--------|-------------|--------|
| Sharpe/Sortino/StdDev 1Y | 200 | Statistical significance |
| Sharpe/Sortino/StdDev 3Y | 600 | Same |
| Sharpe/Sortino/StdDev 5Y | 1000 | Same |
| SMA 50 | 50 | Need full window |
| SMA 200 | 200 | Need full window |
| EMA 20 | 20 | Need min_periods |

---

*Last updated: 2026-04-07*
*Source: JIP Data Engine v2.0*
