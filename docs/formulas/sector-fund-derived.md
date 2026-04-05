# Sector & Fund Derived Metrics — Formula Reference

**Source:** MarketPulse (fie2) `services/compass_sector_intel.py`, `services/compass_rs.py`

## Sector RS (Weighted)

```
For each sector:
  sector_rs = weighted_avg(stock_rs_scores, weights=market_cap_weights)

  Where:
    stock_rs_scores = RS scores of all stocks in sector
    market_cap_weights = market_cap[stock] / sum(market_cap[all stocks in sector])
```

## Sector Rotation

Apply the same quadrant classification at sector level:
```
If sector_rs > 0 AND sector_momentum > 0:  LEADING
If sector_rs > 0 AND sector_momentum <= 0: WEAKENING
If sector_rs <= 0 AND sector_momentum > 0: IMPROVING
If sector_rs <= 0 AND sector_momentum <= 0: LAGGING
```

## Sector Breadth

```
For each sector:
  pct_above_200ema = count(stocks where close > 200 EMA) / total_stocks × 100
  pct_above_50ema = count(stocks where close > 50 EMA) / total_stocks × 100
  pct_positive_rs = count(stocks where rs_score > 0) / total_stocks × 100
```

## Core Sectors (India)

```
BANKNIFTY, NIFTYIT, NIFTYPHARMA, NIFTYFMCG, NIFTYAUTO,
NIFTYMETAL, NIFTYENERGY, NIFTYREALTY, NIFTYINFRA, NIFTYMEDIA,
NIFTYPSUBANK, NIFTYPVTBANK, NIFTYFINANCE
```

## MF Category Rank

```
For each MF in category:
  category_rank = percentile_rank(nav_return_1y, within_category)

  percentile_rank = (count_below / (total - 1)) × 100
```

**Ranking periods:** 1M, 3M, 6M, 1Y, 3Y, 5Y

## MF Rolling Returns

```
For each scheme:
  rolling_return_nM = ((nav[today] / nav[today - n months]) - 1) × 100
```

## MF Risk-Adjusted Returns

```
sharpe = (annualized_return - risk_free_rate) / annualized_volatility
sortino = (annualized_return - risk_free_rate) / downside_deviation

risk_free_rate = 0.07  # 7% for India
```

## Fund Flow Analysis

```
For each category:
  net_flow = sum(buy_value - sell_value) for all funds in category
  flow_trend = sign(net_flow[current_month] - net_flow[prev_month])
```

## Edge Cases
- Sectors with < 3 stocks: flag as insufficient, don't rank
- MF categories with < 5 schemes: percentile rank less meaningful
- New funds (< 1Y data): skip CAGR/Sharpe, show only absolute return
- All values in Decimal
- Market cap weights: normalize so sum = 1.0
