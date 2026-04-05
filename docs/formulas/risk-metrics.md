# Risk & Return Metrics — Formula Reference

**Source:** MarketPulse (fie2) `services/pms_service.py`

## Time-Weighted Return (TWR) — Unit NAV

```
NAV_BASE = 100.0

unit_nav[0] = 100 × (nav[0] / corpus[0])

For i >= 1:
  cash_flow = corpus[i] - corpus[i-1]
  adjusted_prev = nav[i-1] + cash_flow
  daily_return = nav[i] / adjusted_prev
  unit_nav[i] = unit_nav[i-1] × daily_return
```

**Source:** `pms_service.py:34-98`

## Cumulative Return

```
cumulative_return = ((final_nav / start_nav) - 1) × 100
```

## CAGR (Compound Annual Growth Rate)

```
years = (end_date - start_date).days / 365.25
cagr = ((final_nav / start_nav) ^ (1 / years) - 1) × 100
```

Only calculated if data spans > 1 year.

## Volatility (Annualized)

```
daily_returns = [(nav[i] - nav[i-1]) / nav[i-1] for each day]
volatility = stdev(daily_returns) × sqrt(252) × 100
```

**Trading days per year:** 252

## Maximum Drawdown

```
For each nav value:
  if nav > peak: peak = nav
  drawdown = (peak - nav) / peak
  max_dd = max(max_dd, drawdown)
```

**Drawdown event threshold:** >= 2%

## Sharpe Ratio

```
risk_free_rate = 0.07 (7% annualized — RBI benchmark for India)
daily_rf = risk_free_rate / 252

daily_returns = nav.pct_change()
excess_returns = daily_returns - daily_rf

sharpe = (mean(excess_returns) / std(excess_returns)) × sqrt(252)
```

**Note:** fie2 uses 7% RF for India (not 4% like global-pulse)

## Sortino Ratio

```
downside_returns = daily_returns[daily_returns < daily_rf]
sortino = (mean(daily_returns) - daily_rf) / std(downside_returns) × sqrt(252)
```

## Calmar Ratio

```
calmar = CAGR / |max_drawdown|
```

## Rolling Returns

```
For each period (1M, 3M, 6M, 1Y, 2Y, 3Y):
  rolling_return[t] = ((nav[t] / nav[t - period]) - 1) × 100
```

**Period definitions:**
| Period | Calendar days |
|--------|-------------|
| 1M | 30 |
| 3M | 91 |
| 6M | 182 |
| 1Y | 365 |
| 2Y | 730 |
| 3Y | 1095 |

## Constants

```python
RISK_FREE_RATE = Decimal("0.07")      # 7% — RBI benchmark
TRADING_DAYS_PER_YEAR = 252
TRADING_DAYS_PER_WEEK = 5
DRAWDOWN_THRESHOLD = Decimal("0.02")  # 2% minimum
NAV_BASE = Decimal("100")
```

## Edge Cases
- Insufficient data for CAGR: return None
- Zero volatility: Sharpe = 0
- No downside returns: Sortino = None
- Zero max drawdown: Calmar = None
- All values in Decimal, never float
- Indian context: RF = 7%, not US 4%
