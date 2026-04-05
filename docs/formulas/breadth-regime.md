# Market Breadth & Regime — Formula Reference

**Source:** MarketPulse (fie2) `services/breadth_engine.py`, `services/sentiment_engine.py`

## Market Regime Classification

```
Drawdown = (current_price - 52_week_high) / 52_week_high × 100

BEAR:       drawdown < -15%
CORRECTION: drawdown -8% to -15%, OR (below 50DMA AND 3M return < -5%)
CAUTIOUS:   below 50DMA but shallow drawdown (< 8%)
BULL:       above 50DMA and drawdown < 8%
```

**Source:** `compass_rs.py:208-268`

## Daily Breadth Metrics (6 indicators)

For the entire universe of stocks, count how many satisfy each condition:

| # | Metric | Condition |
|---|--------|-----------|
| 1 | EMA(21) breadth | close > 21-period EMA |
| 2 | EMA(200) breadth | close > 200-period EMA |
| 3 | RSI deterioration | RSI(14) < 40 |
| 4 | RSI oversold | RSI(14) < 30 |
| 5 | 52-week high | close >= 52W_high × 0.98 (within 2%) |
| 6 | 52-week low | close <= 52W_low × 1.02 (within 2%) |

**Source:** `breadth_engine.py:199-272`

## Monthly Breadth Metrics (6 indicators)

| # | Metric | Condition |
|---|--------|-----------|
| 1 | Monthly 12M EMA | monthly_close > 12-month EMA |
| 2 | Prev month high | close > previous month's high |
| 3 | Prev quarter high | close > previous quarter's high |
| 4 | Prev year high | close > previous year's high |
| 5 | Monthly RSI < 50 | monthly RSI < 50 |
| 6 | Monthly RSI < 40 | monthly RSI < 40 |

**Source:** `breadth_engine.py:277-354`

## Zone Classification

```
Standard Zones (for count-based metrics):
  < 50:   Extreme Fear
  < 100:  Fear Activated
  < 150:  Deterioration
  < 200:  Neutral
  < 300:  Entering Green
  >= 300: Bull Run

For RSI-based metrics (inverted — higher count = worse):
  Invert the count before zone classification
```

**Source:** `breadth_engine.py:53-88`

## 5-Layer Sentiment Composite

```
Layer Weights:
  short_term:  0.20  (7 metrics)
  broad_trend: 0.30  (7 metrics)
  adv_decline: 0.25  (3 metrics)
  momentum:    0.15  (3 metrics)
  extremes:    0.10  (2 metrics)

Per-metric score:
  pct = (count / total) × 100
  threshold = 50
  if inverted: pct = 100 - pct, threshold = 100 - threshold
  score = min(100, max(0, (pct / threshold) × 60 + max(0, pct - threshold) × 2))

Layer_Score = average(all metric scores in layer)
Composite = sum(Layer_Score[k] × weight[k]) for all layers
```

**Zone:**
```
< 30:  Bear
< 45:  Weak
< 55:  Neutral
< 70:  Bullish
>= 70: Strong
```

**Source:** `sentiment_engine.py:28-334`

## Per-Stock Sentiment (22 Metrics, 5 Layers)

### Layer 1: Short-Term (weight 0.20, 4 metrics)
- Close > 10 EMA
- Close > 21 EMA
- RSI(14) > 50
- Close > Previous Day High

### Layer 2: Broad Trend (weight 0.30, 5 metrics)
- Close > 50 EMA
- Close > 200 EMA
- Golden Cross: SMA(50) > SMA(200)
- Close > Prev Month High
- Weekly RSI > 50

### Layer 3: A/D Proxy (weight 0.25, 3 metrics)
- Close > 20 SMA
- Close near high: (close - low) / (high - low) > 0.7
- Volume > 20-day average volume

### Layer 4: Momentum (weight 0.15, 4 metrics)
- MACD bullish cross
- 10-day ROC > 0
- Momentum positive: close[-1] > close[-11]
- ADX(14) > 25

### Layer 5: Extremes (weight 0.10, 3 metrics)
- Within 2% of 52-week high
- Bollinger above middle
- MFI(14) > 50
- (Subtract if within 2% of 52-week low)

```
Per-layer pass_rate = metrics_passing / total_metrics_in_layer
Stock_Composite = sum(pass_rate[layer] × weight[layer]) × 100
```

**Source:** `stock_metrics.py:32-273`

## Edge Cases
- Missing data for any metric: skip that metric, reduce denominator
- All counts and scores in Decimal
- RSI/breadth zones are count-based, not percentage-based
- Inverted metrics: higher count = bearish signal
