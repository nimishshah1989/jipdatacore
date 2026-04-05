# Technical Indicators — Formula Reference

**Source:** MarketPulse (fie2) `services/technical.py`, `services/stock_metrics.py`

## EMA (Exponential Moving Average)

```
k = 2 / (period + 1)
EMA[0] = SMA of first N values
EMA[i] = price[i] × k + EMA[i-1] × (1 - k)
```

**Standard periods:** 10, 21, 50, 200
**Source:** `technical.py:13-43`

## SMA (Simple Moving Average)

```
SMA[t] = sum(close[t-N+1 : t+1]) / N
```

**Standard periods:** 20 (Bollinger), 50, 200
**Golden Cross:** SMA(50) > SMA(200)

## RSI (Relative Strength Index) — Wilder Smoothing

```
gains = [max(0, close[i] - close[i-1]) for each day]
losses = [max(0, close[i-1] - close[i]) for each day]

avg_gain[0] = sum(gains[:period]) / period
avg_loss[0] = sum(losses[:period]) / period

For i >= period:
  avg_gain[i] = (avg_gain[i-1] × (period-1) + gains[i]) / period
  avg_loss[i] = (avg_loss[i-1] × (period-1) + losses[i]) / period

RS = avg_gain / avg_loss
RSI = 100 - (100 / (1 + RS))
```

**Default period:** 14
**Output range:** 0-100
**Source:** `technical.py:48-72`

## ADX (Average Directional Index)

```
+DM = max(high[i] - high[i-1], 0) if high_diff > low_diff else 0
-DM = max(low[i-1] - low[i], 0) if low_diff > high_diff else 0
TR = max(high-low, |high-prev_close|, |low-prev_close|)

+DI = Wilder_Smooth(+DM) / Wilder_Smooth(TR) × 100
-DI = Wilder_Smooth(-DM) / Wilder_Smooth(TR) × 100
DX = |+DI - -DI| / (+DI + -DI) × 100
ADX = Wilder_Smooth(DX) over 14 periods
```

**Default period:** 14
**Trend threshold:** ADX > 25 indicates trending
**Source:** `technical.py:77-142`

## MFI (Money Flow Index)

```
Typical_Price = (high + low + close) / 3
Raw_Money_Flow = typical_price × volume

For each period window:
  pos_flow = sum of flows where TP[i] > TP[i-1]
  neg_flow = sum of flows where TP[i] < TP[i-1]

MF_Ratio = pos_flow / neg_flow
MFI = 100 - (100 / (1 + MF_Ratio))
```

**Default period:** 14
**Output range:** 0-100
**Source:** `technical.py:147-179`

## MACD

```
MACD_Line = EMA(12) - EMA(26)
Signal_Line = EMA(9) of MACD_Line
MACD_Histogram = MACD_Line - Signal_Line
Bullish_Cross = MACD_Line > Signal_Line (was previously below)
```

**Source:** `stock_metrics.py:215-227`

## Bollinger Bands

```
Middle = SMA(20)
Upper = Middle + 2 × std(close, 20)
Lower = Middle - 2 × std(close, 20)
Above_Mid = close > Middle
```

**Source:** `stock_metrics.py:259-260`

## Rate of Change (ROC)

```
ROC_10d = (close[-1] / close[-11] - 1) × 100
```

**Source:** `stock_metrics.py:230`

## Edge Cases
- Insufficient data for period: return None
- Division by zero (avg_loss = 0 for RSI): RSI = 100
- Volume = 0 for MFI: skip day
- All computations in Decimal, never float
