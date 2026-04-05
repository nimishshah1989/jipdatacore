# RS (Relative Strength) Scores — Formula Reference

**Source:** MarketPulse (fie2) `services/compass_rs.py`

## RS Score (Raw)

```
RS_Score = (Asset_Return - Benchmark_Return) × 100

Asset_Return = ((price_now / price_period_ago) - 1) × 100
Benchmark_Return = ((benchmark_now / benchmark_period_ago) - 1) × 100
```

**Timeframes:**
| Period | Trading Days | Weight in Composite |
|--------|-------------|-------------------|
| 1M | 21 | — (not used in fie2 composite) |
| 3M | 63 | Default display period |
| 6M | 126 | — |
| 12M | 252 | — |

**Output:** Percentage points of excess return. Positive = outperforming benchmark.

## RS Momentum

```
RS_Momentum = RS_Score[today] - RS_Score[20 trading days ago]
```

**Source:** `compass_rs.py:462-481`
- 4-week (20 trading day) rate of change
- Positive momentum = RS improving
- Negative momentum = RS deteriorating

## Quadrant Classification

```
If RS_Score > 0 AND momentum > 0:  LEADING
If RS_Score > 0 AND momentum <= 0: WEAKENING
If RS_Score <= 0 AND momentum > 0: IMPROVING
If RS_Score <= 0 AND momentum <= 0: LAGGING
```

**Source:** `compass_rs.py:178-190`

## Volume Signal

```
vol_20d = avg(volumes[-20:])
vol_60d = avg(volumes[-60:])
price_now = close[-1]
price_20d_ago = close[-20]

If price UP and vol_20d > vol_60d:   ACCUMULATION
If price UP and vol_20d <= vol_60d:  WEAK_RALLY
If price DOWN and vol_20d > vol_60d: DISTRIBUTION
If price DOWN and vol_20d <= vol_60d: WEAK_DECLINE
```

**Source:** `compass_rs.py:134-175`

## Gate-Based Action Engine

```
G1 = absolute_return > 0  (stock going up?)
G2 = rs_score > 0         (beating benchmark?)
G3 = momentum > 0         (getting stronger?)

G1 & G2 & G3 → BUY
G1 & G2 & !G3 → HOLD (momentum fading)
G1 & !G2 & G3 → WATCH_EMERGING
G1 & !G2 & !G3 → AVOID
!G1 & G2 & G3 → WATCH_RELATIVE
!G1 & G2 & !G3 → SELL
!G1 & !G2 & G3 → WATCH_EARLY
!G1 & !G2 & !G3 → SELL

Overrides:
  BUY → HOLD if volume is DISTRIBUTION
  BUY → HOLD if regime is BEAR
  BUY needs non-weak volume confirmation in CORRECTION
```

**Source:** `compass_rs.py:288-376`

## P/E Zone

```
VALUE:     PE < 15
FAIR:      PE 15-25
STRETCHED: PE 25-40
EXPENSIVE: PE >= 40
```

**Source:** `compass_rs.py:271-285`

## Edge Cases
- Insufficient price history: return None (skip instrument)
- Division by zero (benchmark price = 0): skip
- All values must use `Decimal(str(value))`, never float
- Benchmark for Indian equities: NIFTY 50
