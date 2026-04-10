# C6: New Computations

**Complexity:** High
**Dependencies:** C1 (schema — de_oscillator_weekly, de_oscillator_monthly, de_divergence_signals, de_fib_levels, de_index_pivots, de_intermarket_ratios, plus stochastic/disparity/bollinger_width columns on de_equity_technical_daily)
**Status:** pending

## Files
- app/computation/oscillators.py (new)
- app/computation/divergence.py (new)
- app/computation/fibonacci.py (new)
- app/computation/pivots.py (new)
- app/computation/intermarket.py (new)
- app/computation/runner.py (modify — register new computations in dependency sequence)

## Context

### Existing computation modules (do not duplicate)
- `app/computation/technicals.py` — computes SMA20, SMA50, SMA200, EMA20, RSI14, MACD, Bollinger upper/lower/middle for de_equity_technical_daily. Already upserts daily. C6 adds new columns to the same row.
- `app/computation/rs.py` — relative strength scores
- `app/computation/breadth.py` — breadth composite
- `app/computation/regime.py` — market regime
- `app/computation/runner.py` — orchestrates all in sequence: technicals → rs → breadth → regime → sectors → fund_derived

### Existing runner pattern
The runner exposes `run_technicals_for_date(session, date)` and similar per-step functions, each returning `StepResult`. The overall `run_computations(session, date)` calls them in order. New computations follow the same `StepResult` return convention defined in `app/computation/qa_types.py`.

### Scale
de_equity_ohlcv holds prices for ~2,000 instruments over multiple years. Daily computation batch: ~2,000 rows. Weekly/monthly aggregation: ~400 weekly rows per instrument for 3 years of history. Use SQL aggregation, not pandas groupby, for weekly/monthly bar construction wherever possible.

## What To Build

### oscillators.py — Stochastic Oscillator + Disparity Index + Bollinger Width

All functions are async, accept `AsyncSession` and a `date`, return `StepResult`.

**compute_stochastic_daily(session, business_date) -> StepResult**

Logic (SQL-first):
1. Fetch 30 days of OHLCV per instrument (enough for 14-period stochastic + 3-period smoothing):
   ```sql
   SELECT instrument_id, date, high, low, close
   FROM de_equity_ohlcv
   WHERE date BETWEEN :start AND :business_date
     AND data_status = 'validated'
   ORDER BY instrument_id, date
   ```
2. In Python (pandas vectorized, no iterrows):
   - Group by instrument_id
   - `low_14 = rolling_min(low, 14)`, `high_14 = rolling_max(high, 14)`
   - `k_raw = (close - low_14) / (high_14 - low_14) * 100`
   - `k = k_raw.rolling(3).mean()` (smoothed %K)
   - `d = k.rolling(3).mean()` (%D signal)
   - Take last row per instrument (business_date only)
3. Upsert into de_equity_technical_daily (stochastic_k, stochastic_d) for business_date rows that already exist from technicals step.
   - Use `INSERT ... ON CONFLICT (date, instrument_id) DO UPDATE SET stochastic_k=..., stochastic_d=...`
4. All values cast to `Decimal` before insert. Handle division by zero (high == low) → NULL, not NaN.
5. Return StepResult with rows_written count.

**compute_disparity_daily(session, business_date) -> StepResult**

Prerequisites: SMA20 and SMA50 already computed by technicals step and present in de_equity_technical_daily. Use them directly:
```sql
UPDATE de_equity_technical_daily
SET
  disparity_20 = ROUND(((close_adj - sma_20) / NULLIF(sma_20, 0)) * 100, 4),
  disparity_50 = ROUND(((close_adj - sma_50) / NULLIF(sma_50, 0)) * 100, 4)
WHERE date = :business_date
  AND sma_20 IS NOT NULL
  AND sma_50 IS NOT NULL
```
Pure SQL UPDATE — no Python/pandas needed. Return StepResult with rowcount.

**compute_bollinger_width_daily(session, business_date) -> StepResult**

Prerequisites: bollinger_upper, bollinger_lower, bollinger_middle already computed by technicals step.
```sql
UPDATE de_equity_technical_daily
SET bollinger_width = ROUND(
    ((bollinger_upper - bollinger_lower) / NULLIF(bollinger_middle, 0)) * 100,
    4)
WHERE date = :business_date
  AND bollinger_upper IS NOT NULL
  AND bollinger_lower IS NOT NULL
  AND bollinger_middle IS NOT NULL
```
Pure SQL UPDATE. Return StepResult.

**compute_stochastic_weekly(session, business_date) -> StepResult**

Weekly bars: use the most recent Friday on or before business_date as the week's closing bar.
1. Aggregate OHLCV to weekly bars via SQL:
   ```sql
   SELECT
     instrument_id,
     date_trunc('week', date) + interval '4 days' AS week_end,
     MIN(low) AS low,
     MAX(high) AS high,
     (array_agg(close ORDER BY date DESC))[1] AS close
   FROM de_equity_ohlcv
   WHERE date >= :start_26w AND date <= :business_date
     AND data_status = 'validated'
   GROUP BY instrument_id, date_trunc('week', date)
   ORDER BY instrument_id, week_end
   ```
2. Compute stochastic (14,3,3) and disparity_20 on weekly bars in Python (vectorized).
3. Upsert into de_oscillator_weekly for the current week_end date.
   - ON CONFLICT (date, instrument_id) DO UPDATE.
4. Only run on Fridays (or last trading day of the week). Skip if business_date is not week-end.

**compute_stochastic_monthly(session, business_date) -> StepResult**

Same pattern as weekly but using monthly bars. Only run on last trading day of month.
- SQL group by: `date_trunc('month', date)` with last close of month.
- Upsert into de_oscillator_monthly.

### divergence.py — Divergence Detection

**compute_divergences(session, business_date) -> StepResult**

Algorithm (Python, vectorized):
1. Pull 90 days of daily data per instrument (enough for swing detection):
   ```sql
   SELECT e.instrument_id, e.date, e.close,
          t.rsi_14, t.stochastic_k, t.macd_histogram
   FROM de_equity_ohlcv e
   JOIN de_equity_technical_daily t USING (instrument_id, date)
   WHERE e.date BETWEEN :start AND :business_date
     AND e.data_status = 'validated'
   ORDER BY e.instrument_id, e.date
   ```
2. Swing detection (vectorized per instrument):
   - A swing high occurs when `close[i] > close[i-1]` and `close[i] > close[i+1]` (simple local max).
   - Only accept swings where the move from previous swing is >= 3% for daily (absolute pct change between consecutive swing points).
   - Keep last 4 swing highs and last 4 swing lows per instrument.
3. Divergence check (last 2 swing points):
   - **Bullish regular:** price makes lower low, RSI/stochastic makes higher low
   - **Bearish regular:** price makes higher high, RSI/stochastic makes lower high
   - **Strength:** count consecutive divergent swings. strength=1 (one pair), strength=2 (two pairs), strength=3 (triple — Gautam's strongest signal, three consecutive divergent swings).
4. Only store NEW divergences not already in de_divergence_signals for the same (date, instrument_id, timeframe, indicator). Idempotent insert.
5. Detect on: RSI14, stochastic_k. MACD histogram requires macd_histogram column — check if present, skip if NULL.
6. Timeframes: daily always. Weekly: run if business_date is week-end, pull from de_oscillator_weekly.

**compute_divergences_weekly(session, business_date) -> StepResult**

Same logic but using 52-week window and de_oscillator_weekly values. Only runs on Fridays.

### fibonacci.py — Fibonacci Retracement Levels

**compute_fib_levels(session, business_date) -> StepResult**

1. Pull 180 days of close per instrument:
   ```sql
   SELECT instrument_id, date, high, low, close
   FROM de_equity_ohlcv
   WHERE date BETWEEN :start AND :business_date
     AND data_status = 'validated'
   ORDER BY instrument_id, date
   ```
2. Per instrument (vectorized):
   - Swing detection: same local max/min logic, min move >= 5% for equities.
   - Identify most recent swing pair (last significant high and last significant low).
   - Determine direction: if low is more recent → downswing (fib from high down). If high is more recent → upswing (fib from low up).
   - Calculate levels:
     ```python
     swing_range = swing_high - swing_low
     fib_236 = swing_low + swing_range * Decimal('0.236')
     fib_382 = swing_low + swing_range * Decimal('0.382')
     fib_500 = swing_low + swing_range * Decimal('0.500')
     fib_618 = swing_low + swing_range * Decimal('0.618')
     fib_786 = swing_low + swing_range * Decimal('0.786')
     ```
   - All arithmetic with `Decimal`, not float.
3. Upsert into de_fib_levels with business_date as the `date` column. ON CONFLICT (date, instrument_id) DO UPDATE.
4. Instruments with fewer than 30 data points: skip (insufficient history), do not fail.

### pivots.py — Index Pivot Points

**compute_index_pivots(session, business_date) -> StepResult**

Major indices to compute for (index_code values):

| index_code | Index name in de_index_prices |
|---|---|
| NIFTY50 | NIFTY 50 |
| BANKNIFTY | NIFTY BANK |
| NIFTYIT | NIFTY IT |
| NIFTYMETAL | NIFTY METAL |
| NIFTYPHARMA | NIFTY PHARMA |
| NIFTYENERGY | NIFTY ENERGY |
| NIFTYREALTY | NIFTY REALTY |
| NIFTYAUTO | NIFTY AUTO |
| NIFTYFMCG | NIFTY FMCG |

Source table: `de_index_prices` — verify column names before building queries. Columns expected: index_name, date, open, high, low, close.

1. Fetch previous trading day's OHLC for each index:
   ```sql
   SELECT index_name, high, low, close
   FROM de_index_prices
   WHERE date = (
     SELECT MAX(date) FROM de_index_prices WHERE date < :business_date
   )
   ```
2. Compute pivot formula in Python (Decimal arithmetic, not float):
   ```python
   pivot = (high + low + close) / Decimal('3')
   s1 = Decimal('2') * pivot - high
   s2 = pivot - (high - low)
   s3 = low - Decimal('2') * (high - pivot)
   r1 = Decimal('2') * pivot - low
   r2 = pivot + (high - low)
   r3 = high + Decimal('2') * (pivot - low)
   ```
3. Upsert into de_index_pivots (date=business_date, index_code). ON CONFLICT DO UPDATE.
4. If previous day data not found for an index: log warning, skip that index, do not fail.

### intermarket.py — Intermarket Ratios

**compute_intermarket_ratios(session, business_date) -> StepResult**

Ratios to compute:

| ratio_name | numerator | denominator | source tables |
|---|---|---|---|
| BANKNIFTY_NIFTY | NIFTY BANK close | NIFTY 50 close | de_index_prices |
| MICROCAP_NIFTY | NIFTY MICROCAP 250 close | NIFTY 50 close | de_index_prices |
| GOLD_NIFTY | GOLD close (MCX or yfinance) | NIFTY 50 close | de_global_prices + de_index_prices |

Check `de_global_prices` for a GOLD ticker (likely `GC=F` from yfinance or `GOLD` from FRED). If not found, skip GOLD_NIFTY ratio and log a warning.

1. Fetch last 25 days of daily data for each source:
   ```sql
   SELECT date, index_name, close FROM de_index_prices
   WHERE index_name IN ('NIFTY 50', 'NIFTY BANK', 'NIFTY MICROCAP 250')
     AND date BETWEEN :start_25d AND :business_date
   ORDER BY date
   ```
2. Compute ratio values. Join on date. If denominator is zero or NULL: ratio = NULL.
3. Compute sma_20 of ratio over last 20 days.
4. direction: 'rising' if current value > sma_20, 'falling' if < sma_20, 'flat' if equal.
5. Upsert into de_intermarket_ratios for business_date. ON CONFLICT DO UPDATE.

### runner.py modification

Add new steps to the computation sequence after the existing `technicals` step. The existing dependency chain is:
```
technicals → rs → breadth → regime → sectors → fund_derived
```

New sequence (insert after technicals, before rs):
```
technicals
  → stochastic_daily (needs: de_equity_technical_daily rows for business_date)
  → disparity_daily (needs: sma_20, sma_50 from technicals — pure SQL UPDATE)
  → bollinger_width_daily (needs: bollinger columns from technicals — pure SQL UPDATE)
  → divergence_daily (needs: technicals + stochastic populated)
  → fibonacci (needs: de_equity_ohlcv only — can run parallel with stochastic)
  → pivots (needs: de_index_prices only — independent)
  → intermarket (needs: de_index_prices + de_global_prices — independent)
→ rs → breadth → regime → sectors → fund_derived
```

Weekly/monthly computations (stochastic_weekly, stochastic_monthly, divergence_weekly) run after the main sequence and are gated:
```python
if is_week_end(business_date):
    await compute_stochastic_weekly(session, business_date)
    await compute_divergences_weekly(session, business_date)
if is_month_end(business_date):
    await compute_stochastic_monthly(session, business_date)
```

Import new modules at top of runner.py using the same pattern as existing imports (lazy or top-level, match existing style). Wrap each new step in try/except and return its StepResult. If a new step fails, log the error but do not block the rest of the sequence.

## Edge Cases

- **Division by zero:** Stochastic denominator (high_14 - low_14) == 0 → store NULL for k_raw, propagate NULL to k and d.
- **Insufficient history:** Instruments with < 14 rows → stochastic NULL. Instruments with < 30 rows → fibonacci skipped.
- **NULL oscillator values:** If RSI14 or macd_histogram is NULL in technicals output → skip divergence detection for that indicator on that instrument.
- **Market holidays:** Weekly/monthly gating uses `is_week_end` / `is_month_end` helpers — check if business_date is the last trading session of the week/month, not just calendar Friday/last day. Derive from de_equity_ohlcv trading day presence.
- **Missing index data:** If NIFTY MICROCAP 250 not in de_index_prices → skip that ratio, log once.
- **All-NULL weeks:** If a weekly bar has no OHLCV data (holiday week) → skip that bar, do not insert NULL row.

## Acceptance Criteria
- [ ] Stochastic K/D computed and stored in de_equity_technical_daily.stochastic_k / stochastic_d for all instruments with sufficient history on each business_date
- [ ] Stochastic K/D stored in de_oscillator_weekly on week-end days
- [ ] Stochastic K/D stored in de_oscillator_monthly on month-end days
- [ ] Disparity 20/50 stored via SQL UPDATE in de_equity_technical_daily (no Python loop)
- [ ] Bollinger width stored via SQL UPDATE in de_equity_technical_daily
- [ ] Divergence detection runs on daily timeframe; bullish, bearish, triple variants correctly identified
- [ ] Divergence detection runs on weekly timeframe on week-end days
- [ ] Triple divergence (strength=3) correctly requires 3 consecutive divergent swing pairs
- [ ] Fibonacci levels computed from auto-detected swings, all 5 levels stored (fib_236 through fib_786)
- [ ] Pivot points computed for all 9 major indices from previous day's OHLC
- [ ] Intermarket ratios computed with sma_20 and direction for available ratio pairs
- [ ] All numeric values stored as Decimal (Numeric(18,4) or Numeric(8,4) per column definition)
- [ ] Division-by-zero cases produce NULL, not zero or exception
- [ ] New computation steps registered in runner.py with try/except wrappers
- [ ] `pytest tests/ -v --tb=short` passes
- [ ] `ruff check . --select E,F,W` passes on all new files
