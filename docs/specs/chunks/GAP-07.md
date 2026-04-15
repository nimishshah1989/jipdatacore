# GAP-07 — Targeted multi-year risk column backfill (fast path)

## Goal
Populate the new multi-year risk columns from GAP-05 (`sharpe_3y`, `sharpe_5y`,
`sortino_3y`, `sortino_5y`, `max_drawdown_3y`, `max_drawdown_5y`, `treynor`,
`downside_risk`) across equity, ETF, global, and index v2 technical tables —
**without** re-running the full ~130-indicator pandas-ta engine.

## Why a new script (not backfill_indicators_v2.py)

The full indicator backfill takes ~30–40 min per asset class because pandas-ta's
`DataFrame.ta.strategy()` runs ~130 indicators per instrument inside a Python
loop. For GAP-07 we only need 8 risk columns, all of which are pure numpy
rolling operations on returns. A targeted script can process all 2,800
instruments × 4,800 dates in **2–3 minutes total** by loading close prices as
a single wide DataFrame and computing risk metrics matrix-wise.

The previous GAP-07 attempt ran `scripts/backfill_indicators_v2.py --asset
equity` and hit the 45-minute chunk timeout. This rewrite avoids that class of
slowness entirely.

## Scope

### New script: `scripts/fill_new_risk_columns.py`

For each asset class (equity, etf, global, index):

1. **Load close prices as a wide matrix** in one query:
   ```sql
   SELECT date, instrument_id, close_adj
   FROM de_equity_ohlcv
   WHERE date >= CURRENT_DATE - INTERVAL '6 years'
   ORDER BY date, instrument_id
   ```
   Pivot in pandas: `df = long.pivot(index='date', columns='instrument_id',
   values='close_adj')`. Result is a ~1500 × 2281 float matrix.

2. **Compute returns once**: `returns = df.pct_change()` — single vectorized op.

3. **Risk metrics via column-wise rolling** (numpy, not per-instrument loops):
   - **Sharpe** = `(rolling_mean(r, w) / rolling_std(r, w)) * sqrt(252)` for
     each window w in (252, 756, 1260). One numpy op, whole matrix at once.
   - **Sortino**: same but use downside-only std.
   - **Max drawdown**: `(cum / running_max − 1).rolling(w).min()`.
   - **Treynor**: `(rolling_mean(r) − rf) / rolling_beta(r, nifty)` — needs
     NIFTY 50 return series loaded once for the rolling-beta term.
   - **Downside risk** = rolling std of `clip(r, upper=0)` × sqrt(252).

4. **Clamp** to `Decimal("±999999.9999")` at the pandas level using
   `.clip(lower, upper)` before conversion (matches existing engine boundary).

5. **UPDATE not UPSERT**: write back in chunks via a temp table:
   ```sql
   CREATE TEMP TABLE _risk_stg(instrument_id uuid, date date,
     sharpe_3y numeric(10,4), ...)
   COPY _risk_stg FROM stdin
   UPDATE de_equity_technical_daily t SET
     sharpe_3y = s.sharpe_3y, sortino_3y = s.sortino_3y, ...
   FROM _risk_stg s
   WHERE t.instrument_id = s.instrument_id AND t.date = s.date
   ```
   UPDATE is cheap because Postgres only rewrites the touched columns' heap
   tuple versions, no full-row rewrite.

### Asset table specifics

| Asset | Source table | PK cols | NIFTY proxy for treynor |
|---|---|---|---|
| equity | `de_equity_ohlcv` → `de_equity_technical_daily` | (instrument_id, date) | NIFTY 50 close from `de_index_prices` |
| etf | `de_etf_ohlcv` → `de_etf_technical_daily` | (ticker, date) | NIFTY 50 |
| global | `de_global_prices` → `de_global_technical_daily` | (ticker, date) | SPY from `de_global_prices` |
| index | `de_index_prices` → `de_index_technical_daily` | (ticker, date) | NIFTY 50 |

### Runtime targets
- equity: ≤ 90 seconds
- etf, global, index each: ≤ 30 seconds
- **Total**: ≤ 4 minutes for all 4 asset classes (vs ~3 hours via the full engine)

## Acceptance criteria

- [ ] `scripts/fill_new_risk_columns.py` exists, runs against all 4 asset classes
- [ ] `SELECT COUNT(*) FROM de_equity_technical_daily WHERE sharpe_3y IS NOT NULL` ≥ 2,000,000
- [ ] `SELECT COUNT(*) FROM de_equity_technical_daily WHERE sharpe_5y IS NOT NULL` ≥ 1,000,000
- [ ] Same for etf / global / index with proportional thresholds
- [ ] `downside_risk` non-NULL everywhere `sharpe_3y` is non-NULL (they share the 3y window)
- [ ] Wall-clock runtime for the full 4-asset run is < 5 minutes
- [ ] Commit subject starts with `GAP-07`

## Steps for the inner session

1. Verify no orphan `scripts.backfill_indicators_v2` processes from the prior
   failed run: `ps aux | grep backfill_indicators_v2`. If any, kill them.
2. Read `app/computation/indicators_v2/risk_metrics.py` to confirm the exact
   formulae and windows used by the engine (so the matrix version matches).
3. Read `app/models/indicators_v2.py` to get the precise column names and
   Decimal precision per asset class.
4. Write `scripts/fill_new_risk_columns.py` with a single `main(asset_class)`
   entry point and a CLI that accepts `--asset {equity,etf,global,index,all}`.
5. Run `--asset equity` first, time it, check counts match acceptance.
6. Run `--asset all`. Verify final counts against acceptance criteria.
7. Commit.

## Out of scope
- MF risk backfill (MF goes via GAP-10 / GAP-14).
- Full pandas-ta indicator recomputation (intentionally skipped — this is the point).
- Schema changes (GAP-05 already added the columns).

## Dependencies
- Upstream: GAP-05 (columns), GAP-06 (risk formulae — reference only).
- Downstream: none.
