# Technical Indicators Overhaul — pandas-ta-classic

## Context

Our hand-rolled technical indicator computation in `app/computation/technicals.py`
(15 functions) has a silent-failure bug: on `de_equity_technical_daily`, RSI went
NULL from 2026-04-07 onwards and on 2026-04-13 *every* indicator column is NULL
despite rows being inserted. Rather than debug the formulae, we're replacing them
with a battle-tested library (`pandas-ta-classic`) so we (a) kill the bug class,
(b) expand coverage from ~40 to ~130 indicators, and (c) light up technicals for
asset classes that have zero today — sectoral/broad/thematic indices and eligible
equity mutual funds.

Risk metrics (Sharpe, Sortino, Calmar, max drawdown, beta) don't live in TA
libraries; they come from `empyrical-reloaded` and land in the same tables.

Outcome: one shared indicator engine runs daily over equities, indices, ETFs,
globals, and eligible MFs, writing ~130 typed Decimal columns per instrument per
day into 5 technical tables.

## Scope and asset class coverage

| Asset class | Source table | Target table | Instruments | Status |
|---|---|---|---|---|
| Equities | `de_equity_ohlcv` (close_adj) | `de_equity_technical_daily` | 2,281 | Replace in place |
| ETFs (NSE) | `de_etf_ohlcv` | `de_etf_technical_daily` | 258 | Replace in place |
| Globals (all types) | `de_global_prices` | `de_global_technical_daily` | all of `de_global_instrument_master` incl. ETFs, indices, bonds, commodities, forex, crypto | Replace in place |
| Indian indices | `de_index_prices` | `de_index_technical_daily` (**NEW**) | 135 (22 sectoral + 19 broad + 52 thematic + 42 strategy) | Create |
| MFs (eligible) | `de_mf_nav_daily` (nav) | `de_mf_technical_daily` (**NEW**) | ~800 (see MF filter) | Create |

**Out of scope**: sector/index breadth rollups (% above 50 DMA from constituent
booleans) — deferred to a follow-up that reads the new technical tables. The
data gap where only 1,255 / 13,380 MFs have NAV history is a separate P0 ticket,
not blocking this work. Non-equity-regular-growth MFs are deferred.

## Library choice (already settled)

- **pandas-ta-classic** — 130+ indicators, MIT, actively maintained fork of
  pandas-ta. DataFrame extension API (`df.ta.strategy(...)`).
- **empyrical-reloaded** — Sharpe, Sortino, Calmar, max drawdown, beta.
  Maintained Stefan Jansen fork.
- **TA-Lib** — optional test-time oracle only, installed in a separate CI
  container to generate golden fixtures. NOT a runtime dependency.

## Approach — no parallel-run phase

User wants a decisive cutover, not 14-day dual-write. For each asset class:

1. Compute full 130 indicators via the new engine over full history into a
   **temp v2 table** (e.g. `de_equity_technical_daily_v2`).
2. Sample-check the last ~30 trading days against the existing 40 indicators
   per a written tolerance (1e-4 absolute diff on RSI/SMA/EMA/MACD/BBands).
3. On clean diff: `pg_dump` the old table to `.sql.gz` as a rollback safety net,
   `DROP` the old, `ALTER TABLE ... RENAME` the v2 into place. Same name
   preserved so downstream readers don't change.
4. Move to the next asset class.

For asset classes without an existing table (`de_index_technical_daily`,
`de_mf_technical_daily`), step 2 is replaced by smoke tests against TA-Lib
golden fixtures on a small universe (NIFTY 50 index, top-20 MFs by AUM).

## Module structure

### `app/computation/indicators_v2/`

New package, not a flat module. Replaces `app/computation/technicals.py`.

- `engine.py` — generic `compute_indicators(spec, session, from_date, to_date)`.
  `AssetSpec` is a dataclass binding `(source_model, output_model, id_column,
  close_col, open_col, high_col, low_col, volume_col, min_history_days)`.
  Loads OHLCV per instrument as a pandas DataFrame, runs the shared
  `pandas_ta.Strategy`, converts floats → `Decimal.quantize(Decimal("0.0001"))`
  at the DB boundary, upserts via `pg_insert().on_conflict_do_update()` in
  batches of 200 (reuse existing `runner.py` pattern).
- `strategy.yaml` — versioned indicator catalog. Each entry: `{kind, params,
  output_cols, applies_to}`. Loaded once at import into a `pandas_ta.Strategy`
  object. Lets us diff the indicator set across git history.
- `risk_metrics.py` — empyrical wrapper. Inputs: daily returns Series, benchmark
  returns. Outputs: `risk_sharpe_1y`, `risk_sortino_1y`, `risk_calmar_1y`,
  `risk_max_drawdown_1y`, `risk_beta_nifty`. Called per-instrument inside the
  engine so we reuse the already-loaded DataFrame.
- `assets/equity.py`, `assets/etf.py`, `assets/index.py`, `assets/mf.py`,
  `assets/global_.py` — thin wrappers, each ~20 lines, that instantiate an
  `AssetSpec` and call `engine.compute_indicators`. MF wrapper also applies
  the eligibility filter (see below).
- `__init__.py` — re-exports the 5 per-asset entry points.

### Schema: v2 tables during migration, rename to final names on cutover

For the 3 replace-in-place tables (equity, ETF, global), the Alembic migration
creates a v2 twin (`<name>_v2`) with **all ~130 typed Decimal columns**
(`Numeric(18,4)` for price-scale, `Numeric(10,4)` for ratios/percentages,
`Numeric(8,4)` for bounded oscillators, `BigInteger` for volume aggregates).
The backfill fills the v2 twin, validation diffs old-vs-new, and a second
migration drops the old table and renames the v2 into place.

For the 2 new tables (index, mf), the migration creates them directly with
their final names. No v2 twin needed.

### Schema columns — ~130 indicators

pandas-ta output maps to columns grouped by family:
- **Overlap/Trend**: SMA(5,10,20,50,100,200), EMA(5,10,20,50,100,200), DEMA,
  TEMA, WMA, HMA, VWAP, KAMA, ZLMA, ALMA
- **Momentum**: RSI(7,9,14,21), MACD (line/signal/histogram, multiple param
  sets), Stochastic (K/D, multiple), CCI, MFI, ROC(5,10,21,63,252), TSI,
  Williams %R, CMO, TRIX, Ultimate Oscillator
- **Volatility**: Bollinger (upper/mid/lower/width/pct_b, multiple bandwidths),
  ATR(7,14,21), NATR, True Range, Keltner Channels, Donchian Channels,
  Historical Volatility (20d, 60d, 252d)
- **Volume**: OBV, AD, ADOSC, CMF, EFI, EOM, KVO, PVT, VWAP
- **Trend strength**: ADX(14), +DI, -DI, Aroon(up/down/osc), Supertrend, PSAR
- **Statistics**: Z-score(20), Quantile, Linear Regression (slope/intercept/
  angle/R²), Skew, Kurtosis
- **Risk (empyrical)**: Sharpe 1y, Sortino 1y, Calmar 1y, Max Drawdown 1y,
  Beta vs NIFTY 50, Alpha vs NIFTY 50, Omega, Information Ratio
- **Derived booleans** (GENERATED columns, preserved from current schema):
  `above_50dma`, `above_200dma`, plus new `above_20ema`, `price_above_vwap`,
  `rsi_overbought`, `rsi_oversold`, `macd_bullish`, `adx_strong_trend`

The full column list lives in `strategy.yaml` and is mirrored by the Alembic
migration — one source of truth, code-reviewed together.

## MF filter (prerequisite: add purchase_mode to de_mf_master)

JIP's `de_mf_master` has no `purchase_mode` column today. mfpulse_reimagined
has it as an integer from Morningstar OperationsMasterFile (1=Regular,
2=Direct). Bootstrap path:

1. **Migration**: `alembic/versions/XXX_add_purchase_mode_to_mf_master.py` —
   `ALTER TABLE de_mf_master ADD COLUMN purchase_mode INTEGER`.
2. **Bootstrap script** (`scripts/bootstrap_purchase_mode_from_mfpulse.py`):
   connect to mfpulse's Postgres, `SELECT mstar_id, purchase_mode FROM
   fund_master`, UPSERT into JIP's `de_mf_master`. Confirm row count ≈ 13,380
   after bootstrap; any NULLs stay NULL (funds mfpulse doesn't know about).
3. **Fix JIP Morningstar ingestion** (`app/pipelines/morningstar/` — need to
   locate the OperationsMasterFile handler): capture `purchase_mode` on weekly
   master refresh so the column stays current going forward. *Flag: if this
   ingestion path isn't already pulling OperationsMasterFile, that's a scope
   extension — may need a separate sub-task.*

Final MF eligibility filter (applied in `assets/mf.py`):

```sql
SELECT m.mstar_id FROM de_mf_master m
WHERE m.purchase_mode = 1
  AND m.broad_category = 'Equity'
  AND m.is_active AND NOT m.is_etf AND NOT m.is_index_fund
  AND m.fund_name !~* '\b(IDCW|Dividend|Segregated)\b'
  AND EXISTS (SELECT 1 FROM de_mf_nav_daily n WHERE n.mstar_id = m.mstar_id)
```

Expected ~800 funds (942 today without `purchase_mode`; narrows to ~800 after
Regular-only filter).

## Backfill strategy — chunked by instrument, sequential

t3.large has 2 vCPU and 8 GB RAM. Loading the full `de_equity_technical_daily`
universe at once is infeasible. Instead, stream per-instrument:

1. `scripts/backfill_indicators_v2.py --asset equity --from 2007-01-01`
2. For each instrument: load full OHLCV history (~4,800 rows), run strategy,
   compute risk metrics, bulk upsert to v2 table, free the DataFrame.
3. `multiprocessing.Pool(processes=2)` — each worker handles one instrument at
   a time; memory per worker ~50 MB peak.
4. Resumable via `backfill_cursor` (new tiny table: `asset_class, last_id,
   updated_at`). Crash-recovery just restarts from cursor.
5. ETAs (best estimates pre-benchmark):
   - Equities (2,281 × 4,800 rows × 130 cols): 45–75 min
   - ETFs (258): 5–10 min
   - Globals (~50–200 depending on active count): 5–15 min
   - Indices (135 × 4,800): 10–15 min
   - MFs (800 × up to 5,000 NAV rows): 20–40 min
6. Run order: equities → indices → ETFs → globals → MFs (dependency-free, but
   this matches dashboard priority).

## Validation

### Sample-check (for 3 replace-in-place tables)

`scripts/diff_technicals_old_vs_new.py --asset equity --last-days 30`

Joins v1 and v2 on `(instrument_id, date)` for the **40 overlapping columns**.
Reports per column: `count(rows), max_abs_diff, mean_abs_diff, pct_within_1e_4`.

Pass thresholds:
- Core indicators (RSI/SMA/EMA/MACD/BBands/ATR): 99.5% of rows within `1e-4`
- Secondary (ADX, Stochastic, MFI): 99% within `1e-3`
- Row count equal within ±2 (last-day ingest drift)

Any failure = investigate before cutover. The temp v2 table is kept; nothing
is dropped until sample-check is green.

### Golden-file tests

`tests/computation/fixtures/golden/nifty50_2023_2025.parquet` — OHLCV.
`tests/computation/fixtures/golden/nifty50_indicators_talib.parquet` — TA-Lib
reference outputs, generated once in `Dockerfile.talib-oracle` (CI-only).

`tests/computation/test_indicators_v2_golden.py`:
- Asserts pandas-ta output matches TA-Lib to `1e-6` on RSI(14), MACD(12,26,9),
  ADX(14), Bollinger(20,2), ATR(14), OBV, SMA(50), EMA(20)
- Asserts Decimal quantization is exactly `Decimal("0.0001")` step
- Asserts NaN handling at warmup (no silent 0s — the exact bug that bit us)

Fixture regeneration is a documented manual step in the test module docstring.

### Post-cutover smoke

After each rename, `curl https://data.jslwealth.in/api/v1/observatory/pulse`
should show the pipeline green for the affected stream. Spot-check 5
instruments via psql: RELIANCE equity, NIFTY 50 index, NIFTYBEES ETF,
SPY global ETF, top equity MF.

## Cron wiring

Register in `app/pipelines/registry.py` as a new pipeline `compute_indicators_v2`
with dependencies on the raw data pipelines (`eod`, `amfi`, `index_prices_eod`,
`yfinance_eod`). Wire into `nightly_compute` in `scripts/cron/jip_scheduler.cron`
to fire after the existing `technicals` step (during the transition) and
eventually replace it. After cutover, remove the old `technicals` step
from the nightly chain.

Observatory dashboard (`app/api/v1/observatory.py`) auto-picks up new pipeline
runs from `de_cron_run` and `de_pipeline_log` — no dashboard code changes
needed. It will show one row per asset class per night.

## Deletion of old code

Only after ALL of:
- All 3 replace-in-place cutovers complete and verified (1 week soak)
- `breadth.py` confirmed still working (reads `above_50dma` / `above_200dma`
  GENERATED columns, which we preserve in the new schema — verify)
- `test_breadth.py`, `test_runner.py` green against new tables
- Observatory dashboard green for 7 consecutive nights

Then:
- Delete `app/computation/technicals.py`
- Delete `tests/computation/test_technicals.py` (replaced by
  `test_indicators_v2_golden.py`)
- Delete the `.sql.gz` rollback dumps from `/backups/` (or archive to S3)

## Critical files to modify or create

**New**:
- `app/computation/indicators_v2/engine.py`
- `app/computation/indicators_v2/strategy.yaml`
- `app/computation/indicators_v2/risk_metrics.py`
- `app/computation/indicators_v2/assets/{equity,etf,index_,mf,global_}.py`
- `app/computation/indicators_v2/__init__.py`
- `alembic/versions/XXX_indicators_v2_tables.py` (v2 twins + 2 new tables)
- `alembic/versions/XXX_add_purchase_mode_to_mf_master.py`
- `alembic/versions/XXX_cutover_rename_indicators_v2.py` (rename after diff)
- `scripts/backfill_indicators_v2.py`
- `scripts/diff_technicals_old_vs_new.py`
- `scripts/bootstrap_purchase_mode_from_mfpulse.py`
- `tests/computation/test_indicators_v2_golden.py`
- `tests/computation/fixtures/golden/nifty50_2023_2025.parquet`
- `tests/computation/fixtures/golden/nifty50_indicators_talib.parquet`
- `Dockerfile.talib-oracle` (CI-only, for fixture regeneration)

**Modified**:
- `pyproject.toml` — add `pandas-ta-classic`, `empyrical-reloaded`
- `app/computation/runner.py` — swap `technicals.py` calls for `indicators_v2`
  per-asset-class entry points
- `app/pipelines/registry.py` — register `compute_indicators_v2` pipeline
- `scripts/cron/jip_scheduler.cron` — wire `compute_indicators_v2` into
  `nightly_compute`

**Deleted (after soak)**:
- `app/computation/technicals.py`
- `tests/computation/test_technicals.py`

## Execution order (checklist)

1. Add deps to `pyproject.toml`, verify `pip install` works clean
2. Alembic migration — v2 twin tables + 2 new tables + `purchase_mode` column
3. Write `indicators_v2/engine.py` + `strategy.yaml` + `risk_metrics.py`
4. Write `assets/equity.py`, golden-file tests, run against TA-Lib oracle
5. Run `backfill_indicators_v2.py --asset equity --from 2007-01-01`
6. Run `diff_technicals_old_vs_new.py --asset equity --last-days 30`, review
7. On clean diff: `pg_dump` old table → DROP old → RENAME v2 → smoke test
8. Repeat steps 4–7 for ETF, global (replace-in-place)
9. Write `assets/index_.py`, backfill, smoke test (new table, no diff)
10. Bootstrap `purchase_mode` from mfpulse; fix JIP Morningstar ingestion to
    keep it current
11. Write `assets/mf.py` with eligibility filter, backfill, smoke test
12. Register pipeline in `registry.py`, wire into cron
13. Monitor nightly runs for 7 days
14. Delete old `technicals.py` + tests, drop rollback dumps
15. File follow-up tickets: (a) MF NAV backfill gap (1,255 / 13,380), (b)
    sector breadth rollups from new technical tables

## Open questions / flags

- **Does JIP's Morningstar ingestion path already fetch OperationsMasterFile?**
  If not, adding `purchase_mode` capture is a meaningful sub-task. Verify
  by reading `app/pipelines/morningstar/` before step 10.
- **GENERATED columns during rename**: Postgres `GENERATED ALWAYS AS ...
  STORED` columns persist across `ALTER TABLE ... RENAME`; but the v2 table
  must define them identically. Double-check the migration covers this for
  `above_50dma` / `above_200dma` or `breadth.py` breaks silently.
- **mfpulse DB access from JIP**: confirm network reachability and creds.
  If mfpulse Postgres isn't reachable from the JIP EC2, the bootstrap script
  needs to run from a machine that can reach both (e.g. local dev with SSH
  tunnels) and dump a CSV to upload.
