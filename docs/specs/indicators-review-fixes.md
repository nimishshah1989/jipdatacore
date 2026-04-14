# Indicators Overhaul — Engineering Review Fixes (BINDING)

This document captures gaps surfaced in the engineering review of the chunk plan. **Every chunk spec is read alongside this file**. Where a fix below contradicts the chunk spec, the fix wins.

Read order for the implementer: chunk spec → this addendum → start work.

---

## Fix 1: Pre-flight Morningstar investigation (chunk 1 or new chunk 1.5)

**Background**: Chunk 9 assumes JIP's Morningstar ingestion pulls "OperationsMasterFile". A quick grep of `app/pipelines/morningstar/` shows files `fund_master.py`, `holdings.py`, `risk.py`, `isin_resolver.py`, `client.py` — **no OperationsMasterFile fetching logic**. This is a near-certain scope extension.

**Action** (pre-build, 30 min):
1. Grep `app/pipelines/morningstar/` for endpoint URLs, API methods, and `purchase_mode` references
2. Determine: is `purchase_mode` already fetched but not persisted, or not fetched at all?
3. Document findings in `reports/morningstar_purchase_mode_investigation.md`
4. If not fetched: **file a ticket**, add new chunk **9a (Morningstar API extension)** to the plan, and proceed with 9b = bootstrap-only from mfpulse with stale-data acknowledged

Do this **before** building chunk 1. It unblocks all sequencing decisions for chunks 9/10.

---

## Fix 2: Chunk 3 split into 3a / 3b / 3c

Chunk 3 as written blocks 5 downstream chunks. Split to unblock chunk 4 earlier and reduce PR size.

- **3a — Engine skeleton + spec + strategy loader + SMA/EMA only**
  - `spec.py`, `engine.py` (scaffold), `strategy.yaml` (only SMA/EMA entries), `strategy_loader.py`
  - Unit tests pass against 2 indicators
  - Unblocks chunk 4 (golden tests can start against SMA/EMA baseline)
- **3b — Full strategy.yaml catalog + pandas-ta wiring**
  - Adds all ~130 indicators to strategy.yaml
  - Engine runs full strategy on a DataFrame
  - Column rename map (see Fix 3) lands here
  - Unblocks chunk 5
- **3c — risk_metrics.py (empyrical)**
  - Can run parallel to 3b
  - Unblocks chunks 5, 10 (only when combined with 3b)

Update `tasks.json` and `indicators-chunk-plan.md` dependency graph:
- IND-C3 → IND-C3a, IND-C3b, IND-C3c
- IND-C4 depends on IND-C3a (not full C3)
- IND-C5 depends on IND-C3b + IND-C3c + IND-C4

---

## Fix 3: Engine must have an explicit pandas-ta → schema column rename map

**Problem**: pandas-ta emits names like `MACDh_12_26_9`, `BBU_20_2.0`, `STOCHk_14_3_3`. Our schema uses `macd_histogram`, `bb_upper`, `stochastic_k`. Without explicit translation, engine silently writes zero columns.

**Required** in chunk 3b:
- Each entry in `strategy.yaml` MUST include an `output_columns: {pandas_ta_name: schema_name}` map
- `engine.py` applies the map after `df.ta.strategy(strategy)` runs, via `df.rename(columns=rename_map, inplace=True)`
- Engine asserts the post-rename column set exactly equals the schema column set: `assert set(df.columns) >= set(SCHEMA_COLS)` — fail loud on any mismatch
- Unit test in `test_indicators_v2_engine.py`: run on synthetic data, assert output columns exactly match `strategy.yaml[*].output_columns.values()`

---

## Fix 4: NaN write policy — pin explicitly

**Required** in chunk 3a engine spec:
- NaN values → NULL per column at the `_to_decimal_row` boundary
- Rows are **never skipped** on the basis of individual NaN values
- Rows **are skipped** only when the entire instrument has fewer than `spec.min_history_days` rows
- Unit test: instrument with 300 rows, assert SMA_200 is NULL for rows 0-199 and non-NULL for rows 200-299

---

## Fix 5: Decimal quantization — same dict for INSERT and ON CONFLICT UPDATE

**Problem**: `pg_insert().on_conflict_do_update(set_={...})` could easily be built from the raw pandas row rather than the quantized dict, leaking floats.

**Required** in chunk 3a:
- Engine builds ONE dict per row via `_to_decimal_row`, and uses that same dict for both INSERT VALUES and the ON CONFLICT UPDATE SET clause
- Unit test: mock upsert, assert every value passed to `pg_insert` is `isinstance(v, (Decimal, type(None), int, bool, date, datetime))` — never `float` or `numpy.float64`

---

## Fix 6: Price history gap handling — pin policy

**Problem**: pandas-ta operates on row position. If RELIANCE has a 10-day suspension gap in OHLCV, SMA_50 at the row after the gap will average across the gap, producing silently-wrong output.

**Required policy** (pinned, not negotiable): **Engine assumes row-position semantics; calendar gaps are NOT rebuilt.** We accept that post-suspension SMA values include pre-suspension data in the window. This matches how most Indian market data vendors report historicals.

**Required** in chunk 3a:
- Engine asserts `df[spec.date_column].is_monotonic_increasing` before strategy runs — fail loud on unsorted input
- Unit test in chunk 5: synthetic instrument with a 10-day gap; assert engine does NOT crash, assert SMA values post-gap use row-position windows; document the result in test docstring
- Add to PRD / engine docstring: "Row-position windows; gaps in the calendar are NOT backfilled."

---

## Fix 7: Per-instrument error isolation in backfill (chunk 5)

**Problem**: Chunk 5 says "exits non-zero on any instrument error". For a 2,281-instrument run, one corrupt row kills the whole backfill.

**Required** in chunk 5:
- Wrap per-instrument processing in try/except
- Record failures to an `errors` list with `{instrument_id, error_type, error_message, traceback}`
- Continue processing after each failure
- On completion, write `reports/backfill_errors_{asset}_{date}.md` with error summary
- Exit non-zero if `errors.count > max(10, 0.5% of total)`
- Update acceptance criterion: `errors < 10 OR errors/total < 0.5%`

---

## Fix 8: Cursor ordering (chunk 5)

**Required**: `backfill_cursor` stores the `last completed instrument_id`; workers iterate `ORDER BY instrument_id ASC`. The ordering column must match the cursor's stored key exactly or resumption skips/duplicates work. State this explicitly in chunk 5.

---

## Fix 9: GENERATED column expressions must be byte-identical + smoke test (chunk 2)

**Problem**: `ALTER TABLE RENAME` preserves GENERATED column definitions, but if the v2 table's expression differs (even whitespace), queries using `pg_get_expr` may drift.

**Required** in chunk 2 acceptance:
- All GENERATED expressions written as exact SQL strings (not Python-generated). Pin in chunk spec.
- Post-migration smoke test: insert a dummy row with known values, assert every generated column computes to the expected boolean. Example:
  ```sql
  INSERT INTO de_equity_technical_daily_v2 (date, instrument_id, close_adj, sma_50, sma_200, ema_20, vwap, rsi_14, macd_line, macd_signal, adx_14)
  VALUES ('1999-01-01', '00000000-0000-0000-0000-000000000000', 100, 95, 90, 98, 97, 75, 1.5, 1.0, 30);
  SELECT above_50dma, above_200dma, above_20ema, price_above_vwap, rsi_overbought, rsi_oversold, macd_bullish, adx_strong_trend
  FROM de_equity_technical_daily_v2 WHERE date = '1999-01-01';
  -- Expect: t, t, t, t, t, f, t, t
  DELETE FROM de_equity_technical_daily_v2 WHERE date = '1999-01-01';
  ```

---

## Fix 10: Expanded post-cutover test list (chunk 6)

**Problem**: `above_50dma` is read by far more than `breadth.py`. Grep finds it in `regime.py`, `sectors.py`, `post_qa.py`, `spot_check.py`, `app/api/v1/market.py`.

**Required** in chunk 6 acceptance:
- Post-rename test suite: `pytest tests/computation/test_breadth.py tests/computation/test_runner.py tests/computation/test_regime.py tests/computation/test_sectors.py tests/computation/test_post_qa.py tests/computation/test_spot_check.py -v`
- Market API smoke test: `curl https://data.jslwealth.in/api/v1/market/...` (identify the endpoint reading `above_50dma` during chunk 6 prep)
- GENERATED consistency assertion: `SELECT COUNT(*) FROM de_equity_technical_daily WHERE sma_50 IS NOT NULL AND above_50dma != (close_adj > sma_50)` — must return 0
- Same assertion for `above_200dma`, `above_20ema`, `price_above_vwap`, `rsi_overbought`, `rsi_oversold`, `macd_bullish`, `adx_strong_trend`

---

## Fix 11: Expanded golden-test indicator set (chunk 4)

**Required** in chunk 4:
- Pin `pandas-ta-classic==<exact-version>` in `pyproject.toml` (in chunk 1) — not `>=`
- Expand golden test beyond 8 indicators to cover at least one per family:
  - **Core (from plan)**: RSI(14), MACD, ADX(14), BBands(20,2), ATR(14), OBV, SMA(50), EMA(20)
  - **Added**: CCI(20), TSI(13,25), Supertrend(10,3), Aroon(14), Donchian(20), Keltner(20,2), ZLMA(20), KAMA(20), HV(20), LinearReg(20), Skew(20), Kurtosis(20), VWAP, MFI(14), Williams %R(14), Ultimate Oscillator
- For families where TA-Lib doesn't implement the indicator (e.g., ZLMA, KAMA, HV), use `pandas-ta-classic`'s own unit-test reference values as the baseline and document in the test

---

## Fix 12: Index spec pins `volume_col=None` (chunk 8 + cross-edit chunk 2)

**Pinned decision**: Indices have no volume. `INDEX_SPEC.volume_col = None`. Indices get NO volume-based indicators.

**Cross-chunk edit**: The `de_index_technical_daily` schema in chunk 2 must NOT contain these columns:
- `obv, ad, adosc_3_10, cmf_20, efi_13, eom_14, kvo, pvt, vwap, price_above_vwap, mfi_14`

If chunk 2 is already applied before this fix lands, a follow-up migration drops those columns from `de_index_technical_daily` only.

---

## Fix 13: MF table strict subset (chunk 10 + cross-edit chunk 2)

**Pinned decision**: `de_mf_technical_daily` schema is a strict subset of the universal schema — it contains only indicators whose `strategy.yaml[entry].applies_to` includes `mf`.

**Cross-chunk edit**: Chunk 2's migration for `de_mf_technical_daily` must exclude ALL OHLC-width-dependent, volume-dependent, and true-range-dependent columns:
- All volume columns (same list as Fix 12)
- `atr_7, atr_14, atr_21, natr_14, true_range, keltner_upper, keltner_middle, keltner_lower`
- `psar, supertrend_10_3, supertrend_direction`
- `cci_20, williams_r_14, ultosc` (these use high/low which equal close in MF case → produce zero/constant output)
- `aroon_up, aroon_down, aroon_osc` (position of high/low over window — degenerate for single-price)

Keep only: single-price overlap (SMA, EMA, DEMA, TEMA, WMA, HMA, KAMA, ZLMA, ALMA), single-price momentum (RSI, MACD, ROC, TSI, CMO, TRIX), single-price volatility (BBands, HV), statistics (zscore, skew, kurt, linreg), risk metrics (empyrical). Plus generated booleans that only reference these columns.

- Chunk 10 adds a test asserting `de_mf_technical_daily` has NO `atr_14` / `obv` / `keltner_upper` / etc. columns (not just NULL — literally absent).
- Chunk 10 adds MF eligibility fixture test: 20 synthetic `de_mf_master` rows covering IDCW/Direct/Regular/Equity/Debt/Index-fund edge cases; assert the eligibility SQL returns exactly the expected subset.

---

## Fix 14: Diff script has unit tests (chunk 5)

**Required**: `scripts/diff_technicals_old_vs_new.py` is load-bearing — a bug in it lets a bad cutover pass. Add unit tests with two tiny synthetic tables where the diff result is known; assert the script's thresholds and exit code behave correctly.

---

## Fix 15: RDS connection drop retry in upserts (chunk 3)

**Required** in chunk 3a:
- Wrap upsert batches with `tenacity.retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30), retry=retry_if_exception_type((OperationalError, InterfaceError)))`
- Log retries to structlog with the batch size and instrument_id range for debugging
- First inspect `app/computation/runner.py` to see if an existing retry helper exists — reuse if so

---

## Fix 16: Chunk plan parallelism claim corrected

**Fix in `indicators-chunk-plan.md`**:
- Remove any suggestion that chunk 4 can run parallel with chunk 3. Chunk 4 depends on chunk 3 (specifically 3a, after the split above).
- Parallelism is limited to: chunk 9 can run parallel to chunks 3b/4/5 once chunks 2 and 9-pre are done; chunk 10 can run parallel to chunk 8 once 3c and 9 are done.

---

## Summary of required pre-build work

Before starting chunk 1, the builder must:
1. Run the Morningstar investigation (Fix 1), file findings, decide chunk 9 split
2. Pin the exact `pandas-ta-classic` version (read its current release, write it into Fix 11 and chunk 1)
3. Review and acknowledge this addendum

After these, chunk 1 can start. All other fixes are applied as the chunks they affect are implemented.
