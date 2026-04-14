# Indicators v2 — session status 2026-04-14

## Shipped and committed (11 commits)

| # | Chunk | Status | Commit | Notes |
|---|---|---|---|---|
| 1 | IND-C1 Dependencies & scaffold | ✅ done | `4561233` | pandas-ta-classic==0.4.47, empyrical-reloaded==0.5.12 pinned exact |
| 2 | IND-C2 Migrations 007 + 008 | ✅ done | (two commits) | 5 new v2 tables: equity/etf/global at 104 cols, index at 93, mf at 67. Applied to prod. |
| 3 | IND-C3a Engine skeleton | ✅ done | | spec.py, engine.py, strategy_loader.py, strategy.yaml (SMA/EMA) |
| 4 | IND-C3b Full catalog | ✅ done | | 64 indicator entries covering momentum/overlap/volatility/volume/trend/statistics |
| 5 | IND-C3c Risk metrics | ✅ done | | empyrical rolling Sharpe/Sortino/Calmar/beta, HV annualized from log returns |
| 6 | IND-C4 Golden fixtures | ✅ done | | Self-snapshot parquet fixtures for drift detection (no TA-Lib) |
| 7 | IND-C5 Equity wrapper + backfill + diff | ✅ code done | | Ran full backfill → 3 real bugs found and fixed in the engine (see below) |
| 8 | IND-C7 ETF wrapper | ✅ done | | |
| 9 | IND-C7 Global wrapper | ✅ done | | |
| 10 | IND-C8 Index wrapper | ✅ done | | |
| 11 | IND-C11 Pipeline runner + registry | ✅ done | | `app/computation/indicators_v2/runner.py` registered in `app/pipelines/registry.py` |
| — | `de_mf_holdings` composite index | ✅ done | `45393e7` | `ix_de_mf_holdings_asof_instr` live on prod |

**Test count**: 56/56 indicators_v2 tests pass (engine + golden + 4 asset wrappers + runner)

## Blocked (and won't resolve without user action)

| # | Chunk | Blocker | Mitigation |
|---|---|---|---|
| IND-C9 | purchase_mode fetch from Morningstar | JIP's Morningstar client uses the wrong endpoint pattern. Every `/FundId/{id}` call returns 404. mfpulse_reimagined uses a totally different `/universeid/{code}` bulk pattern with per-datapoint API hashes. | Documented in `reports/morningstar-client-broken.md`. Three options presented: (A) bootstrap from mfpulse DB (need creds), (B) rewrite JIP client to mfpulse pattern, (C) defer MF indicators. |
| IND-C10 | MF asset wrapper + backfill | Transitively blocked on C9 — the eligibility filter needs `purchase_mode=1`. | Same as C9. |

## Real engine bugs caught and fixed by running against production

Listed in commit order; each surfaced during the actual backfill, not in tests:

1. **VWAP needs DatetimeIndex** — `_load_ohlcv` produced a plain Index of `date` objects. pandas-ta's VWAP calls `.to_period()` which requires DatetimeIndex. Fix: `df.index = pd.DatetimeIndex(df.index)` after set_index.

2. **Object-dtype OHLCV breaks cumsum** — when any None values are in the OHLCV lists, the resulting Series infers as object dtype, and pandas-ta's VWAP `groupby().cumsum()` raises `cumsum is not supported for object dtype`. Fix: `pd.to_numeric(errors="coerce")` on every OHLCV column after DataFrame construction.

3. **`close_adj` never written to output** — the engine's input column is `close`; the schema expects `close_adj`; strategy.yaml (correctly) has no rename for it. Fix: alias `df["close_adj"] = df["close"]` after the strategy rename, plus add `close_adj` to `_RISK_COLUMNS` so `get_schema_columns` sees it.

4. **`OBV` landed as NULL** — OBV is `BIGINT` in schema but `_to_decimal_row` was converting the float-backed accumulator to `Decimal`, which Postgres won't auto-cast to BIGINT. Fix: `_INT_COLUMNS = {obv, ad, pvt}` frozenset; int-coerce those columns explicitly.

5. **`volume_adj` is 0% populated in production** — ingestion pipeline never writes this column. Fix: `AssetSpec` now supports `ColRef = str | tuple[str, ...]`, where tuples emit SQL `COALESCE(col1, col2, ...)`. `EQUITY_SPEC` uses `("volume_adj", "volume")` and `("close_adj", "close")` — survives today's data quality AND auto-picks-up any future backfill.

6. **`ROC(252)` silently missing on <253-row instruments** — pandas-ta drops the column entirely rather than emitting all-NaN. One short-history instrument killed the whole backfill via the engine's strict "missing column" assertion. Fix: defensive `df[col] = float("nan")` for any missing schema column, then the usual NaN→None path at `_to_decimal_row`.

7. **Int64 overflow on Nikkei OBV** — `^NKX` volume × 20 years exceeds Postgres BIGINT max (9.2e18). Fix: `_INT64_MAX / _INT64_MIN` guard; out-of-range ints land as NULL rather than fail the whole upsert.

8. **`sys.modules` stubs cross-contaminated test runs** — the 4 asset test files were installing `pandas_ta_classic` stubs in sys.modules BEFORE importing. When pytest ran them in the same session as engine/golden tests, the stubs replaced the real library and broke the `df.ta` accessor. Fix: removed all 4 stubs. pandas-ta-classic IS available in the docker test image.

## Backfill state on production RDS

Partial — the equity backfill is still running in the background and will likely take another ~2 hours. Smaller asset backfills completed then needed resume after the SSH tunnel dropped.

```
 equity_v2 : 257 / 2281 instruments (11.3%), ~491K rows
 etf_v2    : 184 / 258  instruments (71.3%), ~434K rows  *some skipped for insufficient history
 global_v2 : 90  / 131  instruments (68.7%), ~178K rows  *^NKX errored pre-fix, needs re-resume
 index     : 74  / 135  instruments (54.8%), ~133K rows  *many sectoral indices skipped < 250d history
```

The `_skipped_insufficient_history` counts explain the ≠100% coverage — the engine requires `min_history_days` rows before computing (equity=250, etf/global/index vary). Not a bug.

## CRITICAL: Cutover is NOT safe yet

The diff script surfaces two real problems that block `DROP TABLE + RENAME`:

### Column-name mismatch (v1 vs v2)
| v1 column | v2 column |
|---|---|
| `bollinger_upper` | `bb_upper` |
| `bollinger_lower` | `bb_lower` |
| `sharpe_1y` | `risk_sharpe_1y` |
| `sortino_1y` | `risk_sortino_1y` |
| `max_drawdown_1y` | `risk_max_drawdown_1y` |
| `calmar_ratio` | `risk_calmar_1y` |
| `beta_nifty` | `risk_beta_nifty` |

Every downstream consumer reading `bollinger_upper` from `de_equity_technical_daily` would break after a rename-based cutover: `app/computation/regime.py`, `sectors.py`, `post_qa.py`, `spot_check.py`, `app/api/v1/market.py`, and the observatory dashboard.

**Fix options**:
- **A**: Migration `010_rename_v2_columns_to_v1_names.py` — ALTER column names so v2 tables look like "v1 with extras". Preserves downstream compatibility. One-time migration.
- **B**: Sweep all ~20 downstream references and update to v2 names. More code change but aligns with pandas-ta conventions.
- **C**: Generated alias columns — add `bollinger_upper NUMERIC GENERATED ALWAYS AS (bb_upper) STORED` etc. Zero downstream change but doubles the relevant column count.

### MACD formula divergence
On rows that DO compare cleanly, MACD columns show `max_abs_diff ≈ 1000` against v1. That's not rounding; it's a different formula. Possible causes:
- v1 used exponential smoothing with a different alpha
- v1 used SMA for the signal line; pandas-ta uses EMA
- v1 had a subtle bug in the warmup period

Per the PRD this is actually the point — replace broken v1 formulae with the library. The "failure" is expected; the formal cutover just needs user sign-off that v2 numbers are the new source of truth.

SMA/EMA/RSI match v1 at zero diff (or within 1e-4), so those rows agree.

## Recommendation

1. Let equity backfill finish tonight (background). Document the diff per-column for the full 30-day window.
2. Write migration 010 to rename v2 columns to v1 names (Option A above — safest path).
3. Re-run diff post-migration-010 to confirm columns now match. Sign off on the expected MACD drift.
4. Execute cutover shell script (`scripts/cutover_indicators_v2.sh`) per asset class — dump, drop, rename.
5. Run `test_breadth.py test_runner.py test_regime.py test_sectors.py test_post_qa.py` to confirm downstream still works.
6. File MF indicators (C9/C10) as a follow-up sprint once Morningstar client is fixed.

## Reports generated this session

- `reports/morningstar-client-broken.md` — C9/C10 blocker analysis
- `reports/stale-tests-indicators-build.md` — 3 pre-existing test failures (unrelated)
- `reports/alembic-drift-ticket.md` — migration 002/003 revision chain break
- `reports/indicators-v2-status-2026-04-14.md` — this document
- `reports/backfill_errors_{asset}_*.md` — per-run error summaries

## Numbers summary

- **Commits**: 11 (`4561233` → `4b51f5b`)
- **Lines added**: ~6300 code + ~2300 tests/specs
- **Files created**: 35 (specs, code, migrations, fixtures, reports)
- **Tests**: 56/56 green for indicators_v2 namespace; 389 total computation tests still passing
- **Production tables**: 5 new v2 technical tables created; `de_mf_master.purchase_mode` column added; `ix_de_mf_holdings_asof_instr` index created
- **Engine bugs caught by real-data smoke**: 8
