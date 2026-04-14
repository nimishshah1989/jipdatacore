# Chunk 5 — Equity asset wrapper + backfill + diff

**Complexity**: L
**Blocks**: chunk-6
**Blocked by**: chunk-4

## Goal
First real production run of the new engine. Write the equity asset wrapper, the chunked backfill script, and the old-vs-new diff tool. Backfill all 2,281 equities from 2007 into `de_equity_technical_daily_v2`, then diff the last 30 days against the existing `de_equity_technical_daily` and confirm the numbers match within tolerance.

## Files
- **Create**: `app/computation/indicators_v2/assets/equity.py`
  - ~30 lines
  - `EQUITY_SPEC = AssetSpec(source_model=DeEquityOhlcv, output_model=DeEquityTechnicalDailyV2, id_column="instrument_id", date_column="date", close_col="close_adj", open_col="open_adj", high_col="high_adj", low_col="low_adj", volume_col="volume_adj", min_history_days=250, asset_class_name="equity")`
  - `async def compute_equity_indicators(session, instrument_ids=None, from_date=..., to_date=...)` — thin wrapper calling `engine.compute_indicators(EQUITY_SPEC, ...)`
  - Default `instrument_ids=None` means "all active tradeable instruments"
- **Create**: `scripts/backfill_indicators_v2.py`
  - CLI: `python scripts/backfill_indicators_v2.py --asset {equity|etf|global|index|mf} --from YYYY-MM-DD [--to YYYY-MM-DD] [--instrument-id ID] [--workers N]`
  - Creates/reads `backfill_cursor` table (3 cols: `asset_class`, `last_id`, `updated_at`) for resumability
  - Iterates instruments sequentially (default) or via `multiprocessing.Pool(processes=workers)` (default 2)
  - Each worker spawns its own async session (no shared state)
  - Per-instrument: load full history → compute → upsert → update cursor → free DataFrame
  - Progress bar via `tqdm`
  - Logs start/end to `de_pipeline_log`
  - Exits non-zero on any instrument error (so cron catches failures)
- **Create**: `scripts/diff_technicals_old_vs_new.py`
  - CLI: `python scripts/diff_technicals_old_vs_new.py --asset equity --last-days 30`
  - Joins `de_equity_technical_daily` (old) with `de_equity_technical_daily_v2` (new) on `(instrument_id, date)`
  - For each of the 40 overlapping columns (sma_50, sma_200, ema_20, rsi_14, macd_line, macd_signal, macd_histogram, adx_14, plus_di, minus_di, bollinger_upper, bollinger_lower, atr_14, obv, mfi_14, roc_5, roc_10, roc_21, roc_63, volatility_20d, volatility_60d, stochastic_k, stochastic_d, bollinger_width, disparity_20, disparity_50, rsi_7, rsi_9, rsi_21, sharpe_1y, sortino_1y, max_drawdown_1y, calmar_ratio, beta_nifty, relative_volume): computes `count_rows, max_abs_diff, mean_abs_diff, p95_abs_diff, pct_within_1e_4, pct_within_1e_3`
  - Outputs a markdown table report + saves to `reports/technicals_diff_equity_{date}.md`
  - Exits 0 if all core indicators pass thresholds, non-zero otherwise

## Pass thresholds for diff
| Column family | Threshold | Minimum pct |
|---|---|---|
| Core (RSI/SMA/EMA/MACD/BBands/ATR) | 1e-4 | 99.5% |
| Secondary (ADX/Stoch/MFI/DI) | 1e-3 | 99.0% |
| Risk (Sharpe/Sortino/beta) | 1e-2 | 95.0% |
| OBV | 1% relative | 99.0% |

## Backfill strategy
- Equities: 2,281 instruments × ~4,800 rows each
- Per-instrument ETA: ~1.5s (load + compute + upsert)
- Total with 2 workers: ~30 min
- Memory: <100 MB per worker
- Run during off-hours to avoid cron contention

## Acceptance criteria
- `app/computation/indicators_v2/assets/equity.py` — passes golden tests (reuse chunk-4 tests, now parameterized with equity spec)
- `scripts/backfill_indicators_v2.py --asset equity --from 2007-01-01` completes successfully, reports:
  - `instruments_processed` ≈ 2,281 (allow ±5 for delisted)
  - `rows_written` > 8,000,000
  - `errors` = 0
- `scripts/diff_technicals_old_vs_new.py --asset equity --last-days 30` exits 0 (all thresholds pass)
- Markdown report in `reports/` shows per-column parity
- `pytest tests/computation/` — still green
- Spot-check: RELIANCE 2026-04-06 RSI(14) matches old table within 1e-4

## Verification commands
```bash
python scripts/backfill_indicators_v2.py --asset equity --from 2007-01-01 --workers 2
python scripts/diff_technicals_old_vs_new.py --asset equity --last-days 30
cat reports/technicals_diff_equity_*.md
```

## What NOT to do in this chunk
- Do NOT drop the old table yet — that's chunk 6
- Do NOT touch ETF/global wrappers yet — chunk 7
- Do NOT modify `runner.py` to call the new engine — chunk 11
