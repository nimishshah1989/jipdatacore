# Chunk 7 — ETF + global asset wrappers + cutovers

**Complexity**: M
**Blocks**: chunk-8
**Blocked by**: chunk-6

## Goal
Same pattern as equity (chunk 5 + 6) but for `de_etf_technical_daily` and `de_global_technical_daily`. Both are replace-in-place. Smaller universes so this should be fast.

## Files
- **Create**: `app/computation/indicators_v2/assets/etf.py`
  - `ETF_SPEC = AssetSpec(source_model=DeEtfOhlcv, output_model=DeEtfTechnicalDailyV2, id_column="ticker", date_column="date", close_col="close", open_col="open", high_col="high", low_col="low", volume_col="volume", min_history_days=100, asset_class_name="etf")`
  - Note: ETF OHLCV has no `close_adj` — uses raw `close`. Flag in comment. ETFs don't have corp actions in the same sense as equities; for indices/dividends, the reinvestment is reflected in NAV directly.
  - `async def compute_etf_indicators(session, tickers=None, ...)`
- **Create**: `app/computation/indicators_v2/assets/global_.py`
  - `GLOBAL_SPEC = AssetSpec(source_model=DeGlobalPrices, output_model=DeGlobalTechnicalDailyV2, id_column="ticker", date_column="date", close_col="close", open_col="open", high_col="high", low_col="low", volume_col="volume", min_history_days=100, asset_class_name="global")`
  - Covers ALL instrument_types in `de_global_instrument_master`: etf, index, bond, commodity, forex, crypto
  - Volume may be null for forex pairs — engine handles via `requires_volume` strategy filter
  - `async def compute_global_indicators(session, tickers=None, instrument_type=None, ...)` — optional filter by type
- **Create**: `scripts/cutover_etf_global_indicators_v2.sh`
  - Same pattern as equity cutover: dump both old tables, run migration, verify
- **Create**: `alembic/versions/XXX_cutover_etf_global_indicators_v2.py`
  - Two DROP + RENAME pairs in one migration (atomic)
- **Modify**: `app/models/etf.py` — replace `DeEtfTechnicalDaily` with new schema
- **Modify**: `app/models/prices.py` — replace `DeGlobalTechnicalDaily` with new schema
- **Modify**: `app/models/indicators_v2.py` — remove the two v2 twin classes

## Backfill order
1. ETF first (smaller, faster smoke test): `python scripts/backfill_indicators_v2.py --asset etf --from 2015-01-01`
2. Diff: `python scripts/diff_technicals_old_vs_new.py --asset etf --last-days 30`
3. Cutover ETF: `bash scripts/cutover_etf_global_indicators_v2.sh --only etf`
4. Global: `python scripts/backfill_indicators_v2.py --asset global --from 2015-01-01`
5. Diff: `python scripts/diff_technicals_old_vs_new.py --asset global --last-days 30`
6. Cutover global: `bash scripts/cutover_etf_global_indicators_v2.sh --only global`

(The `--only` flag lets us run them sequentially with a gap to validate.)

## Expected instruments and backfill time
- ETFs: 258 × ~2,500 rows × 130 cols → ~650K rows → 5–10 min
- Globals: ~200 active × ~2,500 rows × 130 cols → ~500K rows → 5–15 min (forex without volume is faster)

## Acceptance criteria
- Both backfills complete, `errors=0`
- Both diff reports pass thresholds (same as chunk 5)
- Both cutovers complete, dumps saved
- `de_etf_technical_daily` and `de_global_technical_daily` both have ~130 columns
- GENERATED booleans work on both (`above_50dma`, etc.)
- Spot-check: NIFTYBEES RSI(14), SPY RSI(14), GLD ATR(14) — all non-null
- `test_runner.py`, `test_breadth.py` still green

## Verification commands
```bash
python scripts/backfill_indicators_v2.py --asset etf --from 2015-01-01
python scripts/diff_technicals_old_vs_new.py --asset etf --last-days 30
bash scripts/cutover_etf_global_indicators_v2.sh --only etf
python scripts/backfill_indicators_v2.py --asset global --from 2015-01-01
python scripts/diff_technicals_old_vs_new.py --asset global --last-days 30
bash scripts/cutover_etf_global_indicators_v2.sh --only global
```
