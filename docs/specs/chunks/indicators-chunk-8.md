# Chunk 8 — Index asset wrapper (new table)

**Complexity**: S
**Blocks**: chunk-11
**Blocked by**: chunk-7

## Goal
Compute 130 indicators directly on `de_index_prices` for all 135 Indian indices and populate the new `de_index_technical_daily` table. No cutover needed — this is a greenfield table. This is the REAL unlock: sectoral indices get proper technicals for the first time.

## Files
- **Create**: `app/computation/indicators_v2/assets/index_.py`
  - Filename uses trailing underscore because `index` is a Python builtin — keep imports clean via `from app.computation.indicators_v2.assets import index_ as index_asset`
  - `INDEX_SPEC = AssetSpec(source_model=DeIndexPrices, output_model=DeIndexTechnicalDaily, id_column="index_code", date_column="date", close_col="close", open_col="open", high_col="high", low_col="low", volume_col="volume", min_history_days=250, asset_class_name="index")`
  - Note: `de_index_prices.volume` is often null/zero for sectoral indices (volume isn't meaningful for a weighted index). Engine's `requires_volume` filter will skip OBV, MFI, CMF, VWAP etc. for indices with no volume — OR we can set volume to 1 for calculation purposes. Prefer the former (cleaner NULL semantics).
  - `async def compute_index_indicators(session, index_codes=None, category=None, ...)` — optional filter by category (sectoral/broad/thematic/strategy)

## Backfill
```bash
python scripts/backfill_indicators_v2.py --asset index --from 2010-01-01
```
- 135 indices × ~4,000 rows × 130 cols (minus volume-based) → ~540K rows → 10–15 min

## Smoke tests (no v1 to diff against)
After backfill, verify via psql:
```sql
-- Coverage
SELECT COUNT(DISTINCT index_code) AS indices, COUNT(*) AS rows, MIN(date), MAX(date)
FROM de_index_technical_daily;
-- Expect: ~135 indices, ~540K rows, 2010-01-01 → yesterday

-- Latest NIFTY 50 indicator snapshot
SELECT date, close_adj, sma_50, sma_200, rsi_14, macd_line, macd_signal, adx_14, bb_upper, bb_lower
FROM de_index_technical_daily
WHERE index_code = 'NIFTY 50'
ORDER BY date DESC LIMIT 5;
-- All values should be non-null, in plausible ranges

-- Sectoral indices breakdown
SELECT m.category, COUNT(DISTINCT t.index_code) AS with_technicals
FROM de_index_master m
LEFT JOIN de_index_technical_daily t ON t.index_code = m.index_code
GROUP BY m.category;
-- Expect: broad 19, sectoral 22, thematic 52, strategy 42 (all rows matching de_index_master)

-- Verify breadth-booleans work
SELECT index_code, close_adj, sma_50, above_50dma
FROM de_index_technical_daily
WHERE date = (SELECT MAX(date) FROM de_index_technical_daily)
  AND index_code IN ('NIFTY 50', 'NIFTY BANK', 'NIFTY IT', 'NIFTY FMCG')
ORDER BY index_code;
```

## Acceptance criteria
- `de_index_technical_daily` row count matches expected ~540K
- All 135 indices present
- NIFTY 50, NIFTY BANK, NIFTY IT latest values sanity-check (RSI 0–100, ADX non-negative, MACD sign matches price trend)
- GENERATED boolean columns (`above_50dma`, etc.) populated
- No NULL in `close_adj` for recent dates (raw close passes through)
- `pytest tests/computation/` — all green

## What this enables
Post this chunk, the dashboard can show:
- RSI of NIFTY Bank (not just its constituents)
- MACD of NIFTY IT (sector momentum)
- ADX of NIFTY FMCG (trend strength)
- Bollinger squeeze on any thematic index
All without faking "sector RSI" by averaging constituent RSIs.

## Verification commands
```bash
python scripts/backfill_indicators_v2.py --asset index --from 2010-01-01
psql -h ... -c "SELECT COUNT(DISTINCT index_code), COUNT(*) FROM de_index_technical_daily"
```
