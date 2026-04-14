# Chunk 10 — MF asset wrapper + backfill

**Complexity**: M
**Blocks**: chunk-11
**Blocked by**: chunk-3, chunk-9

## Goal
Compute technicals for ~800 eligible regular-growth-equity mutual funds using NAV as the "price" series, populating the new `de_mf_technical_daily` table.

## Files
- **Create**: `app/computation/indicators_v2/assets/mf.py`
  - MF spec is unusual: the "price" is `nav` from `de_mf_nav_daily`. There's no open/high/low — pandas-ta needs OHLC for some indicators. Solution: synthesize `open=high=low=close=nav` so single-price indicators (SMA, EMA, RSI, MACD, ROC, volatility, Bollinger) work. Skip volume-based and OHLC-spread-based (ATR, Keltner, PSAR) via strategy filter `applies_to`.
  - `MF_SPEC = AssetSpec(source_model=DeMfNavDaily, output_model=DeMfTechnicalDaily, id_column="mstar_id", date_column="nav_date", close_col="nav", open_col="nav", high_col="nav", low_col="nav", volume_col=None, min_history_days=250, asset_class_name="mf")`
  - Engine's strategy loader filters out indicators whose `applies_to` yaml doesn't include `mf`. Update `strategy.yaml` in this chunk to add `applies_to: [equity, etf, index, global]` exclusions for ATR, Keltner, OBV, MFI, CMF, VWAP, PSAR, TR (these don't make sense for NAV-only).
  - `async def compute_mf_indicators(session, mstar_ids=None, ...)` — if `mstar_ids=None`, runs the full eligibility filter:
    ```sql
    SELECT m.mstar_id FROM de_mf_master m
    WHERE m.purchase_mode = 1
      AND m.broad_category = 'Equity'
      AND m.is_active AND NOT m.is_etf AND NOT m.is_index_fund
      AND m.fund_name !~* '\b(IDCW|Dividend|Segregated)\b'
      AND EXISTS (SELECT 1 FROM de_mf_nav_daily n WHERE n.mstar_id = m.mstar_id)
    ```
- **Modify**: `app/computation/indicators_v2/strategy.yaml`
  - Add `applies_to` filter to each indicator entry
  - Indicators that apply to all 5: SMA, EMA, RSI, MACD, BBands (single-price OK), ROC, historical vol, risk metrics, zscore, skew, kurtosis, linreg
  - Indicators that apply to equity/etf/index/global only (need OHLC or volume): ATR, NATR, TrueRange, Keltner, Donchian, OBV, AD, ADOSC, CMF, EFI, EOM, KVO, PVT, VWAP, PSAR, CCI, MFI, Williams %R, Ultosc, Supertrend, Aroon
- **Create**: `tests/computation/test_indicators_v2_mf.py`
  - Unit test: synthesize 500 days of single-price NAV, run engine with MF spec, assert expected column set (no ATR/OBV/etc.)
  - Unit test: eligibility filter returns expected mstar_ids given a seeded `de_mf_master`

## Backfill
```bash
python scripts/backfill_indicators_v2.py --asset mf --from 2010-01-01
```
- ~800 funds × avg 3,000 NAV rows × ~60 applicable cols → ~2.4M rows → 20–40 min

## Smoke tests
```sql
-- Eligibility count matches the filter
SELECT COUNT(DISTINCT mstar_id) FROM de_mf_technical_daily;
-- Expect: ~800

-- Spot-check a top equity fund (HDFC Flexi Cap is a common benchmark)
SELECT nav_date, close_adj AS nav, sma_50, sma_200, rsi_14, macd_line, risk_sharpe_1y, risk_sortino_1y
FROM de_mf_technical_daily t
JOIN de_mf_master m USING (mstar_id)
WHERE m.fund_name ILIKE '%HDFC Flexi Cap%Regular%Growth%'
ORDER BY nav_date DESC LIMIT 5;

-- Risk metrics populated for funds with >1y history
SELECT COUNT(*) AS total, COUNT(risk_sharpe_1y) AS with_sharpe
FROM de_mf_technical_daily
WHERE nav_date = (SELECT MAX(nav_date) FROM de_mf_technical_daily);
-- Expect with_sharpe / total > 90%
```

## Acceptance criteria
- `de_mf_technical_daily` row count ~2.4M
- ~800 distinct mstar_ids
- HDFC Flexi Cap Regular Growth latest RSI/MACD/Sharpe all non-null and plausible
- No OHLC-based indicators present (ATR column doesn't exist in MF table — matches strategy.yaml `applies_to` filter)
- `pytest tests/computation/test_indicators_v2_mf.py -v` green
- Full `pytest tests/` still green

## Verification commands
```bash
python scripts/backfill_indicators_v2.py --asset mf --from 2010-01-01
pytest tests/computation/test_indicators_v2_mf.py -v
psql -h ... -c "SELECT COUNT(DISTINCT mstar_id), COUNT(*) FROM de_mf_technical_daily"
```
