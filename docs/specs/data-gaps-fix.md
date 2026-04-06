# JIP Data Core — Complete Data Gaps Fix Plan

## Current Reality (as of 2026-04-06)

### What works
- 640 stocks with OHLCV (should be 2000+)
- MF NAV for 851 funds (20 years)
- Index prices for 83 indices (10 years)
- MF Master for 13,380 funds
- MF Holdings for 838 funds (230K rows with ISIN)
- Corporate actions downloaded (14,923 rows)

### What's broken (every PROBLEM field)

#### Layer 1: Instrument Master (blocks everything else)
1. **sector**: 18% filled (413/2281) — only from index constituent workaround
2. **industry**: ALL NULL — NSE EQUITY_L.csv doesn't have it. Need ind_nifty500list.csv + ind_niftymidsmallcap400list.csv
3. **OHLCV has only 640 instruments** — BHAV ingested before full instrument list existed

#### Layer 2: Price Adjustments (blocks accurate RS/technicals)
4. **close_adj ALL NULL** (1.37M rows) — de_adjustment_factors_daily is empty
5. **adj_factor only 6% filled** in corporate_actions (877/14923)
6. **cash_value ALL NULL** in corporate_actions

#### Layer 3: MF Holdings Link (blocks fund sector exposure, derived RS)
7. **instrument_id ALL NULL** in de_mf_holdings — ISIN resolver never ran
8. **is_mapped ALL FALSE** — same

#### Layer 4: NAV Returns (blocks pre-computed returns)
9. **return_1d through return_10y ALL NULL** in de_mf_nav_daily
10. **nav_adj ALL NULL** — no dividend adjustment
11. **nav_52wk_high/low ALL NULL**

#### Layer 5: Breadth (partially computed)
12. **pct_above_200dma/50dma ALL NULL** — batch compute didn't populate
13. **mcclellan ALL NULL** — same
14. **new_52w_highs/lows all zeros** — same

#### Layer 6: Regime (only breadth-based)
15. **momentum/volume/global/fii scores all = 50** (hardcoded neutral)

---

## Fix Plan — Dependency Order

### Chunk A: Instrument Master + Sector/Industry (do FIRST)
- Download ind_nifty500list.csv → parse Industry column → update de_instrument.industry for 500 stocks
- Download ind_niftymidsmallcap400list.csv → same for 400 more stocks  
- Map industry → sector using NSE's own grouping
- For remaining ~1,400 stocks without industry: use Morningstar sector_code from de_mf_holdings as fallback
- Expected: 900+ stocks with NSE industry, remaining get Morningstar sector

### Chunk B: BHAV Re-ingestion (10 years only: 2016-2026)
- Re-run bhav_backfill with --force --start-date 2016-04-01
- Now all 2,281 instruments in symbol map → will ingest ~1800 stocks/day
- Expected: ~2,500 trading days × ~1,800 stocks = ~4.5M rows (vs current 1.37M)
- Time: ~3 hours (based on previous run speed)

### Chunk C: Corporate Action Adjustments
- Compute adj_factor for splits/bonuses where ratio_from/ratio_to exist (877 have it)
- For dividends: adj_factor = (close - dividend) / close on ex_date  
- Build de_adjustment_factors_daily: cumulative product of adj_factors per instrument
- Backfill close_adj = close × cumulative_adj_factor

### Chunk D: ISIN Resolution for MF Holdings
- Simple SQL: UPDATE de_mf_holdings SET instrument_id = (SELECT id FROM de_instrument WHERE isin = de_mf_holdings.isin), is_mapped = TRUE WHERE isin IN (SELECT isin FROM de_instrument)
- 1,206 of 1,668 unique ISINs match → ~72% coverage

### Chunk E: MF NAV Returns + 52wk
- Compute return_1d through return_10y from NAV series (vectorized pandas)
- Compute nav_52wk_high/low using rolling 252-day window
- MF dividends: need AMFI data to adjust NAV (separate ingestion)

### Chunk F: Recompute Everything
- Technicals with close_adj for all 2000+ stocks
- RS scores with adjusted prices (stddev should normalize to ~1-3)
- Breadth with proper DMA counts, 52w highs/lows, McClellan
- Regime with real momentum/volume/global/fii scores
- Fund derived with proper holdings-weighted RS, sector exposure, manager alpha
- Sector RS from stock-level aggregation

### Chunk G: Cross-validation
- MarketPulse 50% stock price match (should be 2000+ stocks now)
- Morningstar risk data (with correct API URL format)
- Sector RS direction match
- Fund-level Sharpe/Beta/Vol verification
