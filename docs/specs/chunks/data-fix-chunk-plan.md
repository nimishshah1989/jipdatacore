# Data Fix — Chunk Plan

## Execution Order (dependency-based)

### Wave 1 — Independent, run in parallel
- **Chunk A**: BHAV re-ingestion (10 years, all 2281 stocks) — ~2 hours
- **Chunk B**: Index constituents for all 123 indices from NSE — ~15 min
- **Chunk C**: yfinance sector/industry fetch for uncovered stocks (batches of 200) — ~20 min
- **Chunk D**: Create instrument entries for 232 international ISINs from MF holdings

### Wave 2 — After Wave 1 completes
- **Chunk E**: Sector mapping — merge index-based + yfinance into de_instrument.sector/industry for all stocks
- **Chunk F**: ISIN resolver — link de_mf_holdings.instrument_id to de_instrument via ISIN
- **Chunk G**: Corporate action adjustments — compute cumulative adj_factor, backfill close_adj

### Wave 3 — After Wave 2 completes  
- **Chunk H**: Vectorized recomputation of ALL metrics (technicals, RS, breadth, regime, fund derived) with complete data
- **Chunk I**: Cross-validation against MarketPulse + Morningstar

## Chunk Details

### Chunk A: BHAV Re-ingestion
- Run `bhav_backfill.py --start-date 2016-04-01 --force`
- Now 2281 stocks in symbol map → all will be ingested
- Expected: ~2500 days × ~1800 stocks = ~4.5M rows
- ON CONFLICT DO UPDATE handles existing 640-stock data
- Runs inside Docker container on EC2

### Chunk B: Index Constituents (all 123 indices)  
- Fetch from NSE: niftyindices.com constituent CSVs for ALL sectoral/thematic indices
- Parse Company Name, Industry, Symbol, ISIN from each CSV
- Upsert into de_index_constituents
- Also extract Industry field where available → store for sector mapping in Chunk E

### Chunk C: yfinance Sector/Industry
- Target: ~872 stocks not covered by index constituents
- Batch: 200 stocks, 0.3s delay, 5-min cooldown between batches
- Store: Yahoo sector + Yahoo industry per ISIN
- Retry failures once after cooldown

### Chunk D: International Instrument Entries
- 232 ISINs in MF holdings not in de_instrument (non-INE prefix)
- Create de_instrument entries with: isin, holding_name as company_name, exchange='INTL'
- Sector from Morningstar sector_code (already in de_mf_holdings)

### Chunk E: Sector Mapping (merge all sources)
- Priority: NSE index-based sector (363 stocks) > yfinance mapped (872 stocks) > Morningstar (fallback)
- Yahoo industry → NSE sector mapping table (49 industries → 20 sectors)
- UPDATE de_instrument SET sector = ..., industry = ... for all stocks

### Chunk F: ISIN Resolver
- UPDATE de_mf_holdings SET instrument_id = (SELECT id FROM de_instrument WHERE isin = de_mf_holdings.isin), is_mapped = TRUE
- Expected: ~1438 ISINs resolve (1206 Indian + 232 international)

### Chunk G: Corporate Action Adjustments
- Filter: splits (501) + bonuses (470) with ratio_from/ratio_to
- Compute adj_factor where missing: bonus → ratio_from/(ratio_from+ratio_to), split → ratio_from/ratio_to
- Build de_adjustment_factors_daily: cumulative product per instrument ordered by ex_date DESC
- UPDATE de_equity_ohlcv SET close_adj = close * cum_adj_factor, open_adj = open * cum_adj_factor, etc.

### Chunk H: Vectorized Recomputation
- Same approach as batch_compute.py but with full dataset (~2000 stocks, 2500 days)
- Technicals (SMA50/200, EMA20) with close_adj
- RS scores (vs NIFTY 50/500/MIDCAP 100)  
- Breadth (advance/decline, DMA counts, 52w highs/lows, McClellan)
- Regime (with real component scores where data available)
- Fund derived (holdings-weighted RS, manager alpha, Sharpe/Sortino/Beta/Drawdown, sector exposure)
- Sector RS (aggregated from stock-level)

### Chunk I: Cross-validation
- MarketPulse: 50% stock price match across 2000+ stocks
- MarketPulse: sector RS direction comparison (20 sectors)
- Morningstar: risk metrics for 10 large-cap funds (with correct XML API)
- yfinance: SMA50 spot-check for blue chips
- Self-consistency: RS composite, breadth arithmetic, regime classification
