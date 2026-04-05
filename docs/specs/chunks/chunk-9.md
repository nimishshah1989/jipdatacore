# Chunk 9: Supporting Pipelines

**Layer:** 3
**Dependencies:** C4
**Complexity:** Medium
**Status:** pending

## Files

- `app/pipelines/indices/__init__.py`
- `app/pipelines/indices/nse_indices.py`
- `app/pipelines/indices/vix.py`
- `app/pipelines/flows/__init__.py`
- `app/pipelines/flows/fii_dii.py`
- `app/pipelines/flows/fo_summary.py`
- `app/pipelines/global_data/__init__.py`
- `app/pipelines/global_data/yfinance_pipeline.py`
- `app/pipelines/global_data/fred_pipeline.py`
- `app/pipelines/trading_calendar.py`
- `tests/pipelines/indices/test_nse_indices.py`
- `tests/pipelines/flows/test_fii_dii.py`
- `tests/pipelines/global_data/test_yfinance.py`

## Acceptance Criteria

- [ ] **NSE index prices (Track C):** Download historical and daily prices for all 60+ NSE indices; `INSERT INTO de_index_prices ON CONFLICT (date, index_code) DO UPDATE`
- [ ] **India VIX (Track C):** Fetch India VIX historical and daily values from NSE; `INSERT INTO de_macro_values` where ticker = 'INDIAVIX'
- [ ] **FII/DII flows (Track D):** Primary source: NSE `fiidiiTradeReact` API; fallback to SEBI CSV download on `403` response; `INSERT INTO de_institutional_flows ON CONFLICT DO UPDATE`; covers equity, debt, hybrid market types
- [ ] **F&O summary (Track E, v1.7):** Fetch NSE option chain for NIFTY and BANKNIFTY; compute PCR (put/call ratio by OI and by volume), total OI, OI change, max pain, FII derivatives positions; `INSERT INTO de_fo_summary ON CONFLICT DO UPDATE`
- [ ] **T+1 delivery pipeline (trigger: 09:00 IST):** Get last trading day from `de_trading_calendar`; download NSE delivery data; `UPDATE de_equity_ohlcv SET delivery_vol, delivery_pct WHERE date = last_trading_day`
- [ ] **Pre-market global pipeline (trigger: 07:30 IST, SLA: 08:00 IST):**
  - yfinance tickers: `^GSPC, ^IXIC, ^DJI, ^FTSE, ^GDAXI, ^FCHI, ^N225, ^HSI, 000001.SS, ^AXJO, EEM, URTH`
  - yfinance macro: `DX-Y.NYB, CL=F, BZ=F, GC=F, SI=F, USDINR=X, USDJPY=X, EURUSD=X, USDCNH=X`
  - FRED: `DGS10, DGS2, FEDFUNDS, T10Y2Y, CPIAUCSL, UNRATE`
  - `INSERT INTO de_global_prices ON CONFLICT DO UPDATE`
  - `INSERT INTO de_macro_values ON CONFLICT DO UPDATE`
  - After insert: `UPDATE de_market_regime` global_score for today
  - Invalidate Redis: `global:indices`, `global:macro`
- [ ] **Trading calendar management:** Utility functions to seed annual NSE holiday list; support for ad-hoc Saturday special sessions; `populate_trading_calendar(year)` function
- [ ] All dates normalised to calendar date (US data stored as US calendar date, Indian as Indian calendar date) — never IST-converted
- [ ] Each pipeline track (C, D, E) runs isolated — failure does not affect other tracks
- [ ] All tests use mocked HTTP responses — no live network calls

## Notes

**NSE index list (all 60+ indices):** Includes NIFTY 50, NIFTY NEXT 50, NIFTY 100, NIFTY 200, NIFTY 500, NIFTY MIDCAP, NIFTY SMALLCAP, NIFTY BANK, NIFTY IT, NIFTY PHARMA, NIFTY FMCG, NIFTY AUTO, NIFTY REALTY, NIFTY METAL, NIFTY ENERGY, NIFTY INFRA, INDIA VIX, and all sectoral/thematic indices. Fetch list dynamically from NSE API to avoid hard-coding.

**NSE index price URL:** `https://archives.nseindia.com/content/indices/{index_code}{DATE}.csv` — pattern varies by index. Alternatively use `https://www.nseindia.com/api/allIndices` for current day snapshot.

**FII/DII primary URL:** `https://www.nseindia.com/api/fiidiiTradeReact` (requires NSE session cookie). If 403, fallback to SEBI bulk deal CSV or NSE Excel download.

**F&O option chain URL:** `https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY` and `?symbol=BANKNIFTY`. JSON response contains strike-wise OI data. Max pain = strike where total OI loss is minimum for option sellers.

**PCR calculation:**
- `pcr_oi = total_put_oi / total_call_oi`
- `pcr_volume = total_put_volume / total_call_volume`

**FRED API:** `https://api.stlouisfed.org/fred/series/observations?series_id={SERIES}&api_key={FRED_API_KEY}&sort_order=desc&limit=5&file_type=json`. Store `FRED_API_KEY` in AWS Secrets Manager.

**yfinance usage:** Use `yfinance.download(tickers, period='5d', interval='1d')` for recent data. For historical backfill: `yfinance.Ticker(symbol).history(start='2010-01-01')`. Rate limit: ~2000 requests/hour.

**Global ETF universe:** Top 200 ETFs by AUM (SPY, QQQ, IWM, EFA, EEM, AGG, TLT, GLD, etc.). Seed `de_global_instrument_master` with this list before running pipeline. Local Stooq data files in `/home/ubuntu/global-pulse/` contain historical data — requires Stooq symbol → instrument_type mapping before ingestion.

**Pipeline schedule summary:**
- Pre-market global: 07:30 IST (SLA: 08:00)
- T+1 delivery: 09:00 IST
- EOD (indices, flows, F&O): 18:30 IST (within main EOD pipeline Tracks C, D, E)
