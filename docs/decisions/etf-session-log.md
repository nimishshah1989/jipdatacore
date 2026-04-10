# ETF Universe Expansion — Session Log

Build started: 2026-04-10
Build completed: 2026-04-10

## Chunks

### ETF-C1: NSE India ETF Master + OHLCV Sync
- **Status:** Done
- **Files created:** scripts/ingest/nse_etf_master.py, app/pipelines/etf/nse_etf_sync.py
- **Files modified:** app/pipelines/etf/__init__.py
- **Tests:** 28 tests (test_nse_etf.py)
- **Key decision:** NSE ETFs use yfinance .NS suffix, NOT BHAV copy — de_instrument doesn't contain ETF symbols
- **Bugs found:** DATABASE_URL_SYNC has postgresql+psycopg2:// prefix, psycopg2 rejects it — added prefix stripping

### ETF-C2: Global ETF Expansion + Backfill
- **Status:** Done
- **Files created:** scripts/ingest/etf_backfill.py
- **Files modified:** scripts/ingest/etf_ingest.py
- **Tests:** 29 tests (test_etf_backfill.py)
- **Key decision:** yfinance max period for backfill (skip Stooq tars) — simpler, sufficient coverage
- **Result:** 31 new global ETFs added (fixed income, thematic, frontier)

### ETF-C3: Enrichment Script
- **Status:** Done
- **Files created:** scripts/ingest/etf_enrich.py
- **Tests:** 40 tests (test_etf_enrich.py)
- **Key decision:** expense_ratio=0.0 is valid (use `is not None`, not truthiness)
- **Result:** category populated for ~60% of tickers, expense_ratio for ~40%

### ETF-C4: Pipeline Wiring + Scheduler
- **Status:** Done
- **Files modified:** app/pipelines/registry.py
- **Key decision:** nse_etf_sync placed before etf_prices in EOD schedule
- **Bug documented:** CronSchedule.default() is out of sync with SCHEDULE_REGISTRY — broader fix needed

### ETF-C5: Deploy + End-to-End Verification
- **Status:** Done
- **Deployment:** docker cp into running container + restart
- **Verification:** 258 ETFs in master, 220 active, 435,746 OHLCV rows, technicals + RS computed
- **Key decision:** Deactivated 38 NSE ETFs unavailable on yfinance (ICICI, HDFC, Kotak, UTI variants)
- **Bug found:** etf_backfill.py used Python 3.10+ union types — added __future__ annotations for 3.9 compat

### ETF-C6: Observatory Metadata Fix
- **Status:** Done
- **Files modified:** app/api/v1/observatory.py, app/static/observatory.html, docs/data-map.html
- **Key decision:** Made tree data counts dynamic from coverage API instead of hardcoded

## Design Decisions

1. **NSE ETF source: yfinance .NS, not BHAV copy** — de_instrument only has equities, not ETFs. The BHAV master_refresh pipeline loads from EQUITY_L.csv which excludes ETFs.
2. **Backfill: yfinance max, not Stooq tars** — Same ticker format for US ETFs, covers all exchanges, avoids SCP/parse complexity.
3. **38 NSE ETFs deactivated** — Smaller AMC ETF variants (ICICI, HDFC, Kotak, UTI series) don't exist on yfinance. Set is_active=FALSE.
4. **Observatory counts now API-driven** — Removed hardcoded "130 ETFs", "2,743 instruments" etc. Tree meta pulls from coverage API.
5. **etf_prices pipeline modified** — Maps NSE tickers to .NS for yfinance, maps back for DB storage. Single pipeline handles all exchanges.

## Final Metrics

| Metric | Value |
|--------|-------|
| Total ETFs in master | 258 |
| Active ETFs | 220 |
| NSE India ETFs | 58 active |
| Global ETFs | 162 active |
| OHLCV rows | 435,746 |
| Technical indicators | 220 ETFs computed |
| RS scores | 185 ETFs computed |
| Tests written | 97 |
| Tests passing | 97/97 |
| Lint issues | 0 (ETF files) |
| Commits | 4 |
