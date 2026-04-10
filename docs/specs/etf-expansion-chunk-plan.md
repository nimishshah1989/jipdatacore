# ETF Universe Expansion — Chunk Plan

**PRD:** [etf-expansion-prd.md](etf-expansion-prd.md)
**Total Chunks:** 6
**Build Order:** C1 → C2 → C3 (parallel C2) → C4 (after C1+C2) → C5 (after C4) → C6

---

## Dependency Graph

```
C1 (NSE ETF Master)  ──┐
                        ├──→ C4 (Pipeline Wiring) ──→ C5 (Deploy + Verify)
C2 (Global Expansion) ─┘
C3 (Enrichment Script)     ← independent, run anytime after C1+C2
C6 (Observatory Fix)       ← independent, can run anytime
```

---

## C1: NSE India ETF Master + OHLCV Sync

**Complexity:** Medium
**Files:**
- NEW: `scripts/ingest/nse_etf_master.py`
- NEW: `app/pipelines/etf/nse_etf_sync.py`
- MODIFY: `app/pipelines/etf/__init__.py` (export new pipeline)

**Description:**
1. Create NSE_ETFS dict with ~67 curated NSE ETFs (ticker, name, exchange, country, currency, sector, category, benchmark)
2. Seeder function: upsert into de_etf_master with ON CONFLICT
3. Create NseEtfSyncPipeline (BasePipeline subclass):
   - SQL INSERT...SELECT from de_equity_ohlcv JOIN de_instrument WHERE current_symbol IN NSE ETF tickers
   - ON CONFLICT (date, ticker) DO UPDATE
   - Supports full history (first run) and daily (business_date only)
4. Verify: run seeder, run sync, check row counts

**Acceptance Criteria:**
- [ ] 67+ NSE ETFs in de_etf_master with exchange='NSE'
- [ ] de_etf_ohlcv has NSE ETF rows (historical from de_equity_ohlcv)
- [ ] NseEtfSyncPipeline.execute() returns rows_processed > 0
- [ ] No duplicate rows (ON CONFLICT works correctly)
- [ ] ruff + mypy clean

---

## C2: Global ETF Expansion + Backfill

**Complexity:** Medium
**Files:**
- MODIFY: `scripts/ingest/etf_ingest.py` (add ~33 new entries to ETFS dict)
- NEW: `scripts/ingest/etf_backfill.py`

**Description:**
1. Add ~33 new tickers to ETFS dict:
   - Fixed income: AGG, BNDX, TIP, SHY, BND
   - Commodities: PDBC, PPLT, WEAT
   - Thematic: ARKK, BOTZ, ROBO, DRIV, LIT, CIBR, BUG, GNOM, BLOK, URA, ARKX, QCLN, JETS, MSOS, XHE, CLOU, AIQ, FINX, IBIT
   - Frontier: FM, ENZL, PAK, NGE
2. Run etf_ingest.py to seed de_etf_master (163 global tickers total)
3. Create etf_backfill.py:
   - Accept --tickers (comma-sep) or --new-only flag
   - yfinance download period="max", batch size 50
   - Upsert into de_etf_ohlcv
   - Log ticker, date range, row count per ticker
4. Run backfill for all 33 new tickers

**Acceptance Criteria:**
- [ ] ETFS dict has 163 entries (130 + 33)
- [ ] de_etf_master has 163 global ETFs
- [ ] de_etf_ohlcv has historical data for new tickers (5+ years where available)
- [ ] etf_backfill.py handles yfinance errors gracefully (skip + log)
- [ ] ruff + mypy clean

---

## C3: Enrichment Script

**Complexity:** Low
**Files:**
- NEW: `scripts/ingest/etf_enrich.py`

**Description:**
1. Query de_etf_master for all active tickers
2. For each ticker, call yf.Ticker(ticker).info
3. Extract: category, sector (quoteType-dependent), expenseRatio, fundInceptionDate, currency, longName
4. UPDATE de_etf_master SET category=..., expense_ratio=..., etc.
5. Throttle: 1 req/sec, retry on 429, continue on error
6. Log: tickers enriched, tickers failed, fields populated

**Acceptance Criteria:**
- [ ] category populated for 50%+ of tickers
- [ ] expense_ratio populated for 40%+ of tickers
- [ ] Script handles .NS suffix for NSE tickers (NIFTYBEES → NIFTYBEES.NS for yfinance)
- [ ] No crashes on missing/null info fields
- [ ] ruff + mypy clean

---

## C4: Pipeline Wiring + Scheduler

**Complexity:** Low
**Files:**
- MODIFY: `app/pipelines/registry.py`
- MODIFY: `app/orchestrator/scheduler.py`

**Description:**
1. Register NseEtfSyncPipeline in _PIPELINE_CLASSES
2. Add `nse_etf_sync` to SCHEDULE_REGISTRY["eod"]
3. Add DAG dependency: nse_etf_sync depends on equity_bhav
4. Ensure etf_prices is in CronSchedule.default().eod.pipelines (fix existing gap)
5. Verify: pipeline can be triggered via orchestrator API

**Acceptance Criteria:**
- [ ] `nse_etf_sync` in registry, importable, instantiable
- [ ] DAG shows nse_etf_sync after equity_bhav
- [ ] etf_prices also in CronSchedule.default() eod entry (bug fix)
- [ ] No circular dependencies in DAG
- [ ] ruff + mypy clean

---

## C5: Deploy + End-to-End Verification

**Complexity:** Medium
**Files:**
- All files from C1-C4 deployed to EC2
- Run verification queries on production DB

**Description:**
1. SCP all changed files to EC2 (13.206.34.214)
2. Docker compose restart
3. Run NSE ETF seeder on production
4. Run NSE OHLCV sync (full history)
5. Run global ETF seeder (etf_ingest.py)
6. Run backfill for new global tickers
7. Run enrichment
8. Run etf_technicals on expanded universe
9. Run etf_rs on expanded universe
10. Verification queries:
    - Total de_etf_master count
    - Total de_etf_ohlcv distinct tickers with recent data
    - etf_technicals row count
    - etf_rs score count
11. Verify next-day: EOD pipeline runs, nse_etf_sync fires, etf_prices fires

**Acceptance Criteria:**
- [ ] 230+ tickers in de_etf_master
- [ ] 200+ tickers with OHLCV from last business day
- [ ] etf_technicals computed for all tickers
- [ ] etf_rs scores for all tickers
- [ ] Observatory dashboard green for etf_ohlcv
- [ ] No errors in pipeline logs

---

## C6: Observatory Metadata Fix

**Complexity:** Low
**Files:**
- MODIFY: `app/api/v1/observatory.py`

**Description:**
1. Fix etf_ohlcv stream field descriptions — remove phantom `aum_cr` and `tracking_error` refs
2. Update field list to match actual de_etf_ohlcv columns: date, ticker, open, high, low, close, volume
3. Update data dictionary entry

**Acceptance Criteria:**
- [ ] Observatory API returns correct field descriptions for etf_ohlcv
- [ ] No references to non-existent columns
- [ ] ruff clean

---

## Summary

| Chunk | Name | Complexity | Files | Depends On |
|-------|------|-----------|-------|------------|
| C1 | NSE ETF Master + OHLCV Sync | Medium | 3 | None |
| C2 | Global ETF Expansion + Backfill | Medium | 2 | None |
| C3 | Enrichment Script | Low | 1 | C1, C2 |
| C4 | Pipeline Wiring + Scheduler | Low | 2 | C1 |
| C5 | Deploy + Verify | Medium | 0 (ops) | C1-C4 |
| C6 | Observatory Fix | Low | 1 | None |

**Build order:** C1 and C2 can run in parallel. C6 is independent. C3 after C1+C2. C4 after C1. C5 last.

**Estimated total: 6 chunks, ~4 new files, ~4 modified files.**
