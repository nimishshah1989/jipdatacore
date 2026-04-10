# ETF Universe Expansion — PRD

**Date:** 2026-04-10
**Version:** 1.0
**Design Doc:** [etf-expansion-design-doc.md](etf-expansion-design-doc.md)

---

## 1. Objective

Expand JIP's ETF coverage from 130 US-listed ETFs to ~230 ETFs across NSE India + global markets. Enable unified cross-market ETF analysis (Indian advisors comparing NIFTYBEES vs SPY vs EWJ), daily automation for the full universe, and technical/RS scoring on all tickers.

## 2. Success Metrics

| Metric | Target |
|--------|--------|
| Total ETFs in de_etf_master | 230+ |
| ETFs with fresh OHLCV (T-1) | 200+ (accounting for holidays) |
| NSE ETFs with OHLCV from BHAV | 60+ |
| Technical indicators computed | All 230 tickers, 20 indicators each |
| RS scores computed | All 230 tickers, vs SPY + ^SPX |
| Daily pipeline completes without error | 100% |
| Observatory etf_ohlcv stream | Green |

## 3. Scope

### In Scope
1. **NSE India ETF master data** — curate ~67 NSE ETFs, insert into de_etf_master
2. **NSE OHLCV sync** — new pipeline copies BHAV data from de_equity_ohlcv → de_etf_ohlcv
3. **Global ETF expansion** — add ~33 new tickers (fixed income, thematic, commodities)
4. **Historical backfill** — yfinance `max` for new global tickers
5. **Pipeline wiring** — register nse_etf_sync, add to DAG after equity_bhav
6. **Enrichment** — yfinance Ticker.info for category/sector/expense_ratio on all ETFs
7. **Verification** — count checks, freshness checks, technicals/RS run clean

### Out of Scope
- Schema changes to de_etf_master, de_etf_ohlcv, or de_etf_technical_daily
- Adding `instrument_type` to de_instrument (future improvement)
- Leveraged/inverse ETFs
- Real-time intraday ETF data
- Frontend changes to display new ETFs (existing observatory handles it)
- AUM tracking or tracking error computation

## 4. User Stories

1. **As a wealth advisor**, I want to see NIFTYBEES and BANKBEES alongside SPY in the ETF dashboard, so I can compare Indian vs global ETF performance.
2. **As a data analyst**, I want RS scores for NSE gold ETFs (GOLDBEES) vs SPY, so I can identify relative strength across asset classes.
3. **As a portfolio manager**, I want thematic ETF data (AI, uranium, cybersecurity), so I can track emerging themes.
4. **As the system**, the NSE ETF OHLCV sync should run automatically after BHAV ingestion daily, with no manual intervention.

## 5. Technical Requirements

### 5.1 NSE ETF Master Seeder
- Python script with NSE_ETFS dict (mirrors ETFS dict structure)
- Fields: ticker, name, exchange='NSE', country='IN', currency='INR', sector, category, benchmark
- Upsert into de_etf_master with ON CONFLICT (ticker) DO UPDATE
- Log row count before and after

### 5.2 NSE ETF OHLCV Sync Pipeline
- New BasePipeline subclass: `NseEtfSyncPipeline`
- Execute: SQL INSERT...SELECT from de_equity_ohlcv JOIN de_instrument
- Filter: WHERE current_symbol IN (NSE ETFs from de_etf_master)
- Date range: business_date only (daily), or full history on first run
- ON CONFLICT (date, ticker) DO UPDATE
- Return ExecutionResult with rows_processed count
- Register in pipeline registry, add to DAG after equity_bhav

### 5.3 Global ETF Expansion
- Add ~33 new entries to ETFS dict in etf_ingest.py
- Categories: fixed_income, thematic, commodity, frontier
- Run etf_ingest.py to seed de_etf_master

### 5.4 Historical Backfill Script
- New script: etf_backfill.py
- Takes list of tickers (or "all new" flag)
- yfinance download with period="max", batch size 50
- Upsert into de_etf_ohlcv
- Log: ticker, date range loaded, row count

### 5.5 Enrichment Script
- New script: etf_enrich.py
- For each ticker in de_etf_master where category IS NULL or expense_ratio IS NULL
- Call yf.Ticker(ticker).info
- Extract: longName, category, sector, expenseRatio, fundInceptionDate
- UPDATE de_etf_master SET ... WHERE ticker = ...
- Throttle: 1 request/second, retry on HTTP 429

### 5.6 Pipeline Registry & Scheduler
- Register `nse_etf_sync` in PIPELINE_CLASSES
- Add to SCHEDULE_REGISTRY["eod"] after equity_bhav
- Add DAG dependency: nse_etf_sync depends on equity_bhav

### 5.7 Observatory Metadata
- Fix field descriptions for etf_ohlcv stream (remove phantom `aum_cr`, `tracking_error` refs)
- Verify stream shows correct ticker count

## 6. Data Flow

```
[BHAV Download] → de_equity_ohlcv
       ↓
[nse_etf_sync] → de_etf_ohlcv (NSE ETFs only)
       ↓
[etf_prices]   → de_etf_ohlcv (global ETFs via yfinance)
       ↓
[etf_technicals] → de_etf_technical_daily (all 230 ETFs)
       ↓
[etf_rs]         → de_rs_scores (all 230 ETFs vs SPY + ^SPX)
```

## 7. Dependencies & Constraints

- **de_instrument must have NSE ETF symbols**: BHAV pipeline must have already ingested these instruments. If a symbol is missing from de_instrument, the OHLCV copy will silently skip it.
- **No schema migrations**: All existing tables have sufficient columns.
- **EC2 deployment**: SCP changed files + docker compose restart. No new services.
- **yfinance reliability**: Backfill may need retry. Not blocking for daily ops.

## 8. Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| NSE ETF symbols differ between our list and de_instrument.current_symbol | High — sync produces 0 rows | Medium | Verify symbols against DB before building sync query |
| yfinance blocks IP during 163-ticker daily download | Medium — stale global data | Low | Already running 130; add jitter between batches |
| Some curated NSE ETFs are delisted or suspended | Low — master has inactive entries | Low | Set is_active=False for any with no BHAV data in 30 days |
| etf_technicals/etf_rs slow on 230 tickers | Low — compute is vectorized/SQL | Very Low | Current scripts handle 130 in seconds; 230 is linear scaling |

## 9. Rollback Plan

1. **If NSE ETFs cause issues**: DELETE FROM de_etf_master WHERE exchange = 'NSE' — cascades to de_etf_ohlcv and de_etf_technical_daily (FK CASCADE)
2. **If new global ETFs cause issues**: DELETE FROM de_etf_master WHERE ticker IN (...new tickers...) — same cascade
3. **If nse_etf_sync pipeline fails**: Remove from registry, revert scheduler. Global ETFs unaffected.
4. **If daily automation breaks**: etf_prices pipeline is independent of nse_etf_sync. Can disable sync without affecting global ETF flow.

## 10. Acceptance Criteria (Build-Complete Checklist)

- [ ] `SELECT COUNT(*) FROM de_etf_master` returns 230+
- [ ] `SELECT COUNT(*) FROM de_etf_master WHERE exchange = 'NSE'` returns 60+
- [ ] `SELECT COUNT(DISTINCT ticker) FROM de_etf_ohlcv WHERE date >= '2026-04-01'` returns 200+
- [ ] NSE ETFs have OHLCV rows matching de_equity_ohlcv dates
- [ ] `python -m scripts.compute.etf_technicals` completes without error
- [ ] `python -m scripts.compute.etf_rs --benchmark both` completes without error
- [ ] nse_etf_sync registered in pipeline registry
- [ ] nse_etf_sync in DAG after equity_bhav
- [ ] Observatory etf_ohlcv stream shows green (data fresh within 1 business day)
- [ ] etf_enrich.py populates category/expense_ratio for 50%+ of tickers
- [ ] All new code has ruff clean + mypy clean
- [ ] Deploy to EC2 successful, daily cron fires next business day
