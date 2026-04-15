# GAP-08 — Equity fundamentals ingestion via theta-india port

## Goal
Ship a new `de_equity_fundamentals` table populated by porting the working
Screener.in scraper from the user's own `nimishshah1989/theta-india` repo.
theta-india already has ~634 lines of production-tested scraper code
(`india_alpha/fetchers/screener_fetcher.py` + `screener_enricher.py`) with
~100% NSE/BSE coverage. **Port, don't rewrite.**

## Source decision rationale

Research on 2026-04-15 (see `reports/fundamentals_research.md` if committed)
concluded:
- **Morningstar equity fundamentals** — exists as a separate product tier
  (equityapi.morningstar.com), NOT included in JIP's current fund-master
  datapoint contract. Would require new paid contract. Deferred.
- **yfinance** — theta-india tried it first and ripped it out for
  reliability reasons. Use ONLY as fallback/cross-check, not primary.
- **screener.in** — primary source per theta-india. Requires
  `SCREENER_SESSION_COOKIE` from a logged-in browser session. Rate limit
  ~1 req/1.2s → full refresh of 2,200 stocks ≈ 45 min. Fits daily/weekly.
- **OpenBB Terminal** — thin India coverage, more work to build providers
  than to port theta-india directly.

## Scope

### Port from theta-india
1. Copy `india_alpha/fetchers/screener_fetcher.py` → `app/pipelines/fundamentals/screener_fetcher.py`
2. Copy `india_alpha/fetchers/screener_enricher.py` → `app/pipelines/fundamentals/screener_enricher.py`
3. Adapt the SQL schema:
   - theta-india uses FLOAT; JIP convention is `Numeric(18,4)` / `Numeric(10,4)`
   - theta-india `india_companies` table → rename/refactor to JIP's
     `de_equity_fundamentals` naming
4. Port `universe_builder.py` logic to read from `de_instrument` instead
   of theta-india's universe file

### Alembic migration 012: `de_equity_fundamentals`
```
instrument_id UUID PK (FK → de_instrument)
as_of_date DATE PK
-- Valuation
market_cap_cr NUMERIC(18,2)       -- ₹ crore
pe_ratio NUMERIC(10,4)
pb_ratio NUMERIC(10,4)
peg_ratio NUMERIC(10,4)
ev_ebitda NUMERIC(10,4)
-- Profitability
roe_pct NUMERIC(8,4)
roce_pct NUMERIC(8,4)
operating_margin_pct NUMERIC(8,4)
net_margin_pct NUMERIC(8,4)
-- Balance sheet
debt_to_equity NUMERIC(10,4)
interest_coverage NUMERIC(10,4)
current_ratio NUMERIC(10,4)
-- Per-share
eps_ttm NUMERIC(18,4)
book_value NUMERIC(18,4)
face_value NUMERIC(10,2)
dividend_per_share NUMERIC(18,4)
dividend_yield_pct NUMERIC(8,4)
-- Ownership
promoter_holding_pct NUMERIC(6,2)
pledged_pct NUMERIC(6,2)
fii_holding_pct NUMERIC(6,2)
dii_holding_pct NUMERIC(6,2)
-- Growth (TTM or latest FY)
revenue_growth_yoy_pct NUMERIC(10,4)
profit_growth_yoy_pct NUMERIC(10,4)
-- 52-week
high_52w NUMERIC(18,4)
low_52w NUMERIC(18,4)
-- Audit
source VARCHAR(50) DEFAULT 'screener'
created_at / updated_at TIMESTAMPTZ
```

### Pipeline integration
- Register `FundamentalsPipeline(BasePipeline)` in `app/pipelines/registry.py`
- Weekly schedule (not daily — fundamentals change slowly)
- Read `SCREENER_SESSION_COOKIE` from env
- Fallback to yfinance `.info` dict for any symbol Screener fails on

### Also port from theta-india (bonus scope — if time permits)
- `bse_insider.py`, `bse_shareholding_fetcher.py`, `nse_insider_fetcher.py`,
  `nse_shareholding_fetcher.py` — deeper ownership data. Can land in
  separate `de_equity_ownership` table. Track as a follow-up.

## Acceptance criteria
- [ ] `app/pipelines/fundamentals/screener_*.py` exist, ported from theta-india
- [ ] Migration 012 applied to prod
- [ ] `SELECT COUNT(DISTINCT instrument_id) FROM de_equity_fundamentals` ≥ 2,000
- [ ] Latest row per stock has non-NULL pe_ratio, pb_ratio, roe_pct, eps_ttm,
  market_cap_cr
- [ ] Pipeline registered + scheduled weekly
- [ ] `SCREENER_SESSION_COOKIE` env var documented in .env.example (value placeholder)
- [ ] Commit subject starts with `GAP-08`
- [ ] state.db shows GAP-08 DONE

## Steps for the inner session
1. `git clone https://github.com/nimishshah1989/theta-india.git /tmp/theta-india` (shallow OK)
2. Read `/tmp/theta-india/india_alpha/fetchers/screener_fetcher.py` and
   `screener_enricher.py` — understand shape + output fields
3. Read `/tmp/theta-india/schema.sql` for the fundamentals table structure
4. Create `app/pipelines/fundamentals/` package with ported scrapers
5. Alembic migration 012 for `de_equity_fundamentals` (JIP schema conventions:
   Numeric not FLOAT, Decimal conversion at DB boundary)
6. Pipeline class that loads the universe from de_instrument and calls the scrapers
7. Test against 5 known stocks (RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK) first
8. Run full backfill (~45 min for 2,200 stocks at 1.2s each)
9. Register pipeline, weekly schedule
10. Commit + report

## Out of scope
- Historical time-series of fundamentals (snapshot + weekly refresh only)
- OpenBB adoption (researched; not a fit)
- Morningstar equity API (researched; paid, separate contract)
- Corporate announcements / news (GAP-14 follow-up — use BseIndiaApi)
- Peer comparison view (Atlas-side feature)

## Dependencies
- Upstream: none (unblocked as of 2026-04-15)
- Downstream: Atlas deepdive page backend
