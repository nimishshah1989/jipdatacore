# Data Core Gap Closure — Specification

## Context

Audit against production (2026-04-15) validated 9 gaps in the JIP Data Core
against Atlas's requirements. This spec is the work list to close them in
priority order. Each gap is independently shippable; later gaps unblock
Atlas features but don't block earlier gaps.

Post-indicators-v2 cutover state (2026-04-14):
- Equity/ETF/global technicals are on pandas-ta-classic + empyrical via v2
  tables renamed into their original names
- Index technicals are partial (74/135) — 40 errored in last backfill
- MF technicals are entirely missing (blocked)
- Production has schema migrations 001–010 applied

## Audit-backed gap catalog

### Gap 1 — Upstream index_prices historical backfill (CRITICAL)
**Symptom**: 53 of 135 indices in `de_index_prices` have only ~5 days of
data (MIN(date) ≥ 2026-04-01) despite being declared in `de_index_master`.
Most critical missing: NIFTY PHARMA, NIFTY REALTY, NIFTY PVT BANK,
NIFTY OIL & GAS, NIFTY HEALTHCARE, NIFTY CONSUMER DURABLES,
NIFTY FINANCIAL SERVICES.
**Impact**: sectoral technicals on these 7 key sectors are impossible;
RS and breadth sector aggregation breaks.
**Root cause**: the indices ingestion pipeline (unknown location) was
updated recently to include more indices but doesn't run a historical
backfill for newly-added index_codes. Only forward-looking daily ingestion.
**Files to investigate**: `app/pipelines/indices/`, `scripts/cron/`.
**Data source**: NSE historical index data (niftyindices.com) via
yfinance (`^NSEPHARMA`, etc.) OR direct CSV from NSE archives.
**Done = every index in de_index_master has at minimum 2016-01-01 onwards**
of OHLC + volume where NSE started publishing that index at all.

### Gap 2 — MF purchase_mode bootstrap (UNBLOCKS MF TECHNICALS)
**Symptom**: `de_mf_master.purchase_mode` is NULL for all 13,380 funds.
Migration 007 added the column, but JIP's Morningstar client uses the
wrong endpoint (see `reports/morningstar-client-broken.md`) and no
data has been written.
**Impact**: MF technicals (IND-C10) blocked. Cannot isolate the ~800
equity-regular-growth funds from the 13,380 universe cleanly.
**Root cause**: Morningstar client pattern mismatch with working API
(mfpulse uses `/universeid/{code}` bulk; JIP uses `/{IdType}/{Id}` per-fund).
**Fix**: bootstrap `purchase_mode` from mfpulse_reimagined's `fund_master`
table, which lives on the SAME EC2 (13.206.34.214) at
`/home/ubuntu/mfpulse_reimagined/`. Its DB has `purchase_mode` populated
from the correct Morningstar path.
**Done = UPDATE de_mf_master SET purchase_mode from mfpulse.fund_master**
**After**: run the IND-C10 MF backfill using the bulk/COPY engine, which
should finish in ~15 minutes for ~800 eligible funds.

### Gap 3 — Equity fundamentals ingestion (NEW TABLE)
**Symptom**: NO equity fundamentals table exists in the data core.
Grep across all 148 tables returns nothing for P/E, P/B, ROE, EPS,
debt/equity, dividend yield, market cap at stock level. `de_instrument`
has only symbol/sector/industry/listing flags.
**Impact**: Atlas cannot display valuation or profitability metrics at
individual stock level — a critical feature gap for a wealth platform.
**Source options**:
- Morningstar equity endpoints (if our access code covers them)
- screener.in scrape (reliable, free, ~2,200 stocks coverage)
- yfinance fundamentals (cached Yahoo data, variable coverage)
**Preferred**: screener.in via a scheduled scraper.
**Done = new `de_equity_fundamentals` table with at least P/E, P/B, ROE,
D/E, EPS (TTM), dividend yield, market cap, book value, face value,
for every active NSE listed stock**, refreshed weekly.

### Gap 4 — Multi-year risk metrics (schema + compute extension)
**Symptom**: equity/ETF/global/index v2 tables have only 1-year risk
metrics (`sharpe_1y`, `sortino_1y`, `max_drawdown_1y`, `beta_nifty`,
etc.). No 3-year or 5-year window variants. MF side
(`de_mf_derived_daily`) does have 1y/3y/5y but uses old hand-rolled
formulae.
**Impact**: Atlas risk-screen cannot filter/sort by 3y or 5y metrics;
incomplete for long-term investors.
**Fix plan**:
1. Alembic migration 011: ADD columns `sharpe_3y, sharpe_5y,
   sortino_3y, sortino_5y, calmar_3y, calmar_5y, max_drawdown_3y,
   max_drawdown_5y, volatility_1y, volatility_3y, volatility_5y,
   stddev_1y, stddev_3y, stddev_5y, beta_3y, beta_5y, alpha_3y,
   alpha_5y, treynor_1y/3y/5y, downside_risk_1y/3y/5y,
   information_ratio_3y/5y, omega_3y/5y` on all v2 technical tables.
2. Extend `app/computation/indicators_v2/risk_metrics.py::compute_risk_series`
   to accept a list of windows [(name, days)] = [("1y", 252), ("3y", 756),
   ("5y", 1260)], compute empyrical.roll_* for each, output all columns.
3. Update golden fixtures + tests.
4. Rerun backfill across all 4 asset classes.
**Done = schema has 1y/3y/5y for every risk metric; backfill populated;
Atlas can query 3y Sharpe directly**.

### Gap 5 — Sector mapping reconciliation table
**Symptom**: stocks have 31 JIP-internal sectors (Banking, IT, Pharma,
Automobile...); NSE sectoral indices use different names
(NIFTY BANK, NIFTY IT, NIFTY PHARMA, NIFTY AUTO). No formal mapping.
**Impact**: can't automatically compute "this stock's sector index
technicals" or "aggregate stocks in Banking sector → compare to
NIFTY BANK technicals".
**Fix**: create new table `de_sector_mapping (jip_sector_name VARCHAR(50) PK,
nse_index_code VARCHAR(50) FK → de_index_master(index_code))`
with 31 hand-authored rows covering all JIP sectors. Some JIP sectors
may map to multiple NSE indices (e.g., "Financial Services" → both
NIFTY FIN SERVICE and NIFTY FINSEREXBNK) — use an array column OR
pick the canonical one.
**Done = SELECT nse_index_code FROM de_sector_mapping WHERE jip_sector_name = 'Banking'
returns 'NIFTY BANK', and every JIP sector has at least one mapping.**

### Gap 6 — MF risk metrics migration to empyrical
**Symptom**: `de_mf_derived_daily` has full 1y/3y/5y risk metrics
(sharpe, sortino, max_dd, volatility, beta, treynor, info_ratio) but
was populated by the OLD JIP hand-rolled risk computation — the same
code the user flagged as broken.
**Impact**: MF risk numbers on Atlas are wrong in the same ways v1
technicals were wrong.
**Fix plan**:
1. New `app/computation/mf_risk_v2.py` using empyrical on NAV series
   from `de_mf_nav_daily`.
2. Cutover `de_mf_derived_daily` content — either in-place UPDATE or
   truncate + backfill.
3. Verify vs empyrical scalar calls in a parity test.
**Done = de_mf_derived_daily values come from empyrical, not legacy
formulae.**

### Gap 7 — Repair de_rs_daily_summary (empty)
**Symptom**: `de_rs_daily_summary` has 0 rows. `de_rs_scores` has
14.7M rows. The daily summary view is either broken or never populated.
**Impact**: any downstream consumer reading summary view sees nothing.
**Fix**: investigate the summary population code (likely in
`app/computation/rs.py`), fix the bug, backfill.

### Gap 8 — Global ETF universe expansion
**Symptom**: 83 global ETFs in `de_global_instrument_master` vs target 100+.
**Impact**: Atlas global-ETF screener has narrower universe.
**Fix**: research list of top 100+ traded global ETFs, add to the
ingestion universe, run yfinance backfill for new tickers.

### Gap 9 — Index backfill rerun (40 errors)
**Symptom**: Most recent `backfill_indicators_v2.py --asset index`
had 2 chunks fail with overflow cascade despite the Decimal clamp
added in commit 396e196.
**Impact**: index technicals at 74/135 instead of where it should be.
**Fix**: investigate the overflow (the clamp is at ±999,999.9999,
something exceeds that — likely a degenerate zscore or linreg on a
pathological index series). Tighten or skip that indicator for indices.
Rerun backfill.

## Out of scope for this spec
- `purchase_mode` ingestion from Morningstar directly (requires client
  rewrite — deferred; use mfpulse bootstrap instead for gap 2)
- Morningstar `OperationsMasterFile` integration
- Deletion of old v1 technical code (`technicals.py`) — keep for now
- Breadth/RS rollups per sector (depends on gap 1 + gap 5)

## Sequencing constraints

- Gap 1 and Gap 2 are fully independent — can run in parallel
- Gap 3 (fundamentals) is independent
- Gap 4 (multi-year risk) is independent of all others
- Gap 5 (sector mapping) depends on nothing but unlocks richer downstream
- Gap 6 (MF risk v2) depends on Gap 2 (purchase_mode) being done
- Gap 7, 8, 9 are independent cleanups
- Atlas-critical order: Gap 2 → Gap 1 → Gap 4 → Gap 3 → Gap 5 → rest

## Acceptance criteria summary

All nine gaps green when:
- All 135 declared indices in `de_index_master` have ≥ 250 days of
  OHLC history and have populated technicals in `de_index_technical_daily`
- `de_mf_master.purchase_mode` is populated for ≥ 13,000 funds
- `de_mf_technical_daily` is populated for ~800 eligible MFs
- A new `de_equity_fundamentals` table exists with weekly refresh
- All v2 technical tables have 1/3/5-year variants for every risk metric
- `de_sector_mapping` table exists and covers every distinct JIP sector
- `de_mf_derived_daily` rows recomputed via empyrical
- `de_rs_daily_summary` has rows
- `de_global_instrument_master` has ≥ 100 ETFs
- Last index backfill shows 0 errored chunks
