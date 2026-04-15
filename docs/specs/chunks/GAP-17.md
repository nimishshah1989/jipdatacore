# GAP-17 — Historical fundamentals time-series from screener.in

## Goal
Extend the GAP-08 screener.in scraper to also extract the 10-year
annual/quarterly tables (Sales, OP, PAT, EPS, book value, etc.) that
appear lower on every company page. Enables "compare P/E vs 5-year
history" and YoY growth charts on Atlas.

## Scope

### Alembic migration 014: `de_equity_fundamentals_history`

```
instrument_id UUID NOT NULL
fiscal_period_end DATE NOT NULL       -- last day of quarter or FY
period_type VARCHAR(10) NOT NULL      -- 'quarterly' | 'annual' | 'ttm'
-- P&L
revenue_cr NUMERIC(18,2)
expenses_cr NUMERIC(18,2)
operating_profit_cr NUMERIC(18,2)
opm_pct NUMERIC(8,4)
other_income_cr NUMERIC(18,2)
interest_cr NUMERIC(18,2)
depreciation_cr NUMERIC(18,2)
profit_before_tax_cr NUMERIC(18,2)
tax_pct NUMERIC(8,4)
net_profit_cr NUMERIC(18,2)
eps NUMERIC(18,4)
-- Balance sheet
equity_capital_cr NUMERIC(18,2)
reserves_cr NUMERIC(18,2)
borrowings_cr NUMERIC(18,2)
other_liabilities_cr NUMERIC(18,2)
fixed_assets_cr NUMERIC(18,2)
cwip_cr NUMERIC(18,2)
investments_cr NUMERIC(18,2)
other_assets_cr NUMERIC(18,2)
total_assets_cr NUMERIC(18,2)
-- Cash flow
cfo_cr NUMERIC(18,2)  -- operating cash flow
cfi_cr NUMERIC(18,2)  -- investing
cff_cr NUMERIC(18,2)  -- financing
-- Audit
source VARCHAR(50) DEFAULT 'screener'
created_at TIMESTAMPTZ DEFAULT now()
updated_at TIMESTAMPTZ DEFAULT now()
PRIMARY KEY (instrument_id, fiscal_period_end, period_type)
```

Index: `(instrument_id, period_type, fiscal_period_end DESC)`.

### Parser extension

Edit `app/pipelines/fundamentals/screener_fetcher.py` to also parse:
- Quarters table (id=`quarters`)
- Profit & Loss table (id=`profit-loss`)
- Balance Sheet table (id=`balance-sheet`)
- Cash Flow table (id=`cash-flow`)
- Ratios table (id=`ratios`)

Each is a standard HTML `<table>` with year/quarter columns. Use
BeautifulSoup to extract to a dict-of-dicts. Column headers are the
fiscal period ends.

### Pipeline extension

Add a new method `FundamentalsPipeline._ingest_history(instrument_id, html)`
that calls the new parsers and UPSERTs to `de_equity_fundamentals_history`.

### Scope note

Screener.in shows:
- 10 years annual data
- 12 quarters of quarterly data

This is the ceiling we can get for free. Pre-listing and older data is not
available. That's fine for Atlas.

### Rate limit

No additional requests — same HTML page already being fetched for GAP-08's
snapshot path. We're just parsing more tables out of it. Zero marginal cost.

## Acceptance criteria
- [ ] Migration 014 applied to prod
- [ ] `SELECT count(DISTINCT instrument_id) FROM de_equity_fundamentals_history WHERE period_type='annual'` ≥ 2,000
- [ ] `SELECT count(*) FROM de_equity_fundamentals_history WHERE period_type='annual'` ≥ 15,000 (≥ 7 years × 2k stocks on average)
- [ ] `SELECT count(DISTINCT instrument_id) FROM de_equity_fundamentals_history WHERE period_type='quarterly'` ≥ 1,500
- [ ] Golden parity test for RELIANCE — at least net_profit_cr for each of the last 5 FYs non-null
- [ ] Commit subject starts with `GAP-17`

## Steps for the inner session
1. Fetch one company HTML locally and inspect the four target tables
2. Write parsers with unit tests against the saved HTML fixture
3. Alembic migration 014
4. Extend FundamentalsPipeline to call the new parsers
5. Run backfill for the same 2,272 stocks covered by GAP-08
6. Verify + commit

## Out of scope
- Historical ratios (P/E, P/B) over time — compute on-the-fly from
  EPS + close_adj + shares_outstanding when Atlas needs them
- XBRL-sourced data (NSE/BSE filings) — clean but much more work
- Pre-listing or pre-screener-coverage history

## Dependencies
- Upstream: GAP-08 (screener fetcher exists — DONE)
- Downstream: GAP-15 deepdive endpoint can consume history section
