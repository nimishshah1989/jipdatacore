# GAP-20 — BSE-derived sector & MF rollups

## Goal
Turn the raw BSE streams from GAP-18a/b/c into **derived signals at
sector-level and MF-fund-level**, so Atlas can surface:

**Sector dashboards:**
- "Promoters are pledging more in Real Estate this month"
- "Insider net-buy spike in PSU Banks this week"
- "Consumer Goods has 12 stocks on ASM — elevated risk"
- "FII net inflow into IT sector quarter-over-quarter"
- "Bulk-deal flow rotation: Utilities out, Autos in"

**MF fund dashboards:**
- "Fund X holds 3 ASM-flagged stocks (7% of portfolio)"
- "Fund Y has 12% exposure to high-pledge stocks"
- "Fund Z's top 10 holdings saw net insider buying this quarter"
- "Fund manager-proxy signal: insider buys in holdings weighted by
  portfolio %"

## Scope

### Alembic migration 018: five derived tables

```
-- Sector rollups (daily snapshot of latest-available data)
de_sector_ownership_rollup (
  sector VARCHAR(100),
  as_of_date DATE,
  stocks_total INTEGER,
  avg_promoter_pct NUMERIC(6,2),
  avg_pledged_pct NUMERIC(6,2),
  avg_fii_pct NUMERIC(6,2),
  avg_dii_pct NUMERIC(6,2),
  fii_trend_qoq NUMERIC(6,2),       -- delta vs previous quarter
  pledge_trend_qoq NUMERIC(6,2),
  stocks_asm_flagged INTEGER,
  stocks_gsm_flagged INTEGER,
  PRIMARY KEY (sector, as_of_date)
)

de_sector_insider_flow (
  sector VARCHAR(100),
  as_of_date DATE,
  window_days INTEGER,              -- 7 / 30 / 90
  total_buys_cr NUMERIC(18,2),
  total_sells_cr NUMERIC(18,2),
  net_cr NUMERIC(18,2),
  buy_count INTEGER,
  sell_count INTEGER,
  PRIMARY KEY (sector, as_of_date, window_days)
)

de_sector_deal_flow (
  sector VARCHAR(100),
  as_of_date DATE,
  window_days INTEGER,
  bulk_buy_cr NUMERIC(18,2),
  bulk_sell_cr NUMERIC(18,2),
  block_buy_cr NUMERIC(18,2),
  block_sell_cr NUMERIC(18,2),
  net_cr NUMERIC(18,2),
  PRIMARY KEY (sector, as_of_date, window_days)
)

-- MF rollups
de_mf_ownership_risk (
  mstar_id VARCHAR(20),
  as_of_date DATE,
  total_holdings INTEGER,
  holdings_asm_flagged INTEGER,
  holdings_gsm_flagged INTEGER,
  asm_weight_pct NUMERIC(6,2),      -- % of AUM in ASM-flagged
  gsm_weight_pct NUMERIC(6,2),
  avg_pledged_pct_weighted NUMERIC(6,2),   -- weighted by holding
  high_pledge_exposure_pct NUMERIC(6,2),   -- % of AUM in >20% pledged
  PRIMARY KEY (mstar_id, as_of_date)
)

de_mf_insider_signal (
  mstar_id VARCHAR(20),
  as_of_date DATE,
  window_days INTEGER,
  weighted_net_insider_cr NUMERIC(18,4),   -- net insider flow through holdings weighted by holding %
  net_insider_holdings INTEGER,            -- count of holdings with net insider buy
  insider_positive_weight_pct NUMERIC(6,2),-- % of AUM in holdings where insiders are net buying
  PRIMARY KEY (mstar_id, as_of_date, window_days)
)
```

### Compute script: `scripts/compute_bse_rollups.py`

Pure SQL materialization (no per-row Python). Each table is one
`INSERT ... SELECT ... GROUP BY` with the right join/aggregate.
Example for sector_insider_flow:

```sql
INSERT INTO de_sector_insider_flow (sector, as_of_date, window_days, total_buys_cr, total_sells_cr, net_cr, buy_count, sell_count)
SELECT
  COALESCE(f.sector, 'Unclassified') AS sector,
  CURRENT_DATE AS as_of_date,
  :window AS window_days,
  SUM(CASE WHEN i.transaction_type='Buy' THEN i.value_cr ELSE 0 END),
  SUM(CASE WHEN i.transaction_type='Sell' THEN i.value_cr ELSE 0 END),
  SUM(CASE WHEN i.transaction_type='Buy' THEN i.value_cr ELSE -i.value_cr END),
  count(*) FILTER (WHERE i.transaction_type='Buy'),
  count(*) FILTER (WHERE i.transaction_type='Sell')
FROM de_bse_insider_trades i
JOIN de_equity_fundamentals f ON f.instrument_id = i.instrument_id
WHERE i.transaction_date >= CURRENT_DATE - :window
GROUP BY COALESCE(f.sector, 'Unclassified')
ON CONFLICT (sector, as_of_date, window_days) DO UPDATE SET ...
```

Run with `--windows 7,30,90` so Atlas has multiple lookback choices.

### API endpoints (thin read layer)
- `GET /api/v1/sectors/{sector}/ownership` — ownership rollup
- `GET /api/v1/sectors/{sector}/flow?window=30` — insider + deals net
- `GET /api/v1/mf/{mstar_id}/risk_exposure` — ownership-risk metrics
- `GET /api/v1/mf/{mstar_id}/insider_signal?window=30` — weighted signal

### Pipeline registration
- `COMPUTATION_SCRIPTS["bse_rollups"] = "scripts.compute_bse_rollups"`
- Add to `nightly_compute` AFTER `compute_indicators_v2`

## Acceptance criteria
- [ ] Migration 018 applied (5 tables)
- [ ] First run after GAP-18a/b/c completes in < 2 minutes
- [ ] `de_sector_ownership_rollup` has ≥ 25 sector rows
- [ ] `de_sector_insider_flow` has ≥ 75 rows (25 sectors × 3 windows)
- [ ] `de_mf_ownership_risk` has ≥ 500 funds
- [ ] API endpoints return valid JSON for at least 5 test sectors + 5 test funds
- [ ] Registered in nightly_compute
- [ ] Commit subject starts with `GAP-20`

## Steps for the inner session
1. Verify GAP-18a/b/c tables are populated (skip if run before they land)
2. Write migration 018
3. Write `compute_bse_rollups.py` — 5 SQL blocks, one per table, + CLI
4. Write the 4 API endpoints in `app/api/v1/sector_bse.py` and `mf_bse.py`
5. Register in pipeline registry + nightly_compute
6. Smoke-test endpoints
7. Commit

## Out of scope
- Intraday rollups (daily only)
- Cross-sector correlation analysis
- Alert generation (separate concern — can layer on the de_cron_run events)

## Dependencies
- Upstream: GAP-18a, GAP-18b, GAP-18c — all must be DONE first
- Downstream: Atlas sector + fund dashboard views
