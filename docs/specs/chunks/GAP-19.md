# GAP-19 — MF holdings audit + style/sector exposure API

## Goal
Verify `de_mf_holdings` is current and populated for the eligible MF
universe, fix any gaps, and expose holdings + derived style/sector
exposure via API for Atlas fund deepdive pages.

## Mandatory step 0 — audit existing state

Run:
```sql
-- Coverage
SELECT count(DISTINCT mstar_id), min(as_of_date), max(as_of_date)
FROM de_mf_holdings;

-- Staleness per fund (latest holdings vs today)
SELECT
  count(*) FILTER (WHERE max_date >= CURRENT_DATE - INTERVAL '60 days') AS fresh,
  count(*) FILTER (WHERE max_date < CURRENT_DATE - INTERVAL '60 days') AS stale
FROM (SELECT mstar_id, max(as_of_date) max_date FROM de_mf_holdings GROUP BY mstar_id) x;

-- Eligible universe with zero holdings rows
WITH elig AS (
  SELECT mstar_id FROM de_mf_master
  WHERE purchase_mode=1 AND broad_category='Equity' AND is_active
    AND NOT is_etf AND NOT is_index_fund
    AND fund_name !~* '\y(IDCW|Dividend|Segregated|Direct)\y'
)
SELECT count(*) FROM elig e
WHERE NOT EXISTS (SELECT 1 FROM de_mf_holdings h WHERE h.mstar_id = e.mstar_id);
```

Write findings to `reports/mf_holdings_audit_<date>.md`.

## Scope

### Fix A — refresh stale holdings from Morningstar (if audit shows gaps)

The `morningstar_holdings` pipeline exists. Verify it's running monthly
and covering the eligible universe. If not:
1. Force-run it for missing/stale mstar_ids
2. Schedule fix if the cron isn't firing

### Fix B — sector / style exposure materialized view

Create `de_mf_sector_exposure` (or a view):
```
mstar_id VARCHAR PK
as_of_date DATE PK
sector VARCHAR
sector_weight_pct NUMERIC(6,2)
stocks_in_sector INTEGER
```

Aggregate `de_mf_holdings.weight_pct` by joining holding's equity
to `de_equity_master.sector`.

Same for `de_mf_style_exposure` with market-cap buckets (Large/Mid/Small).

### API endpoints
- `GET /api/v1/mf/{mstar_id}/holdings?top=20` — top holdings by weight
- `GET /api/v1/mf/{mstar_id}/sector_exposure` — sector breakdown
- `GET /api/v1/mf/{mstar_id}/style_exposure` — market-cap breakdown

## Acceptance criteria
- [ ] `reports/mf_holdings_audit_<date>.md` exists with coverage numbers
- [ ] Eligible MFs with holdings coverage ≥ 500 (out of 685 covered funds)
- [ ] `de_mf_sector_exposure` / view populated
- [ ] All three API endpoints return valid data for at least 5 test funds
- [ ] Commit subject starts with `GAP-19`

## Steps for the inner session
1. Run the step-0 audit SQL
2. Decide fix path based on findings
3. Implement sector + style exposure views/tables
4. Write API endpoints
5. Test manually + golden fixtures
6. Commit

## Out of scope
- Fixed income holdings analysis (debt funds)
- Overlap analysis across funds (separate analytics layer)
- Historical holdings time-series — single as-of-latest snapshot only

## Dependencies
- Upstream: `de_mf_holdings` exists (morningstar pipeline),
  `de_equity_master.sector` populated (GAP-08 + GAP-09)
- Downstream: Atlas fund deepdive page
