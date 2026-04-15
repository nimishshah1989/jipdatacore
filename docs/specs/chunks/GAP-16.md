# GAP-16 — Sector breadth rollups (% above 50/200 DMA per sector per day)

## Goal
Aggregate per-stock `above_50dma` / `above_200dma` booleans from
`de_equity_technical_daily` into a daily-per-sector rollup table, plus
an API endpoint. Unblocks sector rotation dashboards in Atlas.

## Scope

### Alembic migration 013: `de_sector_breadth_daily`

```
date DATE NOT NULL
sector VARCHAR(100) NOT NULL
-- Counts
stocks_total INTEGER NOT NULL
stocks_above_50dma INTEGER NOT NULL
stocks_above_200dma INTEGER NOT NULL
stocks_above_20ema INTEGER NOT NULL
-- Percentages (derived but stored for read speed)
pct_above_50dma NUMERIC(6,2) NOT NULL
pct_above_200dma NUMERIC(6,2) NOT NULL
pct_above_20ema NUMERIC(6,2) NOT NULL
-- Momentum split
stocks_rsi_overbought INTEGER NOT NULL   -- rsi_14 > 70
stocks_rsi_oversold INTEGER NOT NULL     -- rsi_14 < 30
stocks_macd_bullish INTEGER NOT NULL     -- macd > macd_signal
-- Aggregate signals
breadth_regime VARCHAR(20)  -- 'bullish' if pct_above_50 > 70, 'bearish' < 30, else 'neutral'
-- Audit
created_at TIMESTAMPTZ DEFAULT now()
updated_at TIMESTAMPTZ DEFAULT now()
PRIMARY KEY (date, sector)
```

Index: `(sector, date DESC)` for the per-sector time-series query.

### Compute script: `scripts/compute_sector_breadth.py`

Single SQL materialization per business_date (not per-stock loop):

```sql
INSERT INTO de_sector_breadth_daily (date, sector, stocks_total, ...)
SELECT
  t.date,
  COALESCE(m.sector, 'Unclassified') AS sector,
  count(*) AS stocks_total,
  count(*) FILTER (WHERE t.above_50dma) AS stocks_above_50dma,
  count(*) FILTER (WHERE t.above_200dma) AS stocks_above_200dma,
  ...
FROM de_equity_technical_daily t
JOIN de_equity_master m ON m.instrument_id = t.instrument_id
WHERE t.date = :business_date
GROUP BY t.date, m.sector
ON CONFLICT (date, sector) DO UPDATE SET ...
```

Supports a `--backfill --from 2020-01-01` flag for historical populate.

### Pipeline registration
- Add `sector_breadth` to `COMPUTATION_SCRIPTS` in `app/pipelines/registry.py`
  mapping to `scripts.compute_sector_breadth`
- Add it to the `nightly_compute` schedule AFTER the `compute_indicators_v2`
  step (so breadth reads fresh technicals)

### API endpoint
- New `app/api/v1/sectors.py` router
- `GET /api/v1/sectors/breadth?date=<iso>` — latest if not given
- `GET /api/v1/sectors/breadth/history?sector=<name>&window=1y` — time-series

### Tests
- `tests/computation/test_sector_breadth.py` — golden on a 3-stock 2-sector fixture
- `tests/api/test_sectors_api.py` — route returns correct shape

## Acceptance criteria
- [ ] Migration 013 applied to prod
- [ ] `SELECT count(DISTINCT sector) FROM de_sector_breadth_daily` ≥ 20
- [ ] `SELECT count(*) FROM de_sector_breadth_daily` ≥ 50,000 (after backfill)
- [ ] Pipeline wired into nightly_compute
- [ ] `curl https://data.jslwealth.in/api/v1/sectors/breadth` returns all sectors with non-null pct_above_50dma
- [ ] Commit subject starts with `GAP-16`

## Steps for the inner session
1. Confirm `de_equity_technical_daily.above_50dma` and `above_200dma`
   are populated (they're GENERATED STORED columns from the schema)
2. Read `app/pipelines/registry.py` to understand COMPUTATION_SCRIPTS wiring
3. Write migration 013
4. Write compute script with backfill mode
5. Register in pipeline registry, add to nightly_compute schedule
6. Write API router
7. Run backfill `--from 2020-01-01`
8. Tests + commit

## Out of scope
- Breadth for ETF / MF / global universes (equity only)
- Sentiment/news-based breadth (separate concept)
- Sector beta-weighted breadth

## Dependencies
- Upstream: GAP-07 (technical tables populated with post-cutover names — DONE)
- Downstream: Atlas sector rotation dashboard
