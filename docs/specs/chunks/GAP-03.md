# GAP-03 — Historical backfill for 53 missing indices (existing sources first)

## Goal
Populate historical OHLCV for the 53 indices in `de_index_master` that have
only ~5 days of data in `de_index_prices`. Ingest from the CHEAPEST and FASTEST
source available — in order of preference:

1. **Existing JIP RDS sister projects** — check `fie2.index_prices` and
   `mfpulse_reimagined` DBs on the same EC2 host BEFORE scraping anything
2. **NSE bhav copy / niftyindices.com** — if (1) doesn't cover the gaps
3. **NEVER yfinance** — explicitly excluded; NSE is canonical

## Mandatory step 0: inventory existing sources

Before writing any scraper, the chunk MUST run these queries and report:

```bash
# fie2 (schema: public.index_prices, columns: date VARCHAR, index_name, open/high/low/close_price, volume)
ssh ubuntu@13.206.34.214 "set -a; source /home/ubuntu/fie2/.env; set +a; \
  psql \$DATABASE_URL -c \"SELECT index_name, COUNT(*), MIN(date), MAX(date) \
  FROM index_prices WHERE index_name ILIKE '%PHARMA%' OR index_name ILIKE '%REALTY%' \
  OR index_name ILIKE '%OIL%' OR index_name ILIKE '%PVT BANK%' OR index_name ILIKE '%HEALTH%' \
  OR index_name ILIKE '%CONSR%' OR index_name ILIKE '%FIN%' GROUP BY index_name\""
```

```bash
# mfpulse (DB on docker bridge, query from inside the running container)
docker exec mf-pulse python -c "
import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
c.cursor().execute('SELECT table_name FROM information_schema.tables WHERE table_schema=\\'public\\'')
# list tables + search for any index/price ones
"
```

If either source has the data, ingest from it via direct psql COPY (or
cross-DB COPY via FDW). This is 10x faster than scraping.

## Scope
- Identify the 53 indices needing backfill
- Run the step-0 inventory — log what's found
- Write `scripts/backfill_indices_historical.py` that:
  - Takes a list of `(index_code, nse_index_name)` pairs
  - First queries `fie2.index_prices` via the fie2 DATABASE_URL for each
    missing index
  - For any indices NOT in fie2, falls back to niftyindices.com historical
    CSV endpoint (POST `Backpage.aspx/getHistoricaldatatabletoString`)
  - If niftyindices also doesn't have it, falls back to NSE daily bhav copy
    archive URL: `https://archives.nseindia.com/content/indices/ind_close_all_<DDMMYYYY>.csv`
  - UPSERTs into `de_index_prices` via `ON CONFLICT (date, index_code) DO NOTHING`
  - Map fie2's `index_name` strings to JIP's `index_code` values using
    `de_index_master.index_name` for name→code resolution
- Run backfill from 2016-01-01 onwards
- Every index in de_index_master must end with ≥ 250 days of data

## Acceptance criteria
- [ ] Step 0 inventory report committed to `reports/index_source_inventory_<date>.md`
- [ ] Script exists at `scripts/backfill_indices_historical.py`
- [ ] `SELECT COUNT(*) FROM de_index_master WHERE index_code NOT IN (SELECT index_code FROM de_index_prices GROUP BY index_code HAVING COUNT(*) >= 250)` returns 0
- [ ] 7 critical sectoral indices have ≥ 2,000 days: NIFTY PHARMA, NIFTY REALTY, NIFTY PVT BANK, NIFTY OIL AND GAS, NIFTY HEALTHCARE, NIFTY CONSUMER DURABLES, NIFTY FINANCIAL SERVICES
- [ ] Commit subject starts with `GAP-03`
- [ ] `state.db` shows `GAP-03` with `status='DONE'`

## Out of scope
- yfinance (explicitly excluded)
- Adding historical backfill to the daily cron
- Recomputing index technicals (GAP-04)

## Dependencies
- Upstream: none
- Downstream: GAP-04
