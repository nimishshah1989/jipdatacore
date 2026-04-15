# GAP-18c — BSE trading flags & institutional flows (ASM/GSM + bulk/block deals + circuit filters)

## Goal
Ingest the daily BSE risk-flag and flow-signal datasets: ASM/GSM
surveillance lists, price band / circuit filters, bulk deals, block
deals. Powers Atlas "is this stock under surveillance?" badges and
"institutional buy/sell flow today" dashboards.

## Source

Same `api.bseindia.com/BseIndiaAPI/api` host, same UA+Referer headers.
These endpoints return date-range data — one call covers the full
universe, no per-scripcode loop needed.

Endpoints:
- ASM list: `/ASM/w`
- GSM list: `/GSM/w`
- Price band / circuit filter: `/CircuitFilter/w`
- Bulk deals: `/BulkDeals/w?Fdate={d}&TDate={d}`
- Block deals: `/BlockDeals/w?Fdate={d}&TDate={d}`

## Alembic migration 017: four tables

```
-- de_bse_trading_flags (daily snapshot)
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
scripcode VARCHAR(20)
as_of_date DATE NOT NULL
asm_flag BOOLEAN DEFAULT FALSE
asm_stage VARCHAR(10)          -- 'LT', 'ST', 'Stage 1', 'Stage 2' etc.
gsm_flag BOOLEAN DEFAULT FALSE
gsm_stage VARCHAR(10)
price_band_pct NUMERIC(6,2)    -- 2/5/10/20
upper_circuit_hit BOOLEAN DEFAULT FALSE
lower_circuit_hit BOOLEAN DEFAULT FALSE
trade_to_trade BOOLEAN DEFAULT FALSE
created_at TIMESTAMPTZ DEFAULT now()
UNIQUE (instrument_id, as_of_date)

-- de_bse_bulk_deals
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
scripcode VARCHAR(20)
trade_date DATE NOT NULL
client_name VARCHAR(300)
buy_sell VARCHAR(10)            -- 'Buy' / 'Sell'
qty BIGINT
avg_price NUMERIC(18,4)
value_cr NUMERIC(18,4)          -- qty * avg_price / 1e7
dedup_hash VARCHAR(64) NOT NULL UNIQUE
created_at TIMESTAMPTZ DEFAULT now()
INDEX (trade_date DESC)
INDEX (instrument_id, trade_date DESC)
INDEX (client_name)              -- so we can aggregate by investor

-- de_bse_block_deals — same shape as bulk deals, different source
(identical schema, different table)
```

## Pipeline: `app/pipelines/bse/flags_deals.py`

1. **Bulk-fetch ASM+GSM+CircuitFilter lists** — 3 requests, <2 seconds
2. **Bulk-fetch bulk deals + block deals** for today only — 2 requests
3. **Join with de_instrument on scripcode** — drop unmapped with count log
4. **UPSERT** — ON CONFLICT (instrument_id, as_of_date) DO UPDATE for flags
   (re-run-safe); ON CONFLICT DO NOTHING for deals (immutable once disclosed)
5. **Diff detection** — on ASM/GSM, compare today's flag set with yesterday's
   to emit "NEW ON ASM" / "REMOVED FROM GSM" events to de_cron_run notes

### Pipeline registration
- `PIPELINE_REGISTRY`: `"bse_flags_deals"`
- `SCHEDULE_REGISTRY["bse_flags_deals_daily"] = ["bse_flags_deals"]`
- Cron: after EOD + AMFI late, IST 23:30 (UTC 18:00) so bulk deals
  for today are fully filed —
  `0 18 * * 1-5 $WRAPPER bse_flags_deals_daily`

## Tests
- Saved fixtures for all 5 endpoints
- Parser unit tests
- Diff-detection test: yesterday flagged → today cleared → event logged
- Dedup test on deals

## Acceptance criteria
- [ ] Migration 017 applied (3 new tables — bulk/block share schema)
- [ ] First run: `de_bse_trading_flags` has ≥ 20 ASM-flagged stocks
  (typical baseline)
- [ ] First run with a week's lookback: `de_bse_bulk_deals` has ≥ 50 rows,
  `de_bse_block_deals` has ≥ 20 rows
- [ ] Daily cron wired for weekdays only (no trading on weekends)
- [ ] Run completes in < 30 seconds (it's just 5 API calls + insert)
- [ ] Commit subject starts with `GAP-18c`

## Steps for the inner session
1. Curl each of the 5 endpoints; save fixtures
2. Migration 017
3. Write `flags_deals.py` pipeline
4. Backfill bulk/block deals for last 30 days (one-off `--backfill --days 30`)
5. Daily run smoke test
6. Cron wiring + commit

## Out of scope
- Options/F&O bulk deals (separate FNO universe)
- Historical ASM/GSM pre-screener (no archive available)
- Delivery % (covered via NSE bhavcopy pipeline already)

## Dependencies
- Upstream: `de_instrument.bse_scripcode` from GAP-18a
- Downstream: GAP-20 sector/MF rollups, GAP-21 deepdive extension
