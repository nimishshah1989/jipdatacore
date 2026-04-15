# GAP-18b — BSE ownership & insider activity (shareholding + pledge + insider + SAST)

## Goal
Ingest the four high-value ownership datasets from BSE that aren't in
the BseIndiaApi library but are scrape-safe from the same host:
quarterly shareholding patterns (with time-series), pledge tracking,
SEBI Form-C/D insider trades, and SAST takeover disclosures.

These feed fund-manager-grade signals: "is the promoter pledging more?",
"are insiders dumping?", "who just crossed 5%?", "FII entry/exit trend
per stock quarter-over-quarter."

## Source

Same `api.bseindia.com/BseIndiaAPI/api` host as GAP-18a, but these
endpoints aren't in the library. Must send `User-Agent` + `Referer:
https://www.bseindia.com/` headers. No cookie dance needed (unlike NSE).

Endpoints:
- Shareholding pattern: `/CorpShareHoldingPattern_New/w?scripcode={code}`
- Pledge tracking: `/Shrholdpledge/w?scripcode={code}`
- Insider trades: `/Cinsidertrading/w?scripcode={code}`
- SAST disclosures: `/CorpSASTData/w?scripcode={code}`

Rate limit: treat as 5 rps ceiling (more conservative than library's 8
for the undocumented endpoints). Community-verified safe.

## Alembic migration 016: four tables

```
-- de_bse_shareholding (quarterly snapshots, time-series)
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
scripcode VARCHAR(20)
quarter_end DATE NOT NULL
promoter_pct NUMERIC(6,2)
promoter_pledged_pct NUMERIC(6,2)
public_pct NUMERIC(6,2)
fii_pct NUMERIC(6,2)
dii_pct NUMERIC(6,2)
insurance_pct NUMERIC(6,2)
mutual_funds_pct NUMERIC(6,2)
retail_pct NUMERIC(6,2)
body_corporate_pct NUMERIC(6,2)
total_shareholders INTEGER
raw_json JSONB  -- full payload for audit
created_at TIMESTAMPTZ DEFAULT now()
UNIQUE (instrument_id, quarter_end)

-- de_bse_pledge_history (more granular than shareholding, when available)
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
as_of_date DATE NOT NULL
promoter_holding_qty BIGINT
promoter_pledged_qty BIGINT
pledged_pct NUMERIC(6,2)
total_shares BIGINT
created_at TIMESTAMPTZ DEFAULT now()
UNIQUE (instrument_id, as_of_date)

-- de_bse_insider_trades (SEBI PIT Form C/D)
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
filer_name VARCHAR(200)
filer_category VARCHAR(50)   -- 'Promoter', 'KMP', 'Director', etc.
transaction_type VARCHAR(20) -- 'Buy', 'Sell', 'Pledge', 'Revoke'
qty BIGINT
value_cr NUMERIC(18,4)
transaction_date DATE
acquisition_mode VARCHAR(50)  -- 'Market', 'Off-market', 'ESOP'
intimation_date DATE
dedup_hash VARCHAR(64) NOT NULL UNIQUE
created_at TIMESTAMPTZ DEFAULT now()
INDEX (instrument_id, transaction_date DESC)

-- de_bse_sast_disclosures
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
acquirer_name VARCHAR(300)
acquirer_type VARCHAR(50)
pre_holding_pct NUMERIC(6,2)
post_holding_pct NUMERIC(6,2)
delta_pct NUMERIC(6,2)
transaction_date DATE
disclosure_date DATE
regulation VARCHAR(50)  -- 'Reg 7(1)', 'Reg 10(7)', etc.
dedup_hash VARCHAR(64) NOT NULL UNIQUE
created_at TIMESTAMPTZ DEFAULT now()
INDEX (instrument_id, disclosure_date DESC)
```

## Pipeline: `app/pipelines/bse/ownership.py`

**Per-scripcode design** (no bulk endpoint available):
1. Load scripcode list from `de_instrument WHERE is_active` (~2,200).
2. For each scripcode, call 4 endpoints. **Parallelize with
   asyncio.gather with semaphore=5** (matches the 5 rps ceiling).
3. Parse HTML/JSON → dedup hash → bulk UPSERT.
4. Total expected: 2,200 × 4 = 8,800 requests. At 5 rps ~30 min per run.
5. Run WEEKLY (Sunday) — shareholding is quarterly, insider trades
   dribble in daily but weekly batching is acceptable for Atlas UX.

### Code style
- Use `httpx.AsyncClient` with connection pool of 10
- Retry logic: 3 tries with exponential backoff on 5xx/timeout
- On 404 per scripcode (no data for that scrip): log+skip, don't fail run
- Save `raw_json` for shareholding so future schema additions don't require re-scrape

### Pipeline registration
- `PIPELINE_REGISTRY`: `"bse_ownership"`
- `SCHEDULE_REGISTRY["bse_ownership_weekly"] = ["bse_ownership"]`
- Cron line: Sunday IST 06:00 UTC 00:30 Monday —
  `30 0 * * 1 $WRAPPER bse_ownership_weekly`

## Tests
- Saved fixture HTML for 1 scripcode across all 4 endpoints
- Parser unit tests against fixtures
- Pipeline integration test with mocked httpx
- Dedup test

## Acceptance criteria
- [ ] Migration 016 applied (4 new tables)
- [ ] `de_bse_shareholding` has ≥ 1,500 distinct instruments after first run
- [ ] `de_bse_pledge_history` has ≥ 500 rows
- [ ] `de_bse_insider_trades` has ≥ 300 rows (depends on filing activity)
- [ ] `de_bse_sast_disclosures` has ≥ 100 rows
- [ ] Pipeline run completes in < 45 minutes (5 rps parallel)
- [ ] Weekly cron wired
- [ ] Commit subject starts with `GAP-18b`

## Steps for the inner session
1. Curl each endpoint manually with proper headers — save 1 fixture per endpoint
2. Inspect response shape (HTML table vs JSON)
3. Write parsers in a new `app/pipelines/bse/parsers.py`
4. Write migration 016
5. Write `app/pipelines/bse/ownership.py`
6. Run for a small batch (10 scripcodes) first, verify
7. Full run (2,200 scripcodes)
8. Cron wiring + commit

## Out of scope
- PDF downloads of filings (URL stored, parsing deferred)
- Historical shareholding before what BSE publishes (~8 quarters visible)
- Cross-validation against NSE (separate chunk if needed)

## Dependencies
- Upstream: GAP-18a (uses same `de_instrument.bse_scripcode` map)
- Downstream: GAP-20 (sector/MF rollups), GAP-21 (deepdive API extension)
