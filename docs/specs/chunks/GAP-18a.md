# GAP-18a — BSE corporate filings (announcements + actions + result calendar)

## Goal
Ingest the three library-covered BSE filing streams daily: announcements,
corporate actions (dividends/splits/bonus/buyback), and result calendar.
Powers Atlas "recent filings" and "upcoming events" per stock.

## Scope

### Library
`pip install bse` (github.com/BennyThadikaran/BseIndiaApi). Rate-limited
8 rps by default, no auth, scripcode-keyed.

### Alembic migration 015: three tables

```
-- de_bse_announcements
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK -> de_instrument (NOT NULL, INDEX)
scripcode VARCHAR(20) NOT NULL
announcement_dt TIMESTAMPTZ NOT NULL
headline TEXT NOT NULL
category VARCHAR(100)
subcategory VARCHAR(100)
description TEXT
attachment_url TEXT
dedup_hash VARCHAR(64) NOT NULL UNIQUE  -- sha256(scripcode|dt|headline)
created_at TIMESTAMPTZ DEFAULT now()

INDEX (instrument_id, announcement_dt DESC)
INDEX (announcement_dt DESC)  -- timeline view

-- de_bse_corp_actions
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
scripcode VARCHAR(20) NOT NULL
action_type VARCHAR(30) NOT NULL  -- dividend/split/bonus/buyback/rights/demerger
ex_date DATE
record_date DATE
announced_at TIMESTAMPTZ
purpose_code VARCHAR(10)  -- raw BSE PURPOSE (P5/P6/...)
ratio TEXT                -- e.g. '1:10' or '5 Re per share'
amount_per_share NUMERIC(18,4)
description TEXT
dedup_hash VARCHAR(64) NOT NULL UNIQUE  -- sha256(scripcode|ex_date|action_type)
created_at TIMESTAMPTZ DEFAULT now()

INDEX (instrument_id, ex_date DESC)

-- de_bse_result_calendar
id BIGSERIAL PRIMARY KEY
instrument_id UUID FK (NOT NULL, INDEX)
scripcode VARCHAR(20) NOT NULL
result_date DATE NOT NULL
period VARCHAR(20)  -- 'Q1 FY26', 'Annual FY25', etc.
announced_at TIMESTAMPTZ
dedup_hash VARCHAR(64) NOT NULL UNIQUE  -- sha256(scripcode|result_date)
created_at TIMESTAMPTZ DEFAULT now()

INDEX (instrument_id, result_date)
```

### Pipeline: `app/pipelines/bse/filings.py` — `BseFilingsPipeline(BasePipeline)`

Fast-path design:
1. **Scripcode resolution** — load instrument_id → bse_scripcode map from
   `de_instrument` (single query, cached for the pipeline run). If many
   instruments lack bse_scripcode, inline-backfill from `listSecurities()`
   on first run.
2. **Bulk fetch announcements** — `BSE.announcements(from_date=T-7,
   to_date=T, segment='Equity', page_no=0)` then paginate. DO NOT fetch
   per-scripcode; fetch by date range and filter locally. This cuts 2,200
   API calls to ~20 paginated ones.
3. **Bulk fetch corp actions** — `BSE.actions(segment='Equity',
   from_date=T-30, to_date=T+60)` once. Wide window so ex-dates don't fall
   through the cracks.
4. **Bulk fetch result calendar** — `BSE.resultCalendar(from_date=T-7,
   to_date=T+30)` once.
5. **Parse + dedup hash + bulk UPSERT** via `COPY ... ON CONFLICT DO NOTHING`.
6. **Log metrics**: rows fetched, rows inserted, rows skipped (dedup), unmapped scripcodes.

### Pipeline registration
- `PIPELINE_REGISTRY`: `"bse_filings": ("app.pipelines.bse.filings", "BseFilingsPipeline")`
- `SCHEDULE_REGISTRY["bse_filings_daily"] = ["bse_filings"]`
- Cron line: after nightly_compute at IST 01:30 (UTC 20:00) —
  `0 20 * * * $WRAPPER bse_filings_daily`

### Tests
- Saved BSE JSON fixture (10 announcements, 5 actions, 5 calendar entries)
- Dedup test: re-ingest same fixture, zero new rows
- Scripcode mapping fallback test

## Acceptance criteria
- [ ] Migration 015 applied to prod (3 new tables)
- [ ] First run inserts ≥ 1,000 announcement rows covering ≥ 200 distinct instruments
- [ ] `de_bse_corp_actions` has ≥ 100 rows with future ex_date
- [ ] `de_bse_result_calendar` has ≥ 50 rows
- [ ] Pipeline registered + cron wired in `scripts/cron/jip_scheduler.cron`
- [ ] Per-stock fetch count < 3 (bulk-by-date, not per-scripcode loop)
- [ ] Commit subject starts with `GAP-18a`

## Steps for the inner session
1. `pip install bse` inside docker container; smoke-test `BSE().announcements()`
2. Inspect response shape for all 3 endpoints — save a fixture
3. Check `de_instrument` for existing `bse_scripcode` column. If missing,
   add it (migration) and backfill via `BSE().listSecurities()` in the
   same chunk.
4. Write migration 015 for the 3 tables
5. Write `app/pipelines/bse/filings.py` following BasePipeline template
6. Register in registry.py, add cron line
7. Run first ingestion; verify counts
8. Commit

## Out of scope
- Shareholding / insider trades / SAST (GAP-18b)
- ASM/GSM / bulk deals / circuit flags (GAP-18c)
- PDF attachment download + parse
- NSE cross-validation (separate later chunk)

## Dependencies
- Upstream: `de_instrument` (exists). `bse_scripcode` column may need inline backfill.
- Downstream: GAP-21 (extend deepdive endpoint to include announcements section)
