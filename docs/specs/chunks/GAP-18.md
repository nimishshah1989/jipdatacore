# GAP-18 — BSE corporate announcements ingestion via BseIndiaApi

## Goal
Daily pull of raw corporate announcements from BSE (earnings, board
meetings, dividends, rating changes, insider trades, etc.) and store
linked to `de_instrument` so Atlas can show "last 30 announcements"
per stock on the deepdive page.

## Source
`pip install bse` (github.com/BennyThadikaran/BseIndiaApi). Active,
built-in rate limiting, no auth. API methods: `BSE.announcements()`,
`BSE.actions()`, `BSE.resultCalendar()`.

## Scope

### Alembic migration 015: `de_corporate_announcements`

```
id BIGSERIAL PRIMARY KEY
instrument_id UUID NOT NULL (FK → de_instrument, index)
bse_scrip_code VARCHAR(20) NOT NULL
announcement_date TIMESTAMPTZ NOT NULL
category VARCHAR(100)                  -- 'Board Meeting', 'Dividend', 'Result', 'Insider', ...
subject TEXT
description TEXT
pdf_url TEXT
source VARCHAR(20) DEFAULT 'bse'
dedup_hash VARCHAR(64) NOT NULL UNIQUE -- sha256(scrip + date + subject) for idempotency
created_at TIMESTAMPTZ DEFAULT now()
```

Indexes:
- `(instrument_id, announcement_date DESC)` — Atlas lookup
- `(announcement_date DESC)` — timeline view

### Pipeline: `app/pipelines/announcements/bse_announcements.py`

Standard `BasePipeline` subclass:
1. `extract()` — call `BSE.announcements(from_date, to_date)` for the
   trailing 7 days (idempotent — dedup_hash handles re-inserts)
2. `transform()` — map scrip_code → instrument_id via
   `de_instrument.bse_scrip_code`; drop rows for unmapped scrips (log count)
3. `load()` — bulk insert with ON CONFLICT DO NOTHING on dedup_hash

### Cron wiring
- Add to `SCHEDULE_REGISTRY` as `announcements_daily`
- Add cron line to `jip_scheduler.cron` at IST 19:00 (after trading, before nightly_compute)

### API endpoint (optional but recommended)
- `GET /api/v1/instrument/{symbol}/announcements?limit=20` — consumed by deepdive

### Tests
- Fixture with saved BSE response JSON
- Dedup test: re-ingest same data, count unchanged

## Acceptance criteria
- [ ] Migration 015 applied to prod
- [ ] First ingestion produces ≥ 500 rows covering ≥ 100 distinct instruments
- [ ] Pipeline registered + cron wired
- [ ] Commit subject starts with `GAP-18`

## Steps for the inner session
1. `pip install bse` and test manually inside docker container
2. Inspect the response format (keys, date parsing)
3. Write migration 015
4. Write pipeline class following `app/pipelines/framework.BasePipeline` template
5. Map bse_scrip_code → instrument_id — may need to backfill
   `de_instrument.bse_scrip_code` first if it's empty (check before coding)
6. Test ingest for last 7 days
7. Cron wiring, commit

## Out of scope
- NSE announcements (BSE covers most dual-listed; NSE has its own
  `nsepython` path — separate follow-up)
- Text sentiment extraction (goldilocks-style; can add later)
- PDF download + parse (pdf_url stored; parsing is a separate pipeline)

## Dependencies
- Upstream: `de_instrument` must have bse_scrip_code populated. If not,
  inline sub-step to backfill from BSE master (scripts.ingest.bse_master)
- Downstream: GAP-15 deepdive endpoint consumes announcements section
