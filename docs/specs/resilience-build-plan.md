# Data Pipeline Resilience — Forge Build Plan

**Date:** 2026-04-10
**Goal:** Zero-intervention data pipeline — all streams green every day, self-healing, self-learning
**Sessions:** 2-3 focused sessions

---

## Gap Analysis (from /review)

| # | Gap | Severity | Impact |
|---|-----|----------|--------|
| 1 | `__validate_ohlcv__` + `__goldilocks_compute__` not implemented | CRITICAL | Nightly schedule fails on first step |
| 2 | `breadth_regime` / `fund_metrics` no --start-date | HIGH | Full rebuilds timeout (~50 min) |
| 3 | Observatory table name mismatches (6 wrong names) | HIGH | Dashboard lies — shows red when data is green |
| 4 | 15+ pipelines not covered by any managed agent | HIGH | Delivery, VIX, RSS, ETF, global never auto-run |
| 5 | ETF/global computation scripts no incremental args | MEDIUM | Wasteful full rebuilds |
| 6 | Nightly compute shell script bypasses trigger API | MEDIUM | Two execution paths diverge |
| 7 | No self-healing agent loop | HIGH | Failures require manual intervention |
| 8 | No daily forge-compile / learning loop | LOW | Wiki doesn't capture new patterns |

---

## Build Plan: 6 Chunks

### Chunk 1: Fix All Broken Pipelines (P0)
**Goal:** Every pipeline callable via trigger API without errors

**Tasks:**
- Implement `__validate_ohlcv__` handler in registry — SQL UPDATE data_status raw→validated for today
- Implement `__goldilocks_compute__` handler — call computation runner + goldilocks scraper
- Add --start-date to `breadth_regime.py` (incremental from date, not full rebuild)
- Add --start-date to `fund_metrics.py` (incremental)
- Add --start-date to `etf_technicals.py`, `etf_rs.py`, `global_technicals.py`, `global_rs.py`
- Fix `fred_macro` API error (debug and fix)
- Fix `yfinance_global` returning 0 rows (date range logic — period="5d" vs explicit start/end)
- Test every computation script via trigger API: `POST /trigger/single/{name}?business_date=today`

**Acceptance:** All 10 computation scripts + all 23 pipelines return success via trigger API

### Chunk 2: Fix Dashboard Data Binding (P0)
**Goal:** Dashboard shows accurate real-time status for ALL streams

**Tasks:**
- Fix 6 wrong table names in observatory.html tree:
  - `de_rs_score_daily` → `de_rs_scores`
  - `de_market_breadth_daily` → `de_breadth_daily`
  - `de_market_regime_daily` → `de_market_regime`
  - `de_mf_nav` → `de_mf_nav_daily_y2026` (or pulse stream_id)
  - `de_mf_metrics` → `de_mf_derived_daily`
  - `de_index_ohlcv` → `de_index_prices`
- Wire tree node colors to pulse API `status` field directly (not recalculating)
- Add new Goldilocks tree nodes (oscillators, pivots, intermarket, fibonacci, divergences)
- Add row counts from coverage API to tree node tooltips
- Test: every tree node shows correct color matching pulse API status

**Acceptance:** Dashboard shows correct green/yellow/red for ALL 12+ streams

### Chunk 3: Unified Nightly Pipeline (P0)
**Goal:** Single execution path — nightly_compute goes through trigger API

**Tasks:**
- Replace `nightly_compute.sh` with trigger API calls:
  1. `POST /trigger/single/__validate_ohlcv__` (validate today's OHLCV)
  2. `POST /trigger/single/equity_technicals_sql?business_date=today`
  3. `POST /trigger/single/relative_strength?business_date=today`
  4. `POST /trigger/single/market_breadth?business_date=today`
  5. ... (all 11 computation steps from runner.py)
  6. `POST /trigger/single/__goldilocks_compute__`
- OR: Create a new schedule group `nightly_compute` that the trigger API orchestrates
- The `nightly_compute` schedule is already in the registry — just needs the trigger API to handle the special names
- Remove the shell script entirely — everything goes through API

**Acceptance:** `POST /trigger/nightly_compute` runs the full 11-step pipeline in dependency order

### Chunk 4: Managed Agent Workforce (P1)
**Goal:** Complete agent coverage — every pipeline auto-runs on schedule

**Current:** 3 agents (plan limit: 3)
- jip-eod-ingestion (18:33 IST weekdays)
- jip-computations (19:03 IST weekdays)
- jip-fund-metrics (21:03 IST weekdays)

**Option A — Consolidate into 3 smart agents:**
- **Agent 1: EOD Ingestion** (18:33 IST) — trigger/eod (BHAV, indices, AMFI, yfinance, FRED, FII/DII, ETF, delivery, VIX)
- **Agent 2: Nightly Compute** (19:33 IST) — trigger/nightly_compute (validate → 11 computations → goldilocks)
- **Agent 3: Health + Self-Heal** (23:33 IST) — check pulse, fix any stale streams, report status

Update existing agent prompts to cover more pipelines.

**Option B — Plan upgrade for more agents:**
- Add: weekly Morningstar, monthly holdings, RSS qualitative, health check
- Need 7-8 agents total

**Tasks:**
- Update Agent 1 prompt to include ALL ingestion pipelines (not just EOD group)
- Update Agent 2 to call /trigger/nightly_compute (single API call for all computations)
- Redesign Agent 3 as a self-healing health check agent:
  - Check observatory/pulse for ALL streams
  - For each stale stream: identify which pipeline to run
  - Auto-trigger the pipeline via API
  - Verify it fixed the staleness
  - Log results
  - If still failing after 2 retries: flag as needs-human-investigation
- Add weekend agent logic: skip equity ingestion on weekends, still run global/macro

**Acceptance:** Full week runs Mon-Fri with zero intervention. Health agent catches and fixes any failures.

### Chunk 5: Docker Image Rebuild + CI/CD (P1)
**Goal:** Single deployment path — no more docker cp hotfixes

**Tasks:**
- Rebuild Docker image with all current code (scripts/ + app/ + computation/)
- Update docker-compose to mount scripts/ as well: `- ./scripts:/app/scripts`
- OR: just rebuild image and stop using volume mount (simpler)
- Create a deploy script: `git pull && docker compose build && docker compose up -d`
- Add GitHub Actions workflow: on push to main → SSH → rebuild → restart
- Verify all pipelines work after rebuild

**Acceptance:** `git push` → automatic deploy → all pipelines still work

### Chunk 6: Self-Learning Loop (P2)
**Goal:** System learns from failures and improves daily

**Tasks:**
- Create `/api/v1/observatory/daily-report` endpoint:
  - Summarizes today's pipeline runs
  - Highlights any failures with error details
  - Calculates uptime percentage per stream (7-day rolling)
  - Returns structured JSON for agent consumption
- Health check agent (Agent 3) reads daily report
- Agent writes findings to a structured log
- Weekly: /forge-compile runs as a Claude scheduled agent
  - Reads the week's failure logs
  - Creates wiki articles for new patterns
  - Updates existing articles with new sightings
- Dashboard shows: "Last 7 days: X% uptime, Y pipelines green, Z fixes applied"

**Acceptance:** After 1 week, wiki has new articles from automated analysis. Dashboard shows uptime trend.

---

## Execution Order

```
Session 1: Chunk 1 (fix pipelines) + Chunk 2 (fix dashboard) + Chunk 3 (unified nightly)
Session 2: Chunk 4 (agent workforce) + Chunk 5 (Docker/CI)
Session 3: Chunk 6 (self-learning loop)
```

## Success Criteria

After all 6 chunks:
- [ ] ALL streams green on dashboard every weekday by 23:00 IST
- [ ] Zero manual SSH/intervention for 1 full week
- [ ] Health agent auto-fixes at least 1 failure per week
- [ ] Dashboard accurately reflects all 36+ wiki patterns
- [ ] Docker rebuild deploys cleanly from git push
- [ ] Weekly wiki compilation captures new patterns automatically
