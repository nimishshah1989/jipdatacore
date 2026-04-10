# Resilience Session 2 — Chunk Plan
## Chunks 4+5+6: Agent Workforce, Docker Fix, Self-Learning

**Date:** 2026-04-10
**Depends on:** Session 1 (chunks 1-3) — completed and deployed

---

## Chunk 5A: Docker Volume Mount Fix (5 min)
**Goal:** Eliminate docker cp hotfixes — scripts/ available in container without rebuild

**Files:**
- `docker-compose.yml`

**Tasks:**
- Add `./scripts:/app/scripts` volume mount to data-engine service
- Deploy: docker compose down && docker compose up -d

**Acceptance:** `docker exec ... cat /app/scripts/compute/fund_metrics.py` shows latest code without docker cp

**Complexity:** Trivial
**Dependencies:** None

---

## Chunk 4A: Crontab → Trigger API (30 min)
**Goal:** Single execution path — crontab calls trigger API, not docker exec

**Files:**
- `scripts/cron/jip_scheduler.cron`

**Tasks:**
- Rewrite all crontab entries to use `curl -X POST` to trigger API
- Use curl --config for API key (no process list exposure)
- Add weekend logic: Sat/Sun skip equity, still run global/macro
- Add VIX, delivery, FO summary to EOD schedule in registry
- Update nightly_compute.sh to be the canonical entry point

**Acceptance:** `crontab -l` shows only curl calls, no docker exec

**Complexity:** Low
**Dependencies:** Chunk 5A (needs scripts/ mount for trigger API to find scripts)

---

## Chunk 4B: Self-Healing Endpoint + Healing Log (45 min)
**Goal:** API endpoint that tells Agent 3 what's broken and how to fix it

**Files:**
- `app/api/v1/observatory.py` — new endpoint
- `app/models/pipeline.py` — new DeHealingLog model
- `alembic/versions/xxx_add_healing_log.py` — migration

**Tasks:**
- Create `de_healing_log` table: id, date, stream_id, pipeline_triggered, action, result, retries, created_at
- Create `GET /api/v1/observatory/health-action` endpoint:
  - Calls pulse API internally
  - For each stale/critical stream, maps to pipeline name
  - Returns: `[{stream_id, status, pipeline_to_fix, last_healed, retries_today}]`
- Create `POST /api/v1/observatory/healing-result` endpoint:
  - Agent 3 posts results after fix attempt
  - Writes to de_healing_log

**Stream → Pipeline Mapping:**
```
equity_ohlcv        → equity_bhav (via eod schedule)
equity_technicals   → equity_technicals_sql
rs_scores           → relative_strength
market_breadth      → market_breadth
mf_nav              → mf_eod (amfi_nav alias)
mf_derived          → mf_derived
mf_holdings         → morningstar_holdings
global_prices       → yfinance_global
global_technicals   → global_technicals
macro_values        → fred_macro
index_prices        → nse_indices
institutional_flows → fii_dii_flows
```

**Acceptance:** GET /health-action returns actionable fix list; POST /healing-result logs fixes

**Complexity:** Medium
**Dependencies:** None (uses existing observatory infrastructure)

---

## Chunk 4C: Agent Prompts (30 min)
**Goal:** Update all 3 managed agent prompts for complete coverage

**Files:**
- `docs/agents/agent-1-eod-ingestion.md`
- `docs/agents/agent-2-nightly-compute.md`
- `docs/agents/agent-3-health-check.md`

**Tasks:**
- Agent 1 prompt: call trigger/eod, check result, report failures
- Agent 2 prompt: call trigger/nightly_compute, monitor progress
- Agent 3 prompt: call /health-action, fix stale streams, post results, max 2 retries
- Weekend logic in Agent 1: skip equity on Sat/Sun
- All agents use API key from env var

**Acceptance:** Agent prompts are clear, complete, and cover all pipelines

**Complexity:** Low
**Dependencies:** Chunk 4B (Agent 3 needs health-action endpoint)

---

## Chunk 4D: Agent Status on Dashboard (30 min)
**Goal:** Dashboard shows managed agent status with last-run info

**Files:**
- `app/api/v1/observatory.py` — new endpoint
- `app/static/observatory.html` — new dashboard section

**Tasks:**
- Create `GET /api/v1/observatory/agents` endpoint:
  - Queries de_pipeline_log for agent-like pipeline runs (last 7 days)
  - Returns: agent name, last run time, status, pipelines covered
- Add agent status section to observatory.html:
  - 3 agent cards with status badge (green/red/grey)
  - Last run timestamp
  - Pipelines covered count

**Acceptance:** Dashboard shows live agent status with correct colors

**Complexity:** Medium (HTML + API)
**Dependencies:** Chunk 4C (needs agent definitions)

---

## Chunk 6A: Daily Report Endpoint (30 min)
**Goal:** Structured daily summary for agent consumption + dashboard display

**Files:**
- `app/api/v1/observatory.py` — new endpoint

**Tasks:**
- Create `GET /api/v1/observatory/daily-report` endpoint:
  - Query de_pipeline_log for today's runs
  - Query de_healing_log for today's fixes
  - Calculate 7-day rolling uptime per stream
  - Return: {date, runs, failures, fixes, uptime_by_stream, overall_uptime}
- Add uptime summary to dashboard topbar

**Acceptance:** /daily-report returns accurate uptime percentages

**Complexity:** Medium
**Dependencies:** Chunk 4B (needs healing_log table)

---

## Chunk 6B: Dashboard Uptime Display (20 min)
**Goal:** Dashboard shows 7-day uptime trend

**Files:**
- `app/static/observatory.html` — new section

**Tasks:**
- Add uptime section below pipeline heatmap:
  - Overall uptime percentage (big number)
  - Per-stream sparkline (7 days, green/red dots)
  - Healing log: recent auto-fix events
- Fetch from /daily-report endpoint
- Auto-refresh with existing 60s timer

**Acceptance:** Dashboard shows "97.3% uptime, 3 auto-fixes this week"

**Complexity:** Low-Medium (frontend only)
**Dependencies:** Chunk 6A

---

## Build Order

```
5A (docker fix, 5 min)
 ↓
4A (crontab rewrite, 30 min)
 ↓
4B (health endpoint + healing log, 45 min)
 ↓
4C (agent prompts, 30 min)  ←  4D (dashboard agents, 30 min)  [parallel]
 ↓
6A (daily report, 30 min)
 ↓
6B (dashboard uptime, 20 min)
```

**Total: 7 chunks, ~3.5 hours estimated**
