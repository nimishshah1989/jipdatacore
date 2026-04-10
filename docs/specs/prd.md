# JIP Pipeline Orchestration — PRD

**Status:** Pending Approval
**Date:** 2026-04-10
**Sprint:** Pipeline rescue + Claude agent infrastructure
**Deploy Target:** EC2 (13.206.34.214) + Anthropic Cloud (scheduled agents)

---

## 1. Goal

Get the JIP database fully current (all 12 streams updated to today) and replace
the broken cron-based scheduling with Claude scheduled agents that are self-healing,
observable, and require zero SSH maintenance.

## 2. Success Criteria

- [ ] All 12 data streams updated to 2026-04-10
- [ ] Pipeline trigger API operational at data.jslwealth.in
- [ ] 7 Claude scheduled agents running on correct schedules
- [ ] Health check agent detecting staleness and alerting within 1 hour
- [ ] Zero manual intervention required for daily data updates going forward
- [ ] DAG executor, SLA monitor, retry, and alerts all wired up and operational

## 3. Non-Goals

- Rewriting pipeline logic (ingestion code works fine)
- Changing data sources
- New data streams
- Frontend redesign

## 4. Chunks

### Chunk 1: Pipeline Trigger API
**Files:** `app/api/v1/pipeline_trigger.py`, `app/api/deps.py`
**Complexity:** Medium
**Dependencies:** None
**Acceptance Criteria:**
- POST /api/v1/pipeline/trigger/{schedule_name} executes pipeline group
- POST /api/v1/pipeline/trigger/backfill accepts date range
- POST /api/v1/pipeline/trigger/single/{pipeline_name} runs one pipeline
- API key auth via X-Pipeline-Key header
- Returns execution status, rows processed, duration
- Long-running pipelines execute as background tasks with status polling
- Proper error responses for invalid schedule names, locked pipelines

### Chunk 2: Wire Orchestration Layer
**Files:** `app/orchestrator/executor.py` (new), `app/orchestrator/dag.py`, `app/orchestrator/sla.py`, `app/orchestrator/alerts.py`, `app/orchestrator/retry.py`
**Complexity:** Medium
**Dependencies:** Chunk 1
**Acceptance Criteria:**
- DAG executor resolves dependencies before running pipelines
- Track A failure (equity) skips RS/regime but not MF/global
- SLA monitor checks deadlines and posts to Slack on breach
- Retry policy applies (3 attempts for transient, fail-fast for persistent)
- Reconciliation runs after EOD completion
- All execution goes through BasePipeline.run() (locking, logging, validation)

### Chunk 3: Backfill Stale Data
**Files:** No new files — uses trigger API
**Complexity:** Low (execution), High (verification)
**Dependencies:** Chunks 1 + 2
**Acceptance Criteria:**
- Equity OHLCV: Apr 7-10 ingested and verified (row counts match expected)
- MF NAV: Apr 3-10 ingested
- RS Scores: full recompute from current OHLCV
- Technicals: recompute Apr 7-10
- Breadth/Regime: recompute Apr 7-10
- Global Prices: Mar 31 - Apr 10
- ETF OHLCV: Apr 3-10
- MF Category Flows: investigate source, backfill Feb-Apr
- MF Derived: recompute after NAV backfill
- All row counts logged before and after

### Chunk 4: Claude Scheduled Agents
**Files:** None (configured via /schedule CLI)
**Complexity:** Low
**Dependencies:** Chunks 1 + 2 + 3 (data must be current first)
**Acceptance Criteria:**
- 7 recurring agents created with correct cron schedules
- 1 health check agent created
- Each agent's prompt includes: API endpoint, expected response, error handling
- Agents verified: trigger manually, confirm pipeline executes
- API key stored securely in agent environment variables

### Chunk 5: Dashboard Enhancement
**Files:** `dashboard/`, `app/api/v1/observatory.py`
**Complexity:** Low
**Dependencies:** Chunks 1-4
**Acceptance Criteria:**
- Observatory shows agent last-run time and next scheduled run
- Freshness heatmap: green (<6hr), yellow (6-24hr), red (>24hr)
- Manual trigger buttons for each schedule group
- Agent execution history (last 7 days)

## 5. Pipeline Schedule Reference

### Weekday (Mon-Fri)
| Time (IST) | Schedule | Pipelines |
|------------|----------|-----------|
| 07:30 | pre_market | BHAV, corporate actions, indices |
| 09:00 | t1_delivery | FII/DII flows |
| 18:30 | eod | BHAV, indices, AMFI NAV, yfinance, FRED |
| 19:00 | rs_computation | Relative strength (after EOD) |
| 19:30 | technicals | SQL technicals + pandas technicals |
| 20:30 | regime | Breadth + regime detection (after RS) |
| 21:00 | fund_metrics | MF derived metrics |
| 22:00 | global | yfinance global + ETF technicals |
| 23:00 | reconciliation | Cross-source validation |

### Weekly
| Day | Time (IST) | Pipelines |
|-----|------------|-----------|
| Sunday 02:00 | full_rs_rebuild | Full RS historical rebuild |
| Sunday 04:00 | morningstar_weekly | Morningstar fund master |

### Monthly
| Day | Time (IST) | Pipelines |
|-----|------------|-----------|
| 1st 03:00 | holdings_monthly | Morningstar portfolio |

## 6. API Key Security

- Generate 256-bit random key for pipeline triggers
- Store in .env on EC2 (never in code)
- Store in Claude agent environment variables
- Rotate monthly via /schedule update
- Log all trigger attempts (with/without valid key)

## 7. Rollback Plan

If Claude agents prove unreliable:
- The trigger API works with any HTTP client
- Fall back to cron calling `curl -X POST` with API key
- Or use GitHub Actions scheduled workflows as a middle ground

## 8. Timeline

| Chunk | Estimate | Priority |
|-------|----------|----------|
| 1: Trigger API | Build now | P0 |
| 2: Wire orchestration | Build now | P0 |
| 3: Backfill data | Run immediately after 1+2 | P0 |
| 4: Claude agents | Set up after backfill | P1 |
| 5: Dashboard | After agents are running | P2 |
