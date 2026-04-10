# JIP Data Engine — Pipeline Orchestration via Claude Scheduled Agents

**Status:** Draft — Pending Review
**Date:** 2026-04-10
**Author:** Nimish Shah + Claude (Forge OS)
**Supersedes:** Cron-based scheduling (jip_scheduler.cron)

---

## 1. Problem Statement

All 17 cron jobs have stopped running. The database is critically stale:

| Stream | Last Date | Days Stale | Severity |
|--------|-----------|------------|----------|
| Equity OHLCV | Apr 6 | 4 days | CRITICAL |
| Equity Technicals | Apr 6 | 4 days | CRITICAL |
| RS Scores | Apr 2 | 8 days | CRITICAL |
| MF NAV | Apr 2 | 8 days | CRITICAL |
| MF Derived | Apr 2 | 8 days | CRITICAL |
| ETF OHLCV | Apr 2 | 8 days | CRITICAL |
| Global Prices | Mar 30 | 11 days | CRITICAL |
| MF Category Flows | Feb 1 | 68 days | CRITICAL |
| Qualitative | Apr 8 | 2 days | STALE |
| Breadth/Regime | Apr 6 | 4 days | CRITICAL |

### Root Cause Analysis

Three orchestration systems exist but none are connected:

1. **Cron file** (`jip_scheduler.cron`) — 17 jobs targeting `docker exec jip-data-engine`.
   Status: not installed on EC2 crontab, or container is down.

2. **Python Scheduler** (`orchestrator/scheduler.py`) — 11 schedule entries with IST support.
   Status: dead code. Not wired to any execution loop.

3. **DAG Executor** (`orchestrator/dag.py`) — Dependency graph, topological sort, crash recovery.
   Status: dead code. Cron bypasses it entirely, calling `python3 -m app.pipelines.X` directly.

Also dead: SLA monitoring, retry policies, Slack alerts, reconciliation checks.

## 2. Proposed Architecture

Replace fragile cron scripts with Claude scheduled agents (remote triggers) that call
HTTP endpoints on the running FastAPI service. Wire up the existing orchestration layer.

```
Claude Scheduled Agents (Anthropic cloud)
    │
    │  HTTP POST (API key auth)
    ▼
FastAPI: /api/v1/pipeline/trigger/{schedule_name}
    │
    ▼
DAG Executor (orchestrator/dag.py)
    ├── Dependency resolution (topological sort)
    ├── Track A/B failure isolation
    └── Crash recovery (--resume from de_pipeline_log)
        │
        ▼
    BasePipeline.run()
    ├── System flag check
    ├── Trading calendar check
    ├── Advisory lock (prevent concurrent runs)
    ├── Pipeline log (de_pipeline_log)
    ├── execute() → actual data ingestion
    ├── validate() → anomaly detection
    └── Record results
        │
        ├──► SLA Monitor → Slack alerts on breach
        ├──► Retry Policy → transient error recovery
        └──► Reconciliation → cross-source validation

Claude Health Agent (daily 23:30 IST)
    │  GET /api/v1/observatory/pulse
    ▼
    Assess freshness → Alert if stale → Auto-trigger backfill
```

## 3. What We Build

### Chunk 1: Pipeline Trigger API
New endpoint that actually executes pipelines (current `/pipeline/replay` only creates
a pending log entry — doesn't run anything).

- `POST /api/v1/pipeline/trigger/{schedule_name}` — run a schedule group
- `POST /api/v1/pipeline/trigger/backfill` — catch up date range
- `POST /api/v1/pipeline/trigger/single/{pipeline_name}` — run one pipeline
- API key auth header (`X-Pipeline-Key`) — agents can't do OAuth

### Chunk 2: Wire Up Orchestration Layer
Connect DAG executor → BasePipeline → SLA → Retry → Alerts.
The code exists. It just needs to be called.

### Chunk 3: Backfill All Stale Data
One-time catch-up to bring all tables to Apr 10, 2026.

### Chunk 4: Claude Scheduled Agents
Set up 7 agents on cron schedules via `/schedule`:

| Agent | Cron (UTC) | IST | Action |
|-------|------------|-----|--------|
| jip-pre-market | `0 2 * * 1-5` | 07:30 | trigger/pre_market |
| jip-eod | `0 13 * * 1-5` | 18:30 | trigger/eod |
| jip-computations | `30 13 * * 1-5` | 19:00 | trigger/rs_computation + regime |
| jip-fund-metrics | `30 15 * * 1-5` | 21:00 | trigger/fund_metrics |
| jip-global | `30 16 * * 1-5` | 22:00 | trigger/global_data |
| jip-weekly | `30 22 * * 6` | Sun 04:00 | trigger/morningstar_weekly |
| jip-monthly | `30 21 1 * *` | 1st 03:00 | trigger/holdings_monthly |
| jip-health | `0 18 * * *` | 23:30 | observatory/pulse check + alert |

### Chunk 5: Monitoring Dashboard Update
Show agent status, next run, freshness heatmap in existing observatory.

## 4. Why Claude Agents Over Cron

| Dimension | Cron | Claude Agents |
|-----------|------|---------------|
| Self-healing | Fails silently | Can diagnose, retry, alert |
| Observability | SSH + grep logs | API-visible, Slack alerts |
| Backfill | Manual SSH + scripts | Agent assesses gaps, fills automatically |
| Dependencies | Time offsets (brittle) | DAG-aware via API |
| Maintenance | SSH into EC2, edit crontab | Version-controlled, edit anywhere |
| Intelligence | Dumb timer | Can check data source availability first |
| Resilience | EC2 reboot = missed jobs | Cloud-hosted, always runs |

## 5. Build Order

1. **Chunk 1** (Pipeline Trigger API) — unblocks everything else
2. **Chunk 2** (Wire orchestration) — makes triggers use proper DAG execution
3. **Chunk 3** (Backfill) — get data current TODAY
4. **Chunk 4** (Claude agents) — set up recurring schedules
5. **Chunk 5** (Dashboard) — ongoing monitoring

## 6. Key Decisions

1. API key auth for triggers, not JWT — agents can't do OAuth flows
2. All cron expressions in UTC, display in IST
3. DAG executor is the single execution entry point
4. Qualitative RSS (30-min interval) stays as in-process APScheduler — below agent minimum
5. Health check agent runs after all daily pipelines should be complete
6. Backfill endpoint accepts date range, executes sequentially per date

## 7. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Agent 1-hour minimum interval | Only qualitative needs <1hr — use APScheduler for that |
| Agent up to 10 min late | Acceptable for daily data pipelines |
| Agent cloud clone (no local state) | Triggers are HTTP calls — no local code needed |
| API key security | Store in agent environment, rotate monthly |
| Pipeline execution timeout | FastAPI background tasks with status polling |
