# Pipeline Orchestration — Chunk Plan

**Total Chunks:** 5
**Build Order:** Sequential (each depends on previous)
**Date:** 2026-04-10

---

## Dependency Graph

```
[Chunk 1: Trigger API]
        │
        ▼
[Chunk 2: Wire Orchestration]
        │
        ▼
[Chunk 3: Backfill Data]   ← operational, not code
        │
        ▼
[Chunk 4: Claude Agents]   ← configuration, not code
        │
        ▼
[Chunk 5: Dashboard]       ← P2, can defer
```

## Chunk Summary

| # | Name | Type | Files Changed | Complexity | Priority |
|---|------|------|---------------|------------|----------|
| 1 | Pipeline Registry + Trigger API | New code | 5 files (2 new, 3 modified) | Medium | P0 |
| 2 | Wire Orchestration Layer | Wiring | 7 files (1 new, 6 modified) | Medium | P0 |
| 3 | Backfill Stale Data | Operations | 0 files (API calls) | Low code / High verify | P0 |
| 4 | Claude Scheduled Agents | Config | 0 files (8 agent setups) | Low | P1 |
| 5 | Dashboard Enhancement | Frontend | 3 files | Low | P2 |

## Detailed Build Order

### Layer 0: Chunk 1 — Pipeline Registry + Trigger API
**Create:**
- `app/pipelines/registry.py` — Pipeline name→class mapping, schedule groups
- `app/api/v1/pipeline_trigger.py` — HTTP endpoints to trigger pipelines

**Modify:**
- `app/api/deps.py` — Add API key auth dependency
- `app/config.py` — Add `pipeline_api_key` setting
- `app/main.py` — Register new router

**Key decisions:**
- API key auth (not JWT) — Claude agents can't do OAuth
- Background tasks for long-running pipelines (backfill)
- ScriptPipeline adapter for computation scripts that aren't BasePipeline subclasses

### Layer 1: Chunk 2 — Wire Orchestration Layer
**Create:**
- `app/orchestrator/executor.py` — PipelineExecutor (ties DAG + SLA + alerts + retry)

**Modify:**
- `app/orchestrator/dag.py` — Plug in pipeline_runner callback
- `app/orchestrator/sla.py` — Add missing SLA configs (7 more pipelines)
- `app/orchestrator/alerts.py` — Initialize from config (Slack webhook, SMTP)
- `app/config.py` — Add SMTP + alert feature flags
- `app/main.py` — Initialize AlertManager + SLAChecker in lifespan
- `app/api/v1/pipeline_trigger.py` — Use PipelineExecutor instead of direct calls

### Layer 2: Chunk 3 — Backfill (Operations)
No code changes. Run backfill via trigger API:
1. Foundation: trading calendar + instrument master
2. Ingestion: BHAV, indices, AMFI, yfinance, FRED, ETF, flows
3. Computations: technicals, RS, breadth, regime, fund metrics

Verify via observatory/pulse after each phase.

### Layer 3: Chunk 4 — Claude Agents (Configuration)
No code changes. Set up 8 scheduled agents via `/schedule`:
- 5 weekday agents (pre-market, EOD, computations, fund metrics, global)
- 1 weekly agent (Morningstar + RS rebuild)
- 1 monthly agent (holdings)
- 1 daily health check agent (safety net)

### Layer 4: Chunk 5 — Dashboard (Frontend)
Add to observatory:
- Freshness heatmap (12 streams, green/yellow/red)
- Agent timeline (today's schedule with status)
- Manual trigger buttons (admin auth)

## Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Some pipelines lack proper entry points | Chunk 1 blocked | ScriptPipeline adapter wraps scripts |
| DAG dependency graph doesn't match schedules | Chunk 2 complexity | Map schedule groups to DAG subgraphs |
| NSE rate limits during backfill | Chunk 3 slow | Sequential dates with 5s delay |
| Claude agent 10-min lateness | Chunk 4 SLA risk | Health check agent as safety net |
| Dashboard auth for manual triggers | Chunk 5 scope creep | Reuse existing admin JWT |

## Code Metrics (Estimated)

| Metric | Value |
|--------|-------|
| New Python files | 3 |
| Modified Python files | ~8 |
| New lines of code | ~600-800 |
| Modified lines | ~100-150 |
| Test files | 2 (trigger API + executor) |
| Dashboard files | 1-2 (if building Chunk 5) |
