# Pipeline Chunk 1: Pipeline Registry + Trigger API

**Layer:** 0
**Dependencies:** None
**Complexity:** Medium
**Status:** pending

## Overview
Build the missing link: a pipeline registry that maps names to classes, and HTTP endpoints
that actually execute pipelines. The current `/pipeline/replay` only creates a pending log
entry — it doesn't run anything.

## Files to Create
- `app/pipelines/registry.py` — Pipeline name → class mapping + schedule groups
- `app/api/v1/pipeline_trigger.py` — HTTP trigger endpoints

## Files to Modify
- `app/api/deps.py` — Add `verify_pipeline_key` dependency
- `app/config.py` — Add `pipeline_api_key` setting
- `app/main.py` — Register new router

## Detailed Spec

### 1. Pipeline Registry (`app/pipelines/registry.py`)

Two registries:

**PIPELINE_REGISTRY** — maps pipeline_name string → pipeline class:
- Discover all BasePipeline subclasses across `app/pipelines/` subdirectories
- Each pipeline class has a `pipeline_name` attribute — use that as key
- Expose `get_pipeline(name) -> BasePipeline` that instantiates by name
- Expose `list_pipelines() -> list[str]`

**SCHEDULE_REGISTRY** — maps schedule group → ordered list of pipeline names:
- Source from existing `CronSchedule.default()` in `app/orchestrator/scheduler.py`
- Each `ScheduleEntry` has `.name` and `.pipelines` — build dict from that
- Also add computation schedules not in scheduler.py:
  - `technicals`: ["equity_technicals_sql", "equity_technicals_pandas"]
  - `fund_metrics`: ["mf_derived"]
  - `etf_global`: ["etf_technicals", "etf_rs", "global_technicals", "global_rs"]
- Expose `get_schedule(name) -> list[str]` and `list_schedules() -> list[str]`

### 2. API Key Auth (`app/api/deps.py`)

New dependency function:
```python
async def verify_pipeline_key(
    x_pipeline_key: Annotated[str, Header()]
) -> bool:
```
- Compare against `settings.pipeline_api_key` using `hmac.compare_digest`
- Raise HTTPException 401 if invalid
- Log all attempts (valid and invalid)

### 3. Config (`app/config.py`)

Add field:
```python
pipeline_api_key: str = ""  # PIPELINE_API_KEY env var
```

### 4. Trigger Endpoints (`app/api/v1/pipeline_trigger.py`)

Router prefix: `/api/v1/pipeline`

**POST /trigger/{schedule_name}**
- Depends: verify_pipeline_key, get_db
- Query: `business_date: Optional[date] = None` (defaults to today IST)
- Lookup schedule → get pipeline list → run each via BasePipeline.run()
- Run sequentially (respects implicit ordering within schedule group)
- Return: `{ schedule_name, business_date, pipelines: [{ name, status, rows_processed, rows_failed, duration_seconds, error }] }`

**POST /trigger/single/{pipeline_name}**
- Depends: verify_pipeline_key, get_db
- Query: `business_date: Optional[date] = None`
- Instantiate single pipeline → run()
- Return: `{ pipeline_name, business_date, status, rows_processed, rows_failed, duration_seconds, error }`

**POST /trigger/backfill**
- Depends: verify_pipeline_key, get_db
- Body: `{ pipeline_names: list[str], start_date: date, end_date: date }`
- Validate: end_date >= start_date, max range 90 days, all pipeline names valid
- Run as BackgroundTask — return job_id immediately
- For each date in range (ascending), run each pipeline
- Store results in module-level dict keyed by job_id

**GET /trigger/status/{job_id}**
- Depends: verify_pipeline_key
- Return: `{ job_id, status: "running"|"completed"|"failed", progress: { dates_done, dates_total }, results: [...] }`

**GET /trigger/schedules**
- Depends: verify_pipeline_key
- Return: all schedule groups with their pipeline lists

**GET /trigger/pipelines**
- Depends: verify_pipeline_key
- Return: all registered pipeline names

### 5. Background Job Tracking

Module-level dict:
```python
_jobs: dict[str, PipelineJob] = {}

@dataclass
class PipelineJob:
    job_id: str
    status: str  # pending, running, completed, failed
    created_at: datetime
    schedule_name: str | None
    business_dates: list[date]
    results: list[dict]
    progress: dict
```

Prune completed jobs older than 24 hours on each new job creation.

### 6. Computation Scripts Integration

The cron file calls scripts like `scripts/compute/technicals_sql.py` which are NOT
BasePipeline subclasses. For these, create thin wrapper pipelines or call them as
subprocess. Decision: create a `ScriptPipeline` adapter in registry.py that wraps
`subprocess.run(["python3", "-m", script_module])` inside BasePipeline.execute().

## Acceptance Criteria
- [ ] `POST /api/v1/pipeline/trigger/eod?business_date=2026-04-10` runs the EOD pipeline group
- [ ] Returns JSON with per-pipeline status, rows, duration
- [ ] Rejects requests without valid X-Pipeline-Key (401)
- [ ] `POST /api/v1/pipeline/trigger/single/nse_bhav` runs just BHAV
- [ ] `GET /api/v1/pipeline/trigger/schedules` lists all schedule groups
- [ ] Invalid schedule/pipeline names return 404
- [ ] Backfill creates background job and returns job_id
- [ ] Status endpoint shows progress of background jobs

## Risk
- Some pipelines may not have proper `__main__` entry points — registry must handle gracefully
- Computation scripts (technicals, RS, breadth) are standalone scripts, not BasePipeline — need adapter
