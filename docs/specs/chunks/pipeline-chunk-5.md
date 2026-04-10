# Pipeline Chunk 5: Dashboard Enhancement

**Layer:** 4
**Dependencies:** Pipeline Chunks 1-4
**Complexity:** Low
**Status:** pending

## Overview
Enhance the existing observatory dashboard to show Claude agent status,
data freshness heatmap, and manual trigger buttons.

## Files to Modify
- `app/api/v1/observatory.py` — Add agent status + trigger history endpoints
- `dashboard/index.html` or equivalent — Add freshness heatmap, trigger buttons

## New API Endpoints

### GET /api/v1/observatory/agent-status
Returns last trigger time, next scheduled run, and result for each agent schedule:
```json
{
  "agents": [
    {
      "schedule_name": "pre_market",
      "last_triggered": "2026-04-10T07:32:00+05:30",
      "last_status": "success",
      "next_scheduled": "2026-04-11T07:30:00+05:30",
      "pipelines_run": 3,
      "rows_processed": 6234
    }
  ]
}
```
Source: query `de_pipeline_log` grouped by schedule windows.

### GET /api/v1/observatory/freshness-map
Returns per-stream freshness with color coding:
```json
{
  "streams": [
    {
      "name": "equity_ohlcv",
      "last_date": "2026-04-10",
      "hours_since": 2.5,
      "status": "green",  // green <6h, yellow 6-24h, red >24h
      "row_count": 150234
    }
  ]
}
```

### POST /api/v1/observatory/manual-trigger/{schedule_name}
- Same as pipeline trigger but accessible from dashboard (admin JWT auth)
- Calls PipelineExecutor.run_schedule() internally

## Dashboard UI

### Freshness Heatmap
- Grid of all 12 data streams
- Color: green (fresh), yellow (stale), red (critical)
- Click to see last pipeline log + row counts
- Auto-refresh every 60 seconds

### Agent Timeline
- Horizontal timeline showing today's scheduled runs
- Past runs: green (success) / red (failed) / grey (skipped)
- Future runs: outlined
- Click for run details

### Manual Trigger Panel
- Dropdown: select schedule group
- Date picker: business date (defaults to today)
- "Run Now" button
- Shows progress spinner while running

## Acceptance Criteria
- [ ] Freshness heatmap shows all 12 streams with correct colors
- [ ] Agent timeline shows today's schedule with run statuses
- [ ] Manual trigger button executes pipeline group
- [ ] Dashboard auto-refreshes every 60 seconds
- [ ] Works on desktop (1920px+) — wealth management aesthetic

## Risk
- Dashboard is a separate container — needs CORS for new endpoints
- Manual trigger needs admin JWT — dashboard must have auth
