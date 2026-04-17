# Data Audit Observatory — Handoff Note

Branch: `claude/review-codebase-ingestion-YujmR`

## Goal (user's verbatim request)
Build a live view at data.jslwealth.in that shows:
1. **Metric inventory**: metric name, source table(s), # instruments by type
   (equity/MF/indices/sectors), time period available (min/max dates)
2. **Table-level cron schedule**: how each table is set to update
3. **Discrepancy report**: tables not set to refresh, incomplete tables,
   bad-data tables, etc.

## What is DONE

### 1. Backend endpoint `GET /api/v1/observatory/audit`
File: `app/api/v1/observatory.py` (appended at end)

Added new endpoint that returns JSON with:
- `summary` — counters
- `metric_inventory[]` — per-stream: metrics list, metric_count,
  instrument_counts {category: N}, total_instruments, min_date, max_date,
  row_count_exact, row_count_approx, hours_old, freshness, quarantined_rows,
  entity_column, exists
- `table_schedule[]` — per-stream: pipeline, schedule_group, cron_expression,
  cron_label, last_success_at, last_run_at, last_run_status, next_run,
  is_scheduled, is_triggered, exists
- `discrepancies{}` — unscheduled_streams[], stale_streams[], empty_streams[],
  quarantined_tables[], unmapped_db_tables[]

Implementation notes:
- Uses existing `get_observatory_db()` isolated connection
- Cross-references `STREAM_DEFINITIONS` (in observatory.py),
  `STREAM_PIPELINE_MAP` (in observatory.py), `CronSchedule.default()` (from
  `app/orchestrator/scheduler.py`), and `SCHEDULE_REGISTRY` / `DAG_ALIAS`
  (from `app/pipelines/registry.py`)
- Auto-detects entity column via `_ENTITY_CANDIDATES` list
- For `rs_scores`: breaks down `COUNT(DISTINCT entity_id) GROUP BY entity_type`
- Filters out housekeeping cols via `_METRIC_EXCLUDE_COLS`
- `_describe_cron()` helper converts cron expr → human label
- Endpoint is already listed in module docstring

This file has NOT been committed yet. Run `git diff app/api/v1/observatory.py`
to see the full addition.

## What is NOT YET DONE

### 2. HTML dashboard at `app/static/data-audit.html`
Does not exist yet. Must match the dark-navy / teal / Inter style used in
`app/static/observatory.html` (reuse the CSS variables from `:root`).

Required sections (three cards/tables):
- **Metric Inventory table**: columns — Stream | Table | Category | Metrics
  (expandable chips) | Equity # | MF # | Index # | Sector # | Global # |
  Min Date | Max Date | Row Count | Freshness badge
- **Cron Schedule table**: columns — Stream | Table | Pipeline |
  Schedule Group | Cron | Cron Label | Last Success | Last Run Status |
  Next Run | Type (scheduled/triggered/unscheduled badge)
- **Discrepancy panel**: four sub-cards:
  - Stale/critical streams (red) with pipeline to fix
  - Empty streams (amber) with pipeline
  - Unscheduled streams (amber) with reason
  - Quarantined-data tables (red) with row count
  - Unmapped DB tables (grey) — just table names

UX requirements:
- Poll `/api/v1/observatory/audit` every 60 seconds with a countdown in
  topbar (mirror observatory.html pattern)
- Sortable column headers (click to sort by any column)
- Search/filter box at top of each table
- Freshness badge colors: fresh=green, stale=amber, critical=red, unknown=grey
- Top banner with summary counts from `summary` object
- No external framework — self-contained HTML with vanilla JS + one script
  tag for d3 (if needed for charts, but probably not needed)

### 3. UI route registration
File: `app/api/v1/observatory_ui.py` — add a second route:
```python
_AUDIT_HTML_PATH = os.path.join(_STATIC_DIR, "data-audit.html")

@router.get("/data-audit", include_in_schema=False, response_class=HTMLResponse)
async def data_audit_dashboard() -> FileResponse:
    abs_path = os.path.abspath(_AUDIT_HTML_PATH)
    if not os.path.exists(abs_path):
        return HTMLResponse(
            content="<h1>Data audit dashboard not found</h1>", status_code=404,
        )
    return FileResponse(abs_path, media_type="text/html")
```

No change needed to `app/api/v1/__init__.py` — `observatory_ui_router` is
already registered.

### 4. Commit and push
```
git add app/api/v1/observatory.py app/api/v1/observatory_ui.py \
        app/static/data-audit.html DATA_AUDIT_HANDOFF.md
git commit -m "Add live data audit observatory at /data-audit"
git push -u origin claude/review-codebase-ingestion-YujmR
```

## Useful context

### Key files to read first
- `app/api/v1/observatory.py` — the audit endpoint I added (see end of file)
- `app/api/v1/observatory_ui.py` — UI router pattern
- `app/static/observatory.html` — style template to mirror
- `app/orchestrator/scheduler.py` — `CronSchedule.default()` source of truth
- `app/pipelines/registry.py` — `SCHEDULE_REGISTRY`, `DAG_ALIAS`

### Example `curl` to test endpoint (once server is running)
```
curl -s http://localhost:8010/api/v1/observatory/audit | jq '.summary'
```

### Streams tracked (from STREAM_DEFINITIONS in observatory.py)
25+ streams across categories: equity, mf, etf, global, macro, flows,
qualitative. See `STREAM_DEFINITIONS` list in observatory.py for the full
registry of (stream_id, label, table, date_col, category).

### What the user wants next
After this is shipped, the page should be wired to data.jslwealth.in —
likely behind the same nginx/ALB rule as the existing /observatory route.
No DNS or infra change needed if that routing already forwards to port 8010.

## Failure mode encountered
The model kept hitting "Stream idle timeout - partial response received"
mid-generation of the ~500-line HTML file. Solution: in the new session,
generate the HTML in smaller chunks — either skeleton first then fill in
each table separately, or ask the model to keep output tight and avoid
excessive markdown between tool calls.
