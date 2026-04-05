# Chunk 15: Pipeline Monitoring Dashboard

**Layer:** 5
**Dependencies:** C3, C4
**Complexity:** Medium
**Status:** pending

## Files

- `dashboard/__init__.py` (or `dashboard/app.py` if standalone)
- `dashboard/index.html`
- `dashboard/static/main.js`
- `dashboard/static/styles.css`
- `dashboard/api.py` (thin API layer serving dashboard data from Data Engine)
- `tests/dashboard/test_dashboard_api.py`

## Acceptance Criteria

- [ ] **Live pipeline status:** Shows running/complete/failed/partial/holiday_skip for each pipeline track (equity, MF, indices, flows, F&O, qualitative, RS, regime, reconciliation) for today
- [ ] **Data ingestion progress:** Rows processed, rows failed, time elapsed for currently-running pipeline
- [ ] **Today's anomalies by severity:** Critical anomalies highlighted in red; warnings in yellow; info in blue; unresolved anomalies prominently displayed
- [ ] **System health indicators:** Redis connectivity (ping latency), DB active connections, EC2 disk usage (%), last successful pipeline run time per track
- [ ] **SLA tracking:** For each pipeline with a defined SLA deadline, show: expected completion time, actual completion time (or "running"), SLA status (met/missed/at risk)
- [ ] **Historical pipeline run viewer:** Filter by pipeline name and date range; shows run history from `de_pipeline_log`; click row to expand `track_status` JSONB detail
- [ ] **Auto-refresh every 30 seconds** — no manual reload required
- [ ] **Anomaly detail panel:** Click an anomaly to see entity, type, expected range, actual value, pipeline context
- [ ] Dashboard served at `http://127.0.0.1:8099` (SSH tunnel only — not exposed publicly)
- [ ] Professional wealth management aesthetic: white background, subtle borders, teal accents (#1D9E75)
- [ ] Numbers right-aligned, text left-aligned in all tables
- [ ] Desktop-first layout (advisors and admins use large monitors)

## Notes

**Access:** Dashboard is on `127.0.0.1:8099` — SSH tunnel required to view: `ssh -L 8099:127.0.0.1:8099 ubuntu@13.206.34.214`. This is intentional — dashboard has admin access to pipeline data and must not be publicly accessible.

**Technology choice:** The dashboard can be:
- A standalone FastAPI app (port 8099) serving HTML/JS that calls the main Data Engine admin API
- A simple HTML + vanilla JS page served by the main FastAPI app under `/dashboard` with a different port binding
- A lightweight React/Vue SPA

Recommended: Standalone FastAPI app on port 8099 serving HTML with fetch calls to `localhost:8010/api/v1/admin/*`. This keeps the dashboard separate from the main API.

**Data sources for dashboard (all from Data Engine admin API):**
- Pipeline status: `GET /admin/pipeline/status`
- Anomalies: `GET /admin/anomalies?date=today&resolved=false`
- System health: custom health endpoint or Redis/DB connection checks

**SLA deadlines to track:**
| Pipeline | SLA Deadline |
|----------|-------------|
| Pre-Market | 08:00 IST |
| Equity EOD | 19:30 IST |
| MF NAV | 22:30 IST |
| FII/DII flows | 20:00 IST |
| RS computation | 23:00 IST |
| Regime update | 23:30 IST |

**Aesthetic guidelines (from frontend-viz.md):**
- White backgrounds, subtle borders, teal accents (`#1D9E75`)
- Data density: information-rich screens — admins want full detail
- Dates displayed as DD-MMM-YYYY (e.g., 05-Apr-2026) in IST
- Numbers: right-align in tables
- No emojis, no flashy animations — professional financial tool aesthetic

**Auto-refresh implementation:** Use `setInterval(() => fetchData(), 30000)` in JS. Show last-updated timestamp to confirm refresh is working. Do not show a loading spinner on refresh (distracting) — silently update data in place.
