# Chunk 11 — Pipeline registration + cron wiring

**Complexity**: S
**Blocks**: chunk-12
**Blocked by**: chunk-8, chunk-10

## Goal
Register `compute_indicators_v2` as a first-class pipeline in the JIP pipeline registry, wire it into the nightly cron chain, and have the observatory dashboard show it automatically. Remove the old `technicals` step from `runner.py` chain.

## Files
- **Modify**: `app/pipelines/registry.py`
  - Add a new entry:
    ```python
    PipelineSpec(
        name="compute_indicators_v2",
        display_name="Technical Indicators (v2 — pandas-ta)",
        handler="app.computation.indicators_v2.runner.run_indicators_v2_pipeline",
        schedule="nightly_compute",
        depends_on=["eod", "amfi_late", "index_prices_eod", "yfinance_eod"],
        sla_minutes=30,
        priority=10,
    )
    ```
- **Create**: `app/computation/indicators_v2/runner.py`
  - `async def run_indicators_v2_pipeline(session, business_date, pipeline_run_id)`
  - Calls each asset wrapper in order:
    1. `compute_equity_indicators(session, from_date=business_date - 5, to_date=business_date)` — 5-day lookback to cover weekends/holidays
    2. `compute_index_indicators(session, from_date=business_date - 5, to_date=business_date)`
    3. `compute_etf_indicators(...)`
    4. `compute_global_indicators(...)`
    5. `compute_mf_indicators(...)` — last, since largest
  - Logs start/end per asset to `de_pipeline_log`
  - Uses `de_pipeline_run_id` for audit trail
  - On any asset failure: logs error, continues to next asset (partial success preferred over total failure), returns overall status `partial` if any fail
- **Modify**: `app/computation/runner.py`
  - Remove the call to the old `technicals.run_technicals()` function from `run_full_computation_pipeline()`
  - Replace with call to `run_indicators_v2_pipeline()`
  - Keep the downstream steps (breadth, rs, regime, sectors) — they now read from the new-schema tables (already renamed in place)
- **Modify**: `scripts/cron/jip_scheduler.cron`
  - `nightly_compute` entry already exists and calls `runner.py` — no cron change needed since runner.py is the entry point
  - Add a comment documenting that compute_indicators_v2 replaced the old technicals step on {cutover_date}

## Dashboard integration
- Observatory (`app/api/v1/observatory.py`) queries `de_cron_run` + `de_pipeline_log` and auto-picks up the new pipeline name. No dashboard code change required.
- Verify: after the first nightly run, `GET /api/v1/observatory/pulse` should show `compute_indicators_v2` in the pipeline list with status "success"

## Acceptance criteria
- `app/pipelines/registry.py` lists `compute_indicators_v2` with correct dependencies
- Manual trigger works: `curl -X POST -H "X-Pipeline-Key: ..." https://data.jslwealth.in/api/v1/pipeline/trigger/compute_indicators_v2?business_date=2026-04-14`
- Returns 200, pipeline runs to completion, logs in `de_pipeline_log`
- `de_equity_technical_daily`, `de_index_technical_daily`, `de_etf_technical_daily`, `de_global_technical_daily`, `de_mf_technical_daily` all have fresh rows for the business date after the run
- Observatory dashboard shows the new pipeline green
- Next scheduled nightly run (at IST 00:30) completes successfully — monitor for 1 night before marking done
- Old `technicals.run_technicals()` no longer called by anything (grep shows only legacy references in `technicals.py` itself, which is deleted in chunk 12)
- `pytest tests/` all green

## Verification commands
```bash
# Manual trigger
curl -X POST -H "X-Pipeline-Key: $PIPELINE_KEY" \
  "https://data.jslwealth.in/api/v1/pipeline/trigger/compute_indicators_v2?business_date=$(date +%Y-%m-%d)"

# Check run status
psql -h ... -c "SELECT * FROM de_cron_run WHERE schedule_name = 'compute_indicators_v2' ORDER BY started_at DESC LIMIT 5"

# Verify all 5 tables updated
psql -h ... -c "SELECT 'equity' AS t, MAX(date) FROM de_equity_technical_daily UNION ALL SELECT 'index', MAX(date) FROM de_index_technical_daily UNION ALL SELECT 'etf', MAX(date) FROM de_etf_technical_daily UNION ALL SELECT 'global', MAX(date) FROM de_global_technical_daily UNION ALL SELECT 'mf', MAX(nav_date) FROM de_mf_technical_daily"
```
