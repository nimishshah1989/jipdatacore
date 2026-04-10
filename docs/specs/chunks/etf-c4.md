# ETF-C4: Pipeline Wiring + Scheduler

**Parent Plan:** [etf-expansion-chunk-plan.md](../etf-expansion-chunk-plan.md)
**Complexity:** Low
**Dependencies:** C1 (NseEtfSyncPipeline must exist), C2 (global ETFs seeded)
**Blocks:** C5 (Deploy)

---

## Description

Wire the new `NseEtfSyncPipeline` into the pipeline registry and EOD scheduler so it runs automatically after `equity_bhav` each day. Also fix the existing gap where `etf_prices` may not be in the `CronSchedule.default()` EOD entry.

---

## Files to Create/Modify

| Action | File | Purpose |
|--------|------|---------|
| MODIFY | `app/pipelines/registry.py` | Register NseEtfSyncPipeline in _PIPELINE_CLASSES |
| MODIFY | `app/orchestrator/scheduler.py` | Add nse_etf_sync to EOD schedule + DAG dependency |

---

## Detailed Implementation Steps

### Step 1: Register Pipeline in `app/pipelines/registry.py`

1. Add import: `from app.pipelines.etf.nse_etf_sync import NseEtfSyncPipeline`
2. Add entry to `_PIPELINE_CLASSES` dict:
   ```python
   "nse_etf_sync": NseEtfSyncPipeline,
   ```
3. Verify the pipeline can be imported and instantiated:
   ```python
   pipeline = _PIPELINE_CLASSES["nse_etf_sync"]()
   ```

### Step 2: Update EOD Schedule in `app/orchestrator/scheduler.py`

1. Add `"nse_etf_sync"` to `SCHEDULE_REGISTRY["eod"]` pipeline list
2. Add DAG dependency: `nse_etf_sync` must run after `equity_bhav`
   - This ensures BHAV data is ingested before the NSE ETF copy runs
   - The dependency declaration follows the existing DAG pattern in the scheduler
3. **Bug fix:** Verify `etf_prices` is in `CronSchedule.default().eod.pipelines`
   - If missing, add it (this is a known gap from the chunk plan)
4. Verify no circular dependencies are introduced in the DAG

### Step 3: Verify Pipeline Triggerable via API

1. The pipeline should be importable from registry: `get_pipeline("nse_etf_sync")`
2. The orchestrator API `/api/v1/orchestrator/trigger` should accept `nse_etf_sync` as a valid pipeline name
3. DAG ordering should show: `equity_bhav` -> `nse_etf_sync` -> (downstream)

---

## Daily Automation Flow (Post-Wiring)

```
18:30 IST -- EOD schedule fires:
  +-- equity_bhav (NSE BHAV download + parse)
  |     +-- nse_etf_sync (copy NSE ETF rows de_equity_ohlcv -> de_etf_ohlcv)
  +-- etf_prices (yfinance for 163 global ETFs, 4 batches of 50)
  +-- ... other EOD pipelines

After EOD:
  +-- etf_technicals (compute indicators for all ~230 ETFs)
  +-- etf_rs (RS scores vs SPY and ^SPX for all ~230 ETFs)
```

---

## Acceptance Criteria

- [ ] `nse_etf_sync` is in the pipeline registry, importable, and instantiable
- [ ] DAG shows `nse_etf_sync` runs after `equity_bhav`
- [ ] `etf_prices` is in `CronSchedule.default()` EOD entry (bug fix if missing)
- [ ] No circular dependencies in DAG
- [ ] Pipeline can be triggered via orchestrator API: `POST /api/v1/orchestrator/trigger {"pipeline": "nse_etf_sync"}`
- [ ] ruff + mypy clean

---

## Verification Steps

```python
# In Python REPL or test
from app.pipelines.registry import _PIPELINE_CLASSES
assert "nse_etf_sync" in _PIPELINE_CLASSES

from app.orchestrator.scheduler import SCHEDULE_REGISTRY
eod_pipelines = SCHEDULE_REGISTRY["eod"]
assert "nse_etf_sync" in eod_pipelines
assert "etf_prices" in eod_pipelines
```

```bash
# Via API (after deploy)
curl -X POST http://localhost:8010/api/v1/orchestrator/trigger \
  -H "Content-Type: application/json" \
  -d '{"pipeline": "nse_etf_sync"}'
# Expected: 200 OK with execution status
```
