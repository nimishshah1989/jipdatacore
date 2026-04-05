# Chunk 10: Morningstar Integration

**Layer:** 3
**Dependencies:** C4, C8
**Complexity:** Medium
**Status:** pending

## Files

- `app/pipelines/morningstar/__init__.py`
- `app/pipelines/morningstar/client.py`
- `app/pipelines/morningstar/fund_master.py`
- `app/pipelines/morningstar/holdings.py`
- `app/pipelines/morningstar/risk.py`
- `app/pipelines/morningstar/isin_resolver.py`
- `tests/pipelines/morningstar/test_client.py`
- `tests/pipelines/morningstar/test_fund_master.py`
- `tests/pipelines/morningstar/test_holdings.py`

## Acceptance Criteria

- [ ] **Morningstar HTTP client:** Async client with retry logic (3 retries, exponential backoff); handles rate limiting (429) by backing off; credential management via `MORNINGSTAR_API_KEY` from AWS Secrets Manager
- [ ] **Single API endpoint strategy (v2.0):** All Morningstar data fetched via one fund detail endpoint: `{IdType}/{Identifier}?datapoints=Name,CategoryName,BroadCategoryGroup,NetExpenseRatio,ManagerName,TotalNetAssets,...`; mapped via the 5,489 equity funds already in `de_mf_master`
- [ ] **Fund master refresh (weekly — Sunday):** For each active fund in `de_mf_master`, fetch fund detail; `UPDATE de_mf_master SET category_name, broad_category, expense_ratio, primary_benchmark, ...` where changed; INSERT `de_mf_lifecycle` for category changes and name changes
- [ ] **Holdings refresh (monthly — 1st of month):** Fetch portfolio holdings for each target fund; `INSERT INTO de_mf_holdings ON CONFLICT DO UPDATE`; include instrument_id, weight_pct, sector, market_value
- [ ] **ISIN → instrument_id resolution:** For each holding row, resolve ISIN to `instrument_id` via `SELECT instrument_id FROM de_instrument WHERE isin = :isin`; log unresolved ISINs as anomalies (some instruments may not be in `de_instrument` yet)
- [ ] **Risk data fetch:** Fetch risk statistics (Sharpe ratio, standard deviation, alpha, beta vs benchmark) from Morningstar; store in `de_mf_master` or dedicated risk table
- [ ] **Rate limiting:** Respect Morningstar API rate limits; implement per-second and per-day request caps; log call count in `de_pipeline_log.track_status` for cost tracking
- [ ] **Error handling:** On 404 (fund not found in Morningstar), mark `de_mf_master.is_active = FALSE` if fund has been inactive >30 days; log in `de_mf_lifecycle`
- [ ] **Credential management:** `MORNINGSTAR_API_KEY` and `MORNINGSTAR_BASE_URL` loaded from AWS Secrets Manager at startup; never hardcoded
- [ ] All tests use mocked Morningstar API responses — no live network calls in tests
- [ ] Stub endpoints gracefully if API URL not yet confirmed — log warning and return without crash

## Notes

**Morningstar API status (v2.0):** The project memory doc notes that 10 Morningstar API endpoints are available and richer than initially assumed. The v2.0 spec mandates using one primary endpoint (fund detail via IdType/Identifier). However, the client should be structured to call additional endpoints as they are confirmed during the sprint.

**Target universe filter:** `de_mf_master` contains 13,380 total funds. The Morningstar integration targets the equity growth regular universe (~450-550 funds). Filter: `broad_category` is equity + `Accumulated`/`Growth` in name + `Regular` in name + `is_active = TRUE`.

**Holdings table schema (de_mf_holdings):**
- `mstar_id`, `report_date`, `instrument_id` (nullable — not all holdings are in de_instrument), `isin`, `security_name`, `weight_pct NUMERIC(6,2)`, `market_value NUMERIC(18,4)`, `sector`, `created_at`
- PK: `(mstar_id, report_date, isin)` or `(mstar_id, report_date, instrument_id)`

**Holdings data volumes:** 2M+ rows currently in `fie2-db-1.mf_pulse.fund_holding_detail`. Monthly refresh adds ~450 funds × ~50 holdings each = ~22,500 new rows per month.

**ISIN resolution note:** Some holdings may be debt instruments, REITs, or foreign stocks not present in `de_instrument`. Allow `instrument_id = NULL` for unresolved ISINs. Log count of unresolved ISINs in `de_pipeline_log`.

**Morningstar datapoints to fetch:** Name, CategoryName, BroadCategoryGroup, NetExpenseRatio, ManagerName, TotalNetAssets, InceptionDate, Benchmark, Alpha, Beta, StandardDeviation, SharpeRatio, MaxDrawdown, ReturnM1, ReturnM3, ReturnM6, ReturnM12, ReturnM36, ReturnM60.

**Schedule:**
- Fund master refresh: weekly, Sunday (low-priority, non-SLA)
- Holdings refresh: monthly, 1st of month (non-SLA)
