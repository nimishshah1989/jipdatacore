# Chunk 7: Equity Ingestion Pipeline

**Layer:** 3
**Dependencies:** C4
**Complexity:** High
**Status:** pending

## Files

- `app/pipelines/equity/__init__.py`
- `app/pipelines/equity/bhav.py`
- `app/pipelines/equity/master_refresh.py`
- `app/pipelines/equity/corporate_actions.py`
- `app/pipelines/equity/delivery.py`
- `app/pipelines/equity/eod.py`
- `tests/pipelines/equity/test_bhav.py`
- `tests/pipelines/equity/test_master_refresh.py`
- `tests/pipelines/equity/test_corporate_actions.py`

## Acceptance Criteria

- [ ] **Format auto-detection:** BHAV copy format detected by header row; routes to correct parser
  - Pre-2010: `eq_DDMMYYYY_csv.zip` (legacy format)
  - 2010–June 2024: `sec_bhavdata_full_DDMMYYYY.csv` (standard format)
  - July 2024+: UDiFF (Unified Distilled File Format) — different column names, delimiter
- [ ] **BHAV download:** Downloads NSE BHAV copy for given business date with retry on failure (3 retries, exponential backoff)
- [ ] **Freshness validation:** Row count >= 500; source date matches expected business date; SHA-256 checksum computed; duplicate file detected via `de_source_files` checksum check
- [ ] **BHAV ingestion:** `INSERT INTO de_equity_ohlcv ON CONFLICT (date, instrument_id) DO UPDATE SET close=EXCLUDED.close, ...`
- [ ] **Symbol enforcement (v1.7):** On insert, `symbol` must equal `de_instrument.current_symbol` as-of trade date; instruments not found in `de_instrument` are skipped with anomaly logged
- [ ] **Post-insert validation (v1.8):**
  - Price spike: `abs(close - prev_close) / prev_close > 0.20` AND no corporate action today → `anomaly_type='price_spike', severity='warning'`
  - Volume spike: `volume > 10 × rolling_avg_volume_20d` → `anomaly_type='volume_spike', severity='info'`
  - Negative values: any of open/high/low/close < 0 → `anomaly_type='negative_value', severity='critical'`
  - Price range: `high < low` → `anomaly_type='negative_value', severity='critical'`
- [ ] **Data status gating:** Batch-promote `raw → validated` (no critical anomalies) or `raw → quarantined` (critical anomaly) after validation
- [ ] **Master refresh (Step 0, v1.7):** Fetch NSE equity listing file daily before price ingestion; INSERT new instruments into `de_instrument ON CONFLICT DO NOTHING`; handle symbol changes (UPDATE `current_symbol`, INSERT `de_symbol_history`); handle suspensions/delistings
- [ ] **Corporate actions (Step 0.5, v1.7):** Fetch NSE `corporateActions` API; `INSERT INTO de_corporate_actions ON CONFLICT DO UPDATE`; validate split ratios (1:2 to 1:10 range; >100x ratio flagged as anomaly)
- [ ] **Adjustment factor computation:** `adj_factor = ratio_from / ratio_to` for splits/bonuses. Dividend: `adj_factor = (close_before_ex - dividend) / close_before_ex`
- [ ] **Recompute queue integration (v1.8):**
  - Same-day actions (priority=1): inline recomputation immediately
  - Historical corrections (priority=5): enqueue in `de_recompute_queue` for background worker
  - Dedup: `ON CONFLICT (instrument_id) WHERE status='pending' DO NOTHING`
- [ ] **T+1 delivery pipeline:** Downloads NSE delivery data for last trading day; `UPDATE de_equity_ohlcv SET delivery_vol, delivery_pct WHERE date = last_trading_day`
- [ ] **Incremental technical update (Step 9.8):** After OHLCV insert, update `de_equity_technical_daily` for today using incremental SMA/EMA calculation
- [ ] All tests use mocked NSE HTTP responses — no live network calls in tests

## Notes

**BHAV copy URLs:**
- Pre-2010: `https://archives.nseindia.com/content/historical/EQUITIES/{YYYY}/{MMM}/eq_{DD}{MM}{YYYY}_csv.zip`
- Standard: `https://archives.nseindia.com/products/content/sec_bhavdata_full_{DD}{MM}{YYYY}.csv`
- UDiFF: URL pattern TBD — confirm by downloading a sample file from NSE for July 2024+ date. Parser must auto-detect by inspecting header row.

**Symbol contract (v1.7):** The `symbol` column in `de_equity_ohlcv` is an IMMUTABLE HISTORICAL SNAPSHOT — it records the symbol as-of trade date. It is NEVER updated retroactively even if the symbol later changes. This is critical for audit trail.

**Partition pruning:** API must resolve `symbol → instrument_id` before querying OHLCV. Direct `WHERE symbol = ...` bypasses partition pruning (partition key is `date`, not `symbol`).

**Adjustment factor formula (v1.9.1):**
- Stock split 1:10 (1 old → 10 new): `adj_factor = 1/10 = 0.1` (prices go down to match current)
- Reverse split 10:1 (10 old → 1 new): `adj_factor = 10/1 = 10.0` (prices go up)
- Bonus 1:1 (1 bonus for 1 held): `ratio_from=1, ratio_to=2, adj_factor = 0.5`
- `adjusted_price = raw_price × cumulative_factor`
- `adjusted_volume = raw_volume / cumulative_factor`

**Recompute queue constraints (v1.9):**
- Max 2 concurrent recomputes (worker pool size)
- Max 50,000 OHLCV rows per batch
- Schedule: every 15 min during 22:00–06:00 IST; on-demand during market hours for priority=1

**Corporate actions NSE API:** `GET https://www.nseindia.com/api/corporateActions?index=equities&from_date={DD-MM-YYYY}&to_date={DD-MM-YYYY}`. May require session cookie. Use `httpx` with session management.

**EOD pipeline trigger:** 18:30 IST. Equity SLA: must complete by 19:30 IST. Data available by 19:30 IST is the non-negotiable business requirement.
