# Chunk 14: API — Market Pulse + MF Pulse Endpoints

**Layer:** 5
**Dependencies:** C3, C11, C12
**Complexity:** High
**Status:** pending

## Files

- `app/api/v1/__init__.py`
- `app/api/v1/equity.py`
- `app/api/v1/mf.py`
- `app/api/v1/market.py`
- `app/api/v1/qualitative.py`
- `app/api/v1/admin.py`
- `app/api/v1/flows.py`
- `app/services/__init__.py`
- `app/services/symbol_resolver.py`
- `app/services/data_freshness.py`
- `tests/api/test_equity.py`
- `tests/api/test_mf.py`
- `tests/api/test_market.py`
- `tests/api/test_admin.py`

## Acceptance Criteria

### All Endpoints

- [ ] Symbol resolution (v1.7): any endpoint accepting `symbol` MUST resolve `symbol → instrument_id` via `SELECT instrument_id FROM de_instrument WHERE current_symbol = :symbol` BEFORE querying OHLCV/RS tables
- [ ] Data status gating (v1.9): all data-serving queries include `WHERE data_status = 'validated'`
- [ ] Response envelope: all responses use standard envelope (`data`, `meta`, `pagination`)
- [ ] Response headers on all data endpoints: `X-Data-Freshness`, `X-Computation-Version`, `X-System-Status`
- [ ] Redis caching with appropriate TTLs (see C3 notes for TTL per endpoint type)
- [ ] DB fallback: every Redis-cached endpoint works without Redis
- [ ] Pagination on all list endpoints: `?page=1&page_size=50`

### Auth Endpoints

- [ ] `POST /api/v1/auth/token` — issue JWT
- [ ] `POST /api/v1/auth/refresh` — refresh JWT with rotation

### Health

- [ ] `GET /health` — no auth, returns `{"status": "ok", "db": "ok", "redis": "ok"}`

### Equity Endpoints

- [ ] `GET /api/v1/equity/ohlcv/{symbol}?from=&to=` — OHLCV history with close_adj; Redis TTL 24h
- [ ] `GET /api/v1/equity/universe?active=true&sector=&cap_category=` — instrument list with filters; Redis TTL 24h
- [ ] `GET /api/v1/rs/stocks?sector=&min_rs=&limit=&vs_benchmark=` — RS leaderboard from `de_rs_daily_summary`; Redis TTL 1h
- [ ] `GET /api/v1/rs/sectors` — sector RS scores; Redis TTL 1h
- [ ] `GET /api/v1/rs/stock/{symbol}?from=&to=&vs_benchmark=` — single stock RS history; Redis TTL 1h

### MF Endpoints

- [ ] `GET /api/v1/mf/nav/{mstar_id}?from=&to=` — NAV history with returns; Redis TTL 24h
- [ ] `GET /api/v1/mf/universe?category=&min_rs=` — MF list with filters; Redis TTL 24h
- [ ] `GET /api/v1/mf/category-flows?from=&to=` — monthly MF category AUM/flows; Redis TTL 24h
- [ ] `GET /api/v1/mf/derived/{mstar_id}` — fund derived metrics (C12 output); Redis TTL 1h

### Market Endpoints

- [ ] `GET /api/v1/regime/current` — latest market regime; Redis TTL 1h
- [ ] `GET /api/v1/regime/history?from=&to=` — regime history; Redis TTL 24h
- [ ] `GET /api/v1/breadth/latest` — latest 25 breadth indicators; Redis TTL 1h
- [ ] `GET /api/v1/breadth/history?from=&to=` — breadth history; Redis TTL 24h
- [ ] `GET /api/v1/indices/list` — all NSE index codes; Redis TTL 24h
- [ ] `GET /api/v1/indices/{code}/history?from=&to=` — index price history; Redis TTL 24h
- [ ] `GET /api/v1/global/indices` — latest global index closes; Redis TTL 1h
- [ ] `GET /api/v1/global/macro` — latest macro values; Redis TTL 1h
- [ ] `GET /api/v1/flows/fii-dii?from=&to=` — FII/DII flow history; Redis TTL 24h
- [ ] `GET /api/v1/flows/fo-summary?from=&to=` — F&O summary history

### Qualitative Endpoints

- [ ] `POST /api/v1/qualitative/upload` — admin JWT only, 10/hour rate limit; triggers qualitative pipeline
- [ ] `GET /api/v1/qualitative/search?q=&limit=&asset_class=&direction=` — semantic search using pgvector cosine similarity; Redis TTL 24h per unique query
- [ ] `GET /api/v1/qualitative/recent?source=&limit=` — recent documents; Redis TTL 30min

### Admin Endpoints (admin JWT claim required)

- [ ] `GET /api/v1/admin/pipeline/status` — latest pipeline run status per pipeline name
- [ ] `GET /api/v1/admin/migration/report` — migration log summary
- [ ] `GET /api/v1/admin/anomalies?date=&resolved=&severity=` — data anomaly list with filters (v1.9)
- [ ] `POST /api/v1/admin/anomalies/{id}/resolve` — mark anomaly resolved with note (v1.9)
- [ ] `POST /api/v1/admin/data/override` — promote quarantined rows to validated (admin override) (v1.9)
- [ ] `POST /api/v1/admin/pipeline/replay` — re-run pipeline for specific date (idempotent) (v1.9)
- [ ] `POST /api/v1/admin/system/flag` — set/unset system flags (kill switch) (v1.9)

### Response Schema Contract (v1.9, Section 12.7)

- [ ] Every data endpoint wraps response in:
  ```json
  {
    "data": {...},
    "meta": {
      "timestamp": "2026-04-05T18:30:00+05:30",
      "computation_version": 1,
      "data_freshness": "fresh",
      "system_status": "normal"
    },
    "pagination": {
      "page": 1,
      "page_size": 50,
      "total": 1234,
      "has_next": true
    }
  }
  ```
- [ ] p95 latency target: <200ms on Redis-cached endpoints
- [ ] All list endpoints tested with correct pagination behaviour

## Notes

**Symbol resolution critical path (v1.7):** Always resolve `symbol → instrument_id` first. Never query `de_equity_ohlcv WHERE symbol = :symbol` — this bypasses partition pruning (partition key is `date`, not `symbol`) and causes full table scans on 10M+ row table.

**Admin JWT scope:** Admin endpoints require a JWT with `scope=admin` claim. Regular platform tokens (marketpulse, mfpulse) do not have admin scope. Issue admin tokens separately via the same `POST /auth/token` endpoint but with a different client_id that has admin privileges.

**Pipeline replay idempotency (v1.9):** Replay must: (a) be idempotent — ON CONFLICT handles re-inserts; (b) re-run validation (raw → validated/quarantined gating); (c) not duplicate `de_source_files` entries (ON CONFLICT). Use separate advisory lock namespace `replay:eod:{date}` to avoid conflicting with live pipelines.

**Data override (admin):** `POST /admin/data/override` promotes specific quarantined rows to validated after human review. Body: `{"table": "de_equity_ohlcv", "instrument_id": "...", "date": "2026-04-05", "reason": "confirmed stock split"}`. Log in `de_data_anomalies.resolution_note`.

**Base URL:** `http://localhost:8010/api/v1` (internal only). External: `https://core.jslwealth.in/api/v1`.

**API versioning:** Current version is v1 only. No v2 until explicitly planned. Breaking changes require new version prefix with v1 maintained for 6 months minimum.
