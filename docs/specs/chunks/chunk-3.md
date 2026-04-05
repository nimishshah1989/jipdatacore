# Chunk 3: API Auth + Middleware + Response Envelope

**Layer:** 1
**Dependencies:** C1
**Complexity:** Medium
**Status:** done

## Files

- `app/api/__init__.py`
- `app/api/auth.py`
- `app/api/deps.py`
- `app/middleware/__init__.py`
- `app/middleware/auth.py`
- `app/middleware/logging.py`
- `app/middleware/rate_limit.py`
- `app/middleware/cache.py`
- `app/schemas/__init__.py`
- `app/schemas/common.py`
- `tests/api/test_auth.py`
- `tests/middleware/test_rate_limit.py`
- `tests/middleware/test_cache.py`

## Acceptance Criteria

- [ ] `POST /api/v1/auth/token` accepts `{"client_id": "marketpulse", "secret": "<platform-secret>"}` and returns `{"access_token": "...", "refresh_token": "...", "expires_in": 86400}`
- [ ] `POST /api/v1/auth/refresh` accepts `{"refresh_token": "..."}` and returns a new access token; old refresh token is invalidated (rotation)
- [ ] JWT access tokens expire in 24 hours; signed with `JWT_SECRET` loaded from AWS Secrets Manager at startup
- [ ] Refresh tokens stored as bcrypt hash in Redis; revoked on logout or password change
- [ ] Bearer token validation middleware rejects requests with missing/expired/invalid JWT with `401`
- [ ] Rate limiting: 1000 req/minute per platform token; returns `429` when exceeded
- [ ] Every request logged to `de_request_log`: actor (JWT subject), source_ip, method, endpoint, status_code, duration_ms, requested_at
- [ ] Response envelope schema applied to all data endpoints:
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
- [ ] Response headers on all data endpoints: `X-Data-Freshness: fresh|stale|partial`, `X-Computation-Version: 1`, `X-System-Status: normal|degraded`
- [ ] Redis caching layer implemented with `redis_get_safe()` using circuit breaker pattern
- [ ] Circuit breaker: after 3 consecutive Redis failures, bypass Redis for 60 seconds; state is per-worker process (not stored in Redis)
- [ ] Cache stampede protection: `setnx` lock prevents thundering herd on cache miss
- [ ] DB fallback: every endpoint that reads from Redis has a working DB fallback path
- [ ] CORS disabled (internal API only)
- [ ] `/health` endpoint excluded from auth middleware and rate limiting
- [ ] Admin endpoints (`/admin/*`) require separate admin-scoped JWT claim

## Notes

**JWT flow:**
- `JWT_SECRET` stored in AWS Secrets Manager, loaded at FastAPI startup via `boto3`
- Never in `.env` or code
- Refresh tokens: stored as `bcrypt` hash in Redis (`refresh:{token_hash}` → `client_id`)
- Token revocation: `DELETE` key from Redis

**Rate limiting implementation:** Use Redis counter with 60-second sliding window. Key pattern: `ratelimit:{client_id}:{window_minute}`. Return `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers.

**X-Data-Freshness logic:**
- `fresh` = all pipeline tracks completed for today (check `de_pipeline_log.status = 'complete'` for today)
- `stale` = data is from yesterday or older
- `partial` = some tracks failed (`de_pipeline_log.track_status` shows at least one track as `failed`)

**X-System-Status logic:**
- `normal` = all `de_system_flags.value = TRUE`
- `degraded` = any flag is `FALSE`

**Redis circuit breaker (v1.9.1):** State is per-Uvicorn worker, not shared. With 4 workers, a dead Redis gets up to 12 attempts (3 per worker) before all circuits open. Do NOT put circuit state in Redis (circular dependency).

**Cache TTLs by endpoint type:**
- Regime current: 1 hour
- RS scores: 1 hour
- Breadth latest: 1 hour
- Global indices/macro: 1 hour
- OHLCV history: 24 hours
- MF universe: 24 hours
- Index history: 24 hours
- Qualitative search: 24 hours per unique query
- Qualitative recent: 30 minutes

**Pagination:** All list endpoints support `?page=1&page_size=50`. Default page_size=50, max=500.
