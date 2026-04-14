# Session Log — JIP Data Engine v2.0

## 2026-04-14 — IND-C1: Indicators overhaul — dependencies & package scaffold

**Build**: indicators-overhaul (12 chunks)
**Files modified**: `pyproject.toml` (+2 deps pinned exact), `app/computation/indicators_v2/__init__.py` (new), `app/computation/indicators_v2/assets/__init__.py` (new)

**Key decisions**:
- pandas-ta-classic pinned exact to `==0.4.47` (Fix 11 from eng review — drift detection via golden tests, not free-form upgrades)
- empyrical-reloaded pinned exact to `==0.5.12`
- Discovery: module name is `pandas_ta_classic`, NOT `pandas_ta` — chunk 3 spec needs this noted before implementation
- Discovery: project has no local venv and no lockfile; all installs via `docker build .`. Verification path for this build = docker build + pytest inside image.

**Verification evidence** (Docker image `jip-data-engine:ind-c1`):
- Build succeeded (exit 0, ~130s)
- Import smoke test: `pandas_ta_classic 0.4.47`, `empyrical 0.5.12`, `app.computation.indicators_v2.assets` all importable
- Functional smoke: RSI(14) computes correct values on synthetic OHLCV, empyrical.sharpe_ratio and max_drawdown work on random returns
- Existing suite: `tests/computation/` = 428 passed, 3 failed. All 3 failures are pre-existing (see `reports/stale-tests-indicators-build.md`) — grep confirms no existing code imports the new deps, so my changes cannot have caused them. Filed as P2/P3 follow-ups.

**Pre-flight finding** (not a chunk, but did the work):
- `reports/morningstar_purchase_mode_investigation.md` — `app/pipelines/morningstar/fund_master.py` does NOT fetch `PurchaseMode` today. But the client uses a generic datapoint API so adding it is a one-line change. Chunk 9 stays single (no 9a/9b split).

**Pre-existing dep drift observed** (flagged, not fixed):
- Fresh build pulled `pandas 3.0.2`, `numpy 2.4.4`, `sqlalchemy 2.0.49` — major bumps from the `>=` constraints. This is not my change but should be addressed in a lockfile project. For now: acknowledged, not blocking.

**Next**: IND-C2 (Alembic migrations for 5 new technical tables + `de_mf_master.purchase_mode`)

---

## 2026-04-05 — C1-C3: Foundation (scaffold + schema + auth)

**Chunks:** C1 (Project Scaffold), C2 (Database Schema), C3 (API Auth + Middleware)
**Commit:** `b0865cc`
**Files:** 63 files, 11,274 LOC
**Key decisions:**
- Single Alembic migration for all 40+ tables (monolith migration for initial schema)
- Partitioned tables for OHLCV (2000-2035) and MF NAV (2006-2035)
- JWT with refresh token rotation, Redis caching with circuit breaker
- Rate limiting: 1000 req/min per platform
- structlog JSON logging in production, console in dev
**Tests:** test_health.py, test_auth.py, test_models.py
**Bugs:** None
**Review:** Not run (pre-forge activation)

---

## 2026-04-05 — C4: Pipeline Framework

**Chunk:** C4 (Pipeline Framework)
**Commit:** `adb12d6`
**Files:** 12 files, 1,976 LOC
**Key decisions:**
- BasePipeline ABC with 8-step orchestration flow
- Advisory locks via `pg_try_advisory_lock(hashtext())` — non-blocking, skip on contention
- `is_trading_day` fails open (missing = trading day) to prevent silent data gaps
- Quarantine threshold: >5% strict greater-than (exactly 5% does not halt)
- `apply_data_status` uses parameterized `sa.text()` — table name is trusted internal arg
**Tests:** 55 passing (framework, guards, calendar, system_flags, validation)
**Bugs:** None
**Review:** Not run (pre-forge activation)

---

## 2026-04-05 — C5: Data Migrations

**Chunk:** C5 (Data Migrations)
**Commit:** `c26e4e1`
**Files:** 13 files, 2,345 LOC
**Key decisions:**
- BaseMigration ABC with batched read/transform/insert pattern
- SymbolResolver + SchemeCodeResolver with two-layer cache (warm + on-miss)
- Date parsing supports 4 formats with datetime/date passthrough
- All financial values: Decimal, never float. `_to_decimal()` raises on bad input
- Runner CLI enforces dependency order (mf_master before mf_nav/mf_holdings)
- Auto-converts `postgresql://` to `postgresql+asyncpg://` for source URLs
**Tests:** 47 passing (base, equity_ohlcv, symbol_resolver, mf_nav)
**Bugs:** None
**Review:** Not run (pre-forge activation)

---

## 2026-04-05 — C6: PII Encryption

**Chunk:** C6 (PII Encryption)
**Commit:** `0217296`
**Files:** 8 files, 1,337 LOC
**Key decisions:**
- AES-256-GCM with 12-byte random nonce per field encryption
- DEK: 32 bytes from os.urandom, encrypted with master key (also AES-256-GCM)
- Local master key: PBKDF2(jwt_secret, salt="jip-data-engine-pii", 100k iterations)
- HMAC key derived with separate salt ("jip-data-engine-hmac")
- Adapted to actual schema: email_enc/phone_enc/pan_enc (not encrypted_name etc.)
- Append-only key rotation in de_client_keys
**Tests:** 39 passing (encryption, hmac_index, pii_service)
**Bugs:** None
**Review:** Not run (pre-forge activation)
