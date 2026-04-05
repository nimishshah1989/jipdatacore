# Session Log — JIP Data Engine v2.0

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
