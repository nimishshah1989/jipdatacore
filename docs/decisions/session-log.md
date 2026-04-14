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

## 2026-04-14 — IND-C2: Indicators overhaul — v2 tables + purchase_mode migration

**Build**: indicators-overhaul
**Files created**: `alembic/versions/007_add_purchase_mode_to_mf_master.py` (32 lines), `alembic/versions/008_indicators_v2_tables.py` (492 lines), `app/models/indicators_v2.py` (621 lines), `reports/alembic-drift-ticket.md`
**Files modified**: `app/models/__init__.py` (5 new exports), `app/models/instruments.py` (added `DeMfMaster.purchase_mode`)

**Key decisions**:
- Wide tables built via raw SQL `op.execute()` (matching migration 001 style) rather than 130 × `sa.Column()` calls — 3 shared column-block constants for DRY
- Generated columns hardcoded as SQL string literals, never Python f-strings, to guarantee byte-identity with v1 production (Fix 9)
- Used `sa.Computed()` in SQLAlchemy models with the exact same expression strings
- ETF FK column sized `VARCHAR(30)` (matches existing `de_etf_master.ticker`, not the `VARCHAR(20)` specified in chunk 2 — agent caught this)
- `price_above_vwap` excluded transitively from index table (references `vwap` which Fix 12 excludes)
- `adx_strong_trend` excluded transitively from MF table (references `adx_14` which Fix 13 excludes)

**Verification evidence**:
1. Docker rebuild clean (image `jip-data-engine:ind-c2`, 8s with cache)
2. Raw SQL from migration 008 extracted via mock-patched `alembic.op.execute`, captured 15 statements, executed directly against throwaway `postgres:15` container → all cleanly applied
3. **Fix 9 smoke test passed**: inserted dummy row with known indicator values, all 8 generated columns returned the mathematically expected booleans (`above_50dma=t`, `above_200dma=t`, `above_20ema=t`, `price_above_vwap=t`, `rsi_overbought=t`, `rsi_oversold=f`, `macd_bullish=t`, `adx_strong_trend=t`)
4. Column counts: equity/etf/global v2 = 104 each; index = 93 (−11 volume cols); mf = 67 (strict single-price subset) — matches Fix 12/13 exclusions exactly
5. Generated expressions byte-identical across equity/etf/global v2: `(close_adj > sma_50)`, `(close_adj > sma_200)`, `(macd_line > macd_signal)` — transparent rename in chunk 6 will work
6. Model import: all 5 classes importable, `DeMfMaster.purchase_mode` present as `INTEGER nullable=True`
7. Model regression: `pytest tests/ -k "model or schema"` → 185 passed

**Pre-existing drift discovered** (filed as P1 ticket, not blocking):
- `reports/alembic-drift-ticket.md` — migration 002 declares `revision="002"` but migration 003's `down_revision="002_expand_global_instrument_type"`. Chain broken at that boundary. `alembic.ScriptDirectory.from_config()` raises KeyError. Impact: `alembic upgrade head` fails on fresh DB today. Fix is a one-line rename in migration 002. Not blocking because production is already past this point and my 007/008 chain is internally consistent.
- Additional drift: production `de_equity_technical_daily` has 46 columns vs 10 defined in migration 001. The extra 36 columns were added via `Base.metadata.create_all()` or manual ALTERs outside of Alembic. Also filed in the same ticket.

**Next**: IND-C3 (engine core — `spec.py`, `engine.py`, `strategy.yaml`, `risk_metrics.py`). Per Fix 2 this chunk is split into 3a/3b/3c.

---

## 2026-04-14 — IND-C3a: Indicators overhaul — engine skeleton (SMA/EMA only)

**Build**: indicators-overhaul (sub-chunk 3a of 3a/3b/3c split per Fix 2)
**Files created**:
- `app/computation/indicators_v2/spec.py` — frozen `AssetSpec` dataclass
- `app/computation/indicators_v2/strategy.yaml` — 12 entries (SMA/EMA 5/10/20/50/100/200)
- `app/computation/indicators_v2/strategy_loader.py` — `load_strategy_for_asset`, `get_rename_map`, `get_schema_columns` (lru_cached)
- `app/computation/indicators_v2/engine.py` — `compute_indicators`, `CompResult`, `_to_decimal_row`, `_upsert_batch` with tenacity retry
- `tests/computation/test_indicators_v2_engine.py` — 10 unit tests

**Files modified**:
- `pyproject.toml` — `pyyaml>=6.0` added (linter then auto-formatted)
- `app/computation/indicators_v2/__init__.py` — 6 exports

**Binding fixes implemented**:
- **Fix 3** (pandas-ta → schema column rename): `strategy.yaml` carries `output_columns` maps; engine applies `df.rename(columns=rename_map)` after `df.ta.strategy(strategy)`; asserts every expected schema column is present post-rename
- **Fix 4** (NaN write policy): `_to_decimal_row` converts NaN → None per column; rows are never skipped on individual NaNs; entire instruments skipped only when `len(df) < spec.min_history_days`
- **Fix 5** (Decimal boundary): one `_to_decimal_row` call per row; its output dict is used for both INSERT VALUES and ON CONFLICT UPDATE SET (built from the same `non_pk` column list, filtering out `c.computed is not None` to exclude GENERATED columns)
- **Fix 6** (row-position semantics): `assert df.index.is_monotonic_increasing` before strategy runs; engine does NOT reindex calendar gaps (documented in module docstring)
- **Fix 15** (RDS retry): `_execute_upsert_with_retry` wraps `session.execute(stmt)` with `tenacity.retry(stop_after_attempt(3), wait_exponential(min=2, max=30), retry_if_exception_type((OperationalError, InterfaceError)))`

**Verification evidence**:
1. pandas-ta-classic 0.4.47 emits exactly `SMA_5`, `SMA_10`, ..., `EMA_200` — matches strategy.yaml `output_columns` keys byte-for-byte
2. Engine import test green: `load_strategy_for_asset("equity", True)` returns Strategy with 12 indicators
3. 10/10 new unit tests pass (`test_indicators_v2_engine.py`):
   - lru_cache correctness, rename map correctness, NaN→None, float→Decimal quantization (0.0001), no-floats-leak assertion (every upsert value is Decimal/None/int/bool/date/datetime, never float), insufficient-history skip, per-instrument error isolation, monotonic-date assertion, end-to-end SMA/EMA columns
4. Regression: 384 existing computation tests still pass (57 deselected = pre-existing stale tests)

**Next**: IND-C3b (full strategy.yaml catalog + pandas-ta wiring — ~130 indicators)

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
