# JIP Data Engine v2.0 — Chunk Plan

**Total Chunks:** 16
**Parallel Lanes:** Up to 4 concurrent agents after foundation
**Estimated Build Order:** 6 layers, dependency-driven

---

## Dependency Graph

```
LAYER 0 (sequential):
  C1: Project Scaffold
    ↓
LAYER 1 (parallel — 2 agents):
  C2: Database Schema (all Alembic migrations)
  C3: API Auth + Middleware + Response Envelope
    ↓
LAYER 2 (parallel — 4 agents, after C2):
  C4: Pipeline Framework (guards, logging, gating)
  C5: Data Migrations (existing DBs → new schema)
  C6: PII Encryption (envelope encryption, HMAC)
  C3 continues...
    ↓
LAYER 3 (parallel — 4 agents, after C4):
  C7: Equity Ingestion Pipeline (BHAV, corporate actions)
  C8: MF Ingestion Pipeline (AMFI NAV, MF master)
  C9: Supporting Pipelines (indices, flows, F&O, global, macro)
  C10: Morningstar Integration (10 APIs)
    ↓
LAYER 4 (parallel — 3 agents, after C7/C8):
  C11: Technical Indicators + RS + Breadth + Regime
  C12: Sector + Fund Derived Metrics
  C13: Qualitative Pipeline (RSS, upload, Claude API, Playwright)
    ↓
LAYER 5 (parallel — 3 agents, after C11/C12):
  C14: API - Market Pulse + MF Pulse Endpoints
  C15: Pipeline Monitoring Dashboard (frontend)
  C16: Orchestrator + Monitoring + Operational Readiness
```

---

## Chunk Details

### C1: Project Scaffold
**Files:** Project root, app/, tests/, docker/, alembic/
**Complexity:** Medium
**Dependencies:** None
**Acceptance Criteria:**
- FastAPI app boots with /health endpoint
- SQLAlchemy 2.0 async engine configured
- Alembic initialized with async support
- Docker + docker-compose working
- pytest + pytest-asyncio configured
- structlog configured
- .env.example with all required vars
- pyproject.toml with all dependencies
- GitHub Actions CI (lint + type check + test)
- CLAUDE.md for project conventions

### C2: Database Schema
**Files:** alembic/versions/*.py (migrations for all 40+ tables)
**Complexity:** High (largest chunk by table count)
**Dependencies:** C1
**Acceptance Criteria:**
- All tables from spec Section 3 created via Alembic
- Partitioned tables (OHLCV 2000-2035, MF NAV 2006-2035)
- All indexes, constraints, CHECK constraints
- Partition creation functions
- Seed data (system flags, contributors)
- `alembic upgrade head` runs clean

### C3: API Auth + Middleware + Response Envelope
**Files:** app/api/auth.py, app/middleware/, app/api/deps.py
**Complexity:** Medium
**Dependencies:** C1
**Acceptance Criteria:**
- JWT issue + refresh token rotation
- Bearer token validation middleware
- Rate limiting (1000 req/min per platform)
- Request logging to de_request_log
- Response envelope with meta + pagination
- X-Data-Freshness, X-Computation-Version, X-System-Status headers
- Redis caching layer with circuit breaker + stampede protection
- DB fallback when Redis is down
- CORS disabled

### C4: Pipeline Framework
**Files:** app/pipelines/framework.py, app/pipelines/guards.py, app/pipelines/validation.py
**Complexity:** High
**Dependencies:** C2
**Acceptance Criteria:**
- Pipeline guard with session-level advisory locks (hashtext)
- Pipeline logging to de_pipeline_log
- Source file registration (de_source_files)
- System flags check (kill switch)
- Trading calendar check
- Data status gating (raw → validated → quarantined)
- Post-ingestion anomaly detection framework
- Quarantine threshold guardrail (>5% = halt aggregates)
- Freshness validation (checksum, rowcount, date)

### C5: Data Migrations
**Files:** app/migrations/
**Complexity:** High
**Dependencies:** C2
**Acceptance Criteria:**
- Equity OHLCV from fie_v3 compass_stock_prices (1.4M rows, VARCHAR→DATE, DOUBLE→NUMERIC)
- MF NAV from fie2-db-1 nav_daily (25.8M → ~5M filtered equity/growth/regular)
- MF master from fie2-db-1 fund_master (13,380 funds)
- MF holdings from fie2-db-1 fund_holding_detail (2M+ rows)
- Index constituents from fie_v3 (4,638 rows)
- Migration logging (de_migration_log, de_migration_errors)
- Validation gates: row count, date range, spot check, FK resolution

### C6: PII Encryption
**Files:** app/security/encryption.py, app/security/hmac_index.py
**Complexity:** Medium
**Dependencies:** C2
**Acceptance Criteria:**
- Envelope encryption: KMS CMK → per-client DEK → AES-256-GCM
- HMAC blind index computation (truncated to 8 chars)
- Bucket search (query by truncated hash, decrypt small set)
- Key rotation (append-only de_client_keys)
- PII access logging
- Client data migration with encryption

### C7: Equity Ingestion Pipeline
**Files:** app/pipelines/equity/
**Complexity:** High
**Dependencies:** C4
**Acceptance Criteria:**
- BHAV copy downloader with 3 format parsers (pre-2010, standard, UDiFF)
- Format auto-detection by header row
- INSERT into de_equity_ohlcv ON CONFLICT DO UPDATE
- Symbol enforcement (current_symbol at trade date)
- Corporate actions ingestion from NSE API
- Adjustment factor computation
- Recompute queue integration
- Post-ingestion validation (price spike, volume spike, negative values)
- NSE master refresh (Step 0): new listings, symbol changes, delistings

### C8: MF Ingestion Pipeline
**Files:** app/pipelines/mf/
**Complexity:** Medium
**Dependencies:** C4
**Acceptance Criteria:**
- AMFI NAV file downloader + parser
- INSERT into de_mf_nav_daily ON CONFLICT DO UPDATE
- MF lifecycle management (mergers, closures)
- Post-ingestion validation (NAV spike, zero NAV)
- Return computation (1d through 10y from NAV series)
- MF dividend handling for IDCW plans (de_mf_dividends)

### C9: Supporting Pipelines
**Files:** app/pipelines/indices/, app/pipelines/flows/, app/pipelines/global/
**Complexity:** Medium
**Dependencies:** C4
**Acceptance Criteria:**
- NSE index prices ingestion (all 60+ indices)
- India VIX to de_macro_values
- FII/DII flows (primary NSE, fallback SEBI on 403)
- F&O summary (PCR, OI, max pain)
- Pre-market global indices + macro (yfinance + FRED)
- T+1 delivery data pipeline
- Trading calendar management

### C10: Morningstar Integration
**Files:** app/pipelines/morningstar/
**Complexity:** Medium
**Dependencies:** C4, C8
**Acceptance Criteria:**
- Client for all 10 Morningstar API endpoints
- Fund master refresh (weekly) — identifier + category data
- Holdings refresh (monthly) — holdings detail + portfolio summary
- Risk data fetch — risk statistics
- ISIN → instrument_id resolution for holdings
- Rate limiting and error handling
- Credential management via env/Secrets Manager

### C11: Technical Indicators + RS + Breadth + Regime
**Files:** app/computation/
**Complexity:** Very High (core computation engine)
**Dependencies:** C7
**Acceptance Criteria:**
- All ~80 technical indicators per stock per day (de_equity_technical_daily)
- Moving averages (EMA 10/21/50/200, SMA 50/200)
- RSI, MACD, ROC at multiple timeframes
- Weekly and monthly indicators
- Volatility, beta, sharpe, sortino, drawdown
- Volume signals (relative volume, OBV, MFI, delivery analysis)
- RS computation (5 timeframes + composite + percentile)
- RS daily summary
- 25 breadth indicators + sentiment score
- Market regime classification (BULL/BEAR/SIDEWAYS/RECOVERY)
- Incremental daily + full weekly rebuild

### C12: Sector + Fund Derived Metrics
**Files:** app/computation/sectors.py, app/computation/fund_derived.py
**Complexity:** High
**Dependencies:** C8, C11
**Acceptance Criteria:**
- Sector aggregation (market-cap weighted from constituent stocks)
- Sector RS, momentum, volatility, breadth
- MF derived metrics from holdings × stock metrics
- Holdings coverage tracking
- Manager alpha signal (NAV RS vs derived RS)
- MF NAV-based risk metrics (sharpe, sortino, max_drawdown, etc.)

### C13: Qualitative Pipeline
**Files:** app/pipelines/qualitative/
**Complexity:** High
**Dependencies:** C4
**Acceptance Criteria:**
- RSS feed polling for new items
- File upload security gate (magic bytes + ClamAV)
- Content extraction (PDF via PyMuPDF, audio via Whisper, text, URL)
- Claude API structured extraction
- OpenAI embedding generation
- Semantic deduplication (cosine similarity > 0.92)
- Cost guardrails (200 docs/day, per-source rate limits)
- S3 archival after processing
- Playwright automation for Goldilocks Research

### C14: API - Market Pulse + MF Pulse Endpoints
**Files:** app/api/v1/
**Complexity:** High
**Dependencies:** C3, C11, C12
**Acceptance Criteria:**
- All 30+ endpoints from spec Section 8
- Equity: OHLCV, universe, RS (stocks, sectors, single)
- MF: NAV, universe, category flows, derived metrics
- Market: regime, breadth, indices, global, macro, flows, F&O
- Qualitative: search (semantic), recent
- Admin: pipeline status, anomalies, replay, system flags, data override
- Symbol resolution (symbol → instrument_id before query)
- Data status gating (WHERE data_status = 'validated')
- Pagination on all list endpoints
- Redis caching with appropriate TTLs per endpoint

### C15: Pipeline Monitoring Dashboard
**Files:** dashboard/
**Complexity:** Medium
**Dependencies:** C3, C4
**Acceptance Criteria:**
- Live pipeline status (running/complete/failed per track)
- Data ingestion progress (rows processed, time elapsed)
- Today's anomalies by severity (unresolved highlighted)
- System health (Redis, DB connections, disk)
- SLA tracking (met/missed deadlines)
- Historical pipeline run viewer
- Auto-refresh every 30 seconds
- Professional wealth management aesthetic (teal accents, data-dense)

### C16: Orchestrator + Monitoring + Operational Readiness
**Files:** app/orchestrator/, runbooks/, prometheus/
**Complexity:** High
**Dependencies:** C7, C8, C9, C11
**Acceptance Criteria:**
- Central orchestrator with DAG execution
- Cron scheduling for all pipelines
- SLA enforcement with Slack alerting
- Retry policies (transient vs persistent failures)
- Prometheus metrics (prometheus-fastapi-instrumentator)
- Reconciliation pipeline (cross-source validation)
- Runbooks for all failure scenarios (8 runbooks)
- Cost controls (API call caps, recompute throttling)
- VACUUM strategy for high-churn tables
- Docker deployment configuration for EC2

---

## Build Order Summary

| Phase | Chunks | Parallel Agents | Blocking |
|-------|--------|-----------------|----------|
| 0 | C1 | 1 | Everything |
| 1 | C2, C3 | 2 | C4-C16 need C2; C14-C15 need C3 |
| 2 | C4, C5, C6 | 3 | C7-C13 need C4 |
| 3 | C7, C8, C9, C10 | 4 | C11-C12 need C7/C8 |
| 4 | C11, C12, C13 | 3 | C14 needs C11/C12 |
| 5 | C14, C15, C16 | 3 | Final layer |
