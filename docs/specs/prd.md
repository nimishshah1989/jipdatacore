# JIP Data Engine v2.0 — Product Requirements Document

**Status:** Approved for Build
**Date:** 2026-04-05
**Sprint Duration:** Full build (chunked for incremental delivery)
**Git:** github.com/nimishshah1989/jipdatacore.git
**Deploy Target:** EC2 (13.206.34.214) + RDS (existing)
**Source Spec:** JIP_Data_Engine_v2.0.md

---

## 1. Product Vision

Single backend data engine that replaces all per-product databases. Every JIP product reads from one API (core.jslwealth.in:8010). No product has its own database. All computation happens in the Data Engine.

## 2. Success Criteria

1. Market Pulse can switch from its current backend to Data Engine API with zero data loss
2. MF Pulse can be rebuilt entirely against Data Engine API
3. All 6 pipeline tracks run daily without manual intervention
4. Data freshness: equity OHLCV available by 19:30 IST, MF NAV by 22:30 IST
5. 30+ API endpoints respond <200ms p95 with Redis cache
6. Zero plaintext PII in any table
7. Full audit trail: every row traceable to source file + pipeline run

## 3. User Personas

| Persona | Product | Needs from Data Engine |
|---------|---------|----------------------|
| Equity Analyst | Market Pulse | OHLCV, RS scores, breadth, regime, sectors, technicals |
| MF Analyst | MF Pulse | NAV, returns, risk metrics, holdings, fund-vs-stock derived metrics |
| Trader | Champion Trader | Stage analysis, SMA/EMA, weekly/monthly indicators |
| CIO/Principal | All dashboards | Regime, breadth sentiment, FII/DII flows, global macro |
| Admin | Orchestrator | Pipeline status, anomalies, replay, system flags |

## 4. Scope — What We Build

### 4.1 Database Schema (40+ tables)
- Instrument masters (equity, MF, index, global, macro, contributors)
- Price data (OHLCV partitioned, MF NAV partitioned, index, global, macro)
- Flow data (institutional, MF category)
- Computed tables (technical daily, RS scores, RS summary, breadth, regime)
- F&O summary
- Qualitative layer (sources, documents, extracts, outcomes)
- Client portfolios (encrypted PII)
- Champion trades
- Pipeline state (source files, pipeline log, system flags, migration log, request log)
- Operational (trading calendar, recompute queue, data anomalies)

### 4.2 Ingestion Pipelines
- Daily EOD: 5 parallel tracks (equity, MF, indices, flows, F&O)
- Pre-market: global indices + macro
- T+1: delivery data
- Weekly: Morningstar fund master refresh
- Monthly: Morningstar holdings refresh
- Qualitative: RSS + manual upload + Playwright automation
- Reconciliation: cross-source validation

### 4.3 Computation Engine
- ~80 technical indicators per stock per day
- RS computation (5 timeframes + composite + percentile)
- 25 breadth indicators + sentiment score
- Market regime classification (BULL/BEAR/SIDEWAYS/RECOVERY)
- Sector aggregation (market-cap weighted)
- MF derived metrics (holdings × stock metrics)
- MF NAV-based risk metrics (sharpe, sortino, drawdown, etc.)

### 4.4 API Layer (30+ endpoints)
- Auth: JWT issue + refresh
- Equity: OHLCV, universe, RS (stocks, sectors, single stock)
- MF: NAV, universe, category flows
- Market: regime, breadth, indices, global, macro, flows, F&O
- Qualitative: upload, search, recent
- Admin: pipeline status, anomalies, replay, system flags, data override

### 4.5 Infrastructure
- Docker deployment on EC2
- Redis caching with circuit breaker + stampede protection
- PgBouncer connection pooling
- Prometheus + Grafana monitoring
- Slack alerting (#jip-alerts)
- Operational runbooks

### 4.6 Data Migrations
- Equity OHLCV from fie_v3 (1.4M rows, type corrections)
- MF NAV from fie2-db-1 Docker (25.8M → ~5M filtered)
- MF master from fie2-db-1 (13,380 funds)
- MF holdings from fie2-db-1 (2M+ rows)
- Index constituents from fie_v3 (4,638 rows)
- Client data from client_portal (366K rows, encrypt on migration)

## 5. Out of Scope (Phase 2)

- Real-time / intraday data
- RA license compliance layer
- Simulation and Optimization Laboratory
- IDCW/dividend MF plans (filter expansion)
- Synthetic continuous NAV for merged MF schemes

## 6. Non-Negotiable Rules

1. DATE columns store DATE type — never VARCHAR
2. Financial values: NUMERIC(18,4). Never FLOAT
3. Every table has created_at TIMESTAMPTZ DEFAULT NOW()
4. No triggers, no stored procedures
5. Every INSERT uses ON CONFLICT — full idempotency
6. No plaintext PII — all encrypted before insert

## 7. Build Order (Dependency-Driven)

```
LAYER 0: Foundation (scaffold, schema, pipeline framework)
    ↓
LAYER 1: Data Pipelines (equity + MF + indices + flows — parallel)
    ↓
LAYER 2: Computation Engine (technicals, RS, breadth, regime, sector, fund derived)
    ↓
LAYER 3: API Layer (auth, endpoints, caching, response envelope)
    ↓
LAYER 4: Advanced (qualitative, PII encryption, monitoring, orchestrator, runbooks)
    ↓
LAYER 5: Migrations (existing data → new schema)
    ↓
LAYER 6: QA + Operational Readiness (chaos testing, SLA verification, deploy)
```

Note: Layers 1-3 have internal parallelism. Layer 5 (migrations) can start as soon as schema exists.

## 8. Acceptance Criteria per Layer

### Layer 0: Foundation
- [ ] Project structure: FastAPI app, SQLAlchemy 2.0, Alembic, Docker, pytest
- [ ] All 40+ tables created via Alembic migration
- [ ] Pipeline guard (advisory locks) working
- [ ] Trading calendar seeded
- [ ] System flags seeded

### Layer 1: Data Pipelines
- [ ] BHAV copy ingestion (3 format parsers) runs end-to-end
- [ ] AMFI NAV ingestion runs end-to-end
- [ ] Post-ingestion validation detects anomalies
- [ ] Data status gating (raw → validated → quarantined) working
- [ ] Source file lineage tracked for every ingested file

### Layer 2: Computation
- [ ] 80 technical indicators computed for all active stocks
- [ ] RS scores match formula: rs_Nt = (entity_cumreturn_N - benchmark_cumreturn_N) / benchmark_rolling_std_N
- [ ] Breadth metrics produce 25 indicators
- [ ] Market regime classification runs daily

### Layer 3: API
- [ ] JWT auth with refresh token rotation
- [ ] All 30+ endpoints return correct data
- [ ] Redis caching with circuit breaker
- [ ] X-Data-Freshness / X-Computation-Version / X-System-Status headers
- [ ] Response envelope with pagination

### Layer 4: Advanced
- [ ] Qualitative pipeline processes PDFs and text
- [ ] PII encryption + blind index search working
- [ ] Prometheus metrics exposed
- [ ] Slack alerts firing on pipeline failures

### Layer 5: Migrations
- [ ] 1.4M equity rows migrated with type corrections
- [ ] ~5M MF NAV rows migrated (filtered from 25.8M)
- [ ] 13,380 fund master records migrated
- [ ] Validation gates pass (row count, date range, spot check)

### Layer 6: QA
- [ ] All chaos tests pass (Section 12.6 of spec)
- [ ] SLA enforcement verified
- [ ] API p95 < 200ms on cached endpoints

## 9. Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Morningstar API details missing | Holdings refresh blocked | Stub endpoint, wire when URL provided |
| RDS PostgreSQL < 12 | GENERATED ALWAYS fails | Check version first; upgrade if needed |
| EC2 disk space | Build stalls | docker system prune before start |
| NSE BHAV format changes | Parser breaks | Auto-detect by header, 3 parsers ready |
| Migration data quality | Bad data in prod | Validation gates + anomaly detection |

## 10. Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Framework | FastAPI + Uvicorn |
| ORM | SQLAlchemy 2.0 async (mapped_column) |
| Migrations | Alembic |
| Database | PostgreSQL 12+ on RDS |
| Cache | Redis 7+ |
| Pooling | PgBouncer |
| Auth | PyJWT + bcrypt |
| HTTP Client | httpx (async) |
| Data | pandas, numpy |
| Monitoring | prometheus-fastapi-instrumentator |
| Testing | pytest + pytest-asyncio + httpx |
| Logging | structlog |
| Deploy | Docker + docker-compose |
| CI/CD | GitHub Actions |
