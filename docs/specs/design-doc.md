# JIP Data Engine v2.0 — Design Document

**Status:** Approved for Build
**Date:** 2026-04-05
**Author:** Nimish Shah + Claude (Forge OS)
**Source Spec:** JIP_Data_Engine_v2.0.md (2,170 lines, 8 review rounds, 55 issues resolved)

---

## 1. What Is This?

The Data Engine is the **single source of truth** for all financial data in the Jhaveri Intelligence Platform (JIP). One PostgreSQL database on AWS RDS, one FastAPI service (port 8010, internal only). All downstream products (Market Pulse, MF Pulse, Champion Trader, Global Pulse, Simulator, Risk Engine) are **read-only consumers** via authenticated internal API.

**No platform writes to the Data Engine except its own ingestion pipelines.**

## 2. Core Architectural Principle: Stock-as-Atom

Stock is the unit of computation. ALL metrics (RS, momentum, volatility, volume signals, risk metrics) are computed at stock level from `close_adj`. Everything else is aggregation:
- **Sector metrics** = market-cap-weighted aggregation of stock metrics
- **Fund metrics** = holding-weight aggregation of stock metrics
- **Breadth** = universe-wide counts/percentages of stock-level flags
- **Regime** = composite of breadth + flow + global signals

This eliminates data duplication and ensures consistency across all products.

## 3. Data Scope

| Domain | Universe | Source | Volume |
|--------|----------|--------|--------|
| Indian Equities | All NSE-listed (~2,000 active) | NSE BHAV copy (3 format eras) | ~10M OHLCV rows (10yr) |
| Mutual Funds | Equity, Growth, Regular (~450-550) | AMFI NAV daily + Morningstar API | ~5M NAV rows |
| MF Holdings | Same universe | fie2-db-1 Docker (2M+ rows) + Morningstar monthly | 2M+ rows |
| Indices | All 60+ NSE indices | NSE historical | ~500K rows |
| Global | Top 1,000 ETFs + major indices + macro | yfinance + FRED | ~3M rows |
| Flows | FII/DII daily, MF category monthly | NSE + AMFI | ~20K rows |
| F&O | Daily summary (PCR, OI, max pain) | NSE option chain | ~5K rows |
| Qualitative | Research, audio, PDFs | Goldilocks, RSS, manual upload | Ongoing |
| Client Portfolios | All clients | Encrypted PII, envelope encryption | ~366K rows |

## 4. Key Technical Decisions (Locked)

1. **PostgreSQL on existing RDS** — no new DB instances
2. **FastAPI + Pydantic v2** — async endpoints, SQLAlchemy 2.0 async
3. **Alembic** for ALL schema changes — never raw DDL
4. **Redis** for caching — with circuit breaker + DB fallback (Redis is performance, not correctness)
5. **Partitioned tables** — OHLCV by year (2000-2035), MF NAV by year (2006-2035)
6. **Advisory locks** — session-level `pg_advisory_lock` with `hashtext()` for deterministic IDs
7. **Idempotent pipelines** — every INSERT uses ON CONFLICT on natural keys
8. **Data status gating** — raw → validated → quarantined. API serves only validated
9. **Envelope encryption** — KMS CMK → per-client DEK → AES-256-GCM for PII
10. **Truncated HMAC blind indexes** — 8-char for searchable encrypted PII

## 5. Infrastructure

| Component | Location | Port |
|-----------|----------|------|
| Data Engine API | EC2 Docker | 8010 |
| PostgreSQL | RDS (existing) | 5432 |
| Redis | EC2 localhost | 6379 |
| PgBouncer | EC2 localhost | 6432 |
| Orchestrator Dashboard | EC2 127.0.0.1 | 8099 |
| ClamAV | EC2 | daemon |

## 6. Existing Data to Migrate

| Source | Location | Rows | Target |
|--------|----------|------|--------|
| compass_stock_prices | RDS fie_v3 | 1.4M (600 stocks done) | de_equity_ohlcv |
| nav_daily | fie2-db-1 Docker | 25.8M (filter to ~5M) | de_mf_nav_daily |
| fund_master | fie2-db-1 Docker | 13,380 | de_mf_master |
| fund_holding_detail | fie2-db-1 Docker | 2M+ | de_mf_holdings |
| index_constituents | RDS fie_v3 | 4,638 | de_index_constituents |
| cpp_* (client data) | RDS client_portal | 366K | de_clients (encrypted) |

## 7. Pipeline Schedule

| Pipeline | Trigger | SLA |
|----------|---------|-----|
| Pre-Market (global + macro) | 07:30 IST | 08:00 |
| T+1 Delivery | 09:00 IST | — |
| EOD (equity, MF, indices, flows, F&O) | 18:30 IST | Equity 19:30, MF 22:30 |
| RS Computation | After EOD | 23:00 |
| Regime Update | After RS | 23:30 |
| Reconciliation | 23:00 IST | — |
| Qualitative | Every 30 min | — |
| Weekly: Morningstar master | Sunday | — |
| Monthly: Morningstar holdings | 1st of month | — |
| Sunday 02:00: Full RS rebuild | Weekly | — |

## 8. API Surface

30+ endpoints serving Market Pulse, MF Pulse, and all downstream products.
JWT auth, Redis-cached, paginated, with X-Data-Freshness / X-Computation-Version / X-System-Status headers.

## 9. Priority Order (Build Sequence)

Market Pulse needs: equity OHLCV, technical indicators, RS, breadth, regime, flows, indices, F&O
MF Pulse needs: MF master, NAV, holdings, fund derived metrics, category flows

Both are urgent → build foundation, then equity + MF pipelines in parallel, then computation layer, then API layer.
