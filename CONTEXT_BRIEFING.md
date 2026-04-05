# JIP Data Engine v2.0 — Full Context Briefing for Claude Code

## What We're Building
A centralized PostgreSQL Data Engine that replaces ALL legacy fragmented databases (mf_engine, fie_v3, client_portal, mf_pulse Docker DB) with one clean, hardened RDS database. Once built, all existing JIP platforms (Champion Trader, Client Portal, MF Pulse, Market Pulse) will connect ONLY to this database. Everything else gets retired.

## What's Already Done (DO NOT REDO)
- ✅ `data_engine` database CREATED on RDS (`fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com`, user: `fie_admin`, pwd: `Nimish1234`)
- ✅ Python venv created at `/home/ubuntu/data-engine-build/venv/` with all dependencies (FastAPI, SQLAlchemy, Alembic, asyncpg, pandas, pgvector, cryptography, yfinance)
- ✅ Redis installed and running on EC2
- ✅ 54GB free disk space on EC2 (36GB reclaimed via docker prune)
- ✅ KMS CMK created: `arn:aws:kms:ap-south-1:389517402998:key/541f3dbd-aeef-490f-811a-137108b2ae38` (alias: `data-engine-pii`)

## EC2 Access
- SSH: `ssh -i ~/.ssh/jsl-wealth-key.pem ubuntu@13.206.34.214`
- RDS: `fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com` port 5432
- Docker containers running: `fie2-db-1` (Postgres 16 with mf_pulse DB), `champion`, `fie2-redis-1`

## Data Sources Already on EC2
| Source | Location | Rows | Notes |
|--------|----------|------|-------|
| Fund Master | Docker `fie2-db-1` → `mf_pulse.fund_master` | 13,380 | Full Morningstar metadata |
| NAV Daily | Docker `fie2-db-1` → `mf_pulse.nav_daily` | 25.8M | 2006-2026 |
| Fund Holdings | Docker `fie2-db-1` → `mf_pulse.fund_holding_detail` | 2M+ | Sector weights, ISINs |
| Equity OHLCV | RDS `fie_v3.compass_stock_prices` | 1.4M | ⚠️ VARCHAR dates, DOUBLE values — need type conversion |
| Index Constituents | RDS `fie_v3.index_constituents` | 4,638 | |
| Global Pulse | `/home/ubuntu/global-pulse/` | varies | Stooq .txt format |

## MF Filter Logic (CRITICAL)
- `broad_category = 'Equity'`
- `distribution_status = 'Accumulated'` (Growth plans only)
- `fund_name LIKE '%Reg%'` (Regular plans only)
- Target: ~450-550 funds
- NO IDCW/dividend plans for now

## Morningstar API
- Access Code: `ftijxp6pf11ezmizn19otbz18ghq2iu4`
- Universe Code: `hoi7dvf1dvm67w36`
- Base: `https://api.morningstar.com/v2/service/mf`
- All 12 API hashes already in `/home/ubuntu/mfpulse_reimagined/backend/app/core/config.py`
- Key APIs: Identifier Data (`l308tct18q1h759g`), Holdings Detail (`fq9mxhk7xeb20f3b`), Portfolio Summary (`ryt74bh4koatkf2w`)
- NAV comes from AMFI (free), NOT Morningstar

## Credentials to Store in AWS Secrets Manager
- Goldilocks: `jhaveri.3110@gmail.com` / `AICPJ9616P`
- Morningstar Portal: `Mfsupport@jhaveritrade.com` / `fHaJBpeF%NAN$x7`

## Architecture Document
The complete v2.0 PRD with all schemas, pipelines, and operational details is in:
`/Users/nimishshah/projects/jip data core/JIP_Data_Engine_v2.0.md`

## Key Architecture Decisions
1. ONE Postgres DB, ONE FastAPI service (port 8010, internal only)
2. All tables prefixed `de_` (data engine)
3. Equity OHLCV and MF NAV partitioned by year
4. Envelope encryption for PII (KMS CMK → per-client DEK)
5. HMAC blind indexes for searchable encrypted fields
6. ON CONFLICT DO UPDATE everywhere (full idempotency)
7. PgBouncer for connection pooling (transaction mode, 50 server connections)
8. Redis with circuit breaker (3 failures → bypass for 60s)
9. All financial values: NUMERIC(18,4), never FLOAT

## Build Phases
1. ~~Infrastructure~~ (DONE)
2. Schema & Core Foundation (Alembic migrations)
3. Data Migration (migrate from legacy DBs)
4. API & Service Layer (FastAPI)
5. Ingestion Pipelines (EOD, Pre-market, T+1)
6. Computation (RS, Breadth, Regime, Technicals)
7. Validation & Go-Live
