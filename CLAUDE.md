# JIP Data Engine

## Project Overview
Centralized PostgreSQL Data Engine for the Jhaveri Intelligence Platform (JIP).
Single FastAPI service (port 8010), single RDS database, all platforms read-only consumers.

## Tech Stack
- Python 3.11+, FastAPI, SQLAlchemy 2.0 async, Alembic, asyncpg
- PostgreSQL 12+ on RDS, Redis 7+, PgBouncer
- Docker deployment on EC2 (13.206.34.214)

## Key Conventions
- All tables prefixed `de_`
- Financial values: `Numeric(18,4)` — NEVER float
- Dates: DATE type — NEVER VARCHAR
- Every INSERT: ON CONFLICT on natural keys (idempotent)
- PII: encrypted before insert (envelope encryption)
- Data status: raw → validated → quarantined
- Indian formatting: lakh/crore, ₹ prefix, IST timezone

## Commands
- Run: `uvicorn app.main:app --host 0.0.0.0 --port 8010`
- Test: `pytest tests/ -v --tb=short`
- Lint: `ruff check . --select E,F,W`
- Type check: `mypy . --ignore-missing-imports`
- Migrate: `alembic upgrade head`
- New migration: `alembic revision --autogenerate -m "description"`

## Project Structure
```
app/
  main.py              # FastAPI app
  config.py            # Settings (pydantic-settings)
  db/                  # Database session, base
  models/              # SQLAlchemy models
  api/v1/              # API endpoints
  middleware/           # Auth, logging, caching
  pipelines/           # Data ingestion pipelines
    equity/            # BHAV copy, corporate actions
    mf/                # AMFI NAV, MF master
    indices/           # NSE indices
    flows/             # FII/DII
    global/            # yfinance, FRED
    qualitative/       # RSS, upload, Claude API
    morningstar/       # Morningstar API integration
  computation/         # RS, breadth, regime, technicals
  security/            # Encryption, HMAC
  services/            # Business logic
  utils/               # Helpers
tests/
alembic/
dashboard/             # Pipeline monitoring frontend
docker/
docs/specs/
```

## Database
- RDS endpoint: fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com
- Database: data_engine
- Credentials: in .env (never in code)
