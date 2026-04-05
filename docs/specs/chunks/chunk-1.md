# Chunk 1: Project Scaffold

**Layer:** 0
**Dependencies:** None
**Complexity:** Medium
**Status:** done

## Files

- `app/__init__.py`
- `app/main.py`
- `app/config.py`
- `app/db/__init__.py`
- `app/db/session.py`
- `app/db/base.py`
- `tests/__init__.py`
- `tests/conftest.py`
- `alembic.ini`
- `alembic/env.py`
- `alembic/versions/` (empty, migrations added in C2)
- `pyproject.toml`
- `Dockerfile`
- `docker-compose.yml`
- `.env.example`
- `CLAUDE.md`
- `.github/workflows/ci.yml`

## Acceptance Criteria

- [ ] FastAPI app boots and `/health` endpoint returns `{"status": "ok"}`
- [ ] SQLAlchemy 2.0 async engine configured with `asyncpg` driver
- [ ] Alembic initialized with async support (`asyncpg` in `env.py`)
- [ ] Docker image builds cleanly; `docker-compose up` starts the service on port 8010
- [ ] `pytest tests/ -v --tb=short` runs with zero failures (smoke test only at this stage)
- [ ] `pytest-asyncio` configured (asyncio_mode = auto in `pyproject.toml`)
- [ ] `structlog` configured with JSON output in production, pretty in dev
- [ ] `.env.example` contains all required env vars: `DATABASE_URL`, `REDIS_URL`, `JWT_SECRET`, `PII_KMS_KEY_ARN`, `PII_HMAC_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `FRED_API_KEY`
- [ ] `pyproject.toml` lists all runtime dependencies: fastapi, uvicorn, sqlalchemy, asyncpg, alembic, redis, pyjwt, bcrypt, httpx, pandas, numpy, structlog, pydantic-settings, python-dotenv
- [ ] GitHub Actions CI runs on push: lint (`ruff check`), type check (`mypy`), tests (`pytest`)
- [ ] `CLAUDE.md` committed with project conventions

## Notes

This is the foundation chunk. Nothing else can be built until C1 is complete. The FastAPI app runs on port 8010 (`core.jslwealth.in:8010`, internal only). CORS is disabled — this is an internal API.

Database connection uses `pool_size=5, max_overflow=10, pool_pre_ping=True`. Connection goes through PgBouncer on port 6432 in production (port 5432 direct in dev/test).

Target RDS endpoint: `fie-db.c7osw6q6kwmw.ap-south-1.rds.amazonaws.com`, database `data_engine`. All tables use the `de_` prefix.

The `/health` endpoint must be unauthenticated (no JWT required) — it is used by Docker healthcheck and load balancer probes.
