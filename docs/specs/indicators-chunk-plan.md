# Indicators Overhaul — Chunk Plan

Source PRD: `docs/specs/indicators-prd.md`
Total chunks: **12**
Estimated effort (supervised): 8–12 focused days
Estimated effort (autonomous ralph): 2–4 days

## Build order and dependency graph

```
1 (deps)
 └─▶ 2 (migrations)
      ├─▶ 3 (engine core)
      │    ├─▶ 4 (golden tests)
      │    │    └─▶ 5 (equity backfill + diff)
      │    │         └─▶ 6 (equity cutover)
      │    │              └─▶ 7 (etf + global cutovers)
      │    │                   └─▶ 8 (indices — new table)
      │    │                        └─▶ 11 (pipeline + cron)
      │    │                             └─▶ 12 (cleanup)
      │    └─▶ 10 (mf asset) ──┐
      └─▶ 9 (purchase_mode bootstrap + mstar fix) ──┘
                                                    └─▶ 11
```

Chunks 9 and 10 can run in parallel with 4–8 once 2 and 3 land.

## Chunks

| # | Name | Complexity | Blocks | Blocked by |
|---|---|---|---|---|
| 1 | Dependencies & package scaffold | S | 2 | — |
| 2 | Alembic migrations: v2 tables + purchase_mode | M | 3, 9 | 1 |
| 3 | Engine core: spec, engine, strategy.yaml, risk_metrics | L | 4, 5, 7, 8, 10 | 2 |
| 4 | TA-Lib oracle + golden-file tests | M | 5 | 3 |
| 5 | Equity asset wrapper + backfill + diff | L | 6 | 4 |
| 6 | Equity cutover (dump → drop → rename) | M | 7 | 5 |
| 7 | ETF + global asset wrappers + cutovers | M | 8 | 6 |
| 8 | Index asset wrapper (new table) | S | 11 | 7 |
| 9 | purchase_mode bootstrap + Morningstar fix | M | 10 | 2 |
| 10 | MF asset wrapper + backfill | M | 11 | 3, 9 |
| 11 | Pipeline registration + cron wiring | S | 12 | 8, 10 |
| 12 | Cleanup: delete old code after 7-day soak | S | — | 11 |

## Acceptance criteria (global)

Every chunk must pass before the next chunk starts:
- `ruff check . --select E,F,W` — clean
- `mypy . --ignore-missing-imports` — clean
- `pytest tests/ -v --tb=short` — all green
- Any new table: `alembic upgrade head && alembic downgrade -1 && alembic upgrade head` clean
- Any new indicator code: golden-file parity against TA-Lib within `1e-6`

## Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| pandas-ta-classic RSI differs from TA-Lib at 1e-6 | M | L | Relax tolerance to 1e-4 and document; pandas-ta uses Wilder's smoothing by default which matches TA-Lib |
| GENERATED columns break on rename | L | H | Chunk 2 spec includes identical GENERATED clauses; chunk 6 explicitly runs `test_breadth.py` post-rename |
| mfpulse RDS not reachable from JIP EC2 | M | M | Chunk 9 fallback: run bootstrap from local dev with SSH tunnels, dump CSV, upload |
| JIP Morningstar ingestion doesn't fetch OperationsMasterFile | M | M | Chunk 9 sub-task flagged; if missing, scope extends — may split into 9a/9b |
| Backfill OOM on t3.large for globals with long history | L | M | Chunk 5 chunks-by-instrument pattern already handles this |
| 1,255 MFs with NAV << 13,380 master | CERTAIN | L | Out of scope, flagged as P0 follow-up ticket |
