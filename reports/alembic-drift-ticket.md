# Alembic migration chain broken at 002 → 003

**Date**: 2026-04-14
**Priority**: P1 (hygiene — blocks clean `alembic upgrade head` on fresh DB)
**Discovered in**: IND-C2 indicators overhaul verification

## Symptom
`alembic.script.ScriptDirectory.from_config()` raises `KeyError: '002_expand_global_instrument_type'` when trying to build the revision map. As a result, `alembic upgrade head`, `alembic downgrade`, `alembic history`, etc. all fail.

## Root cause
Migration **002** declares `revision = "002"` but migration **003** declares `down_revision = "002_expand_global_instrument_type"`. These identifiers don't match — there is no node `"002_expand_global_instrument_type"` in the revision map because migration 002 was never renamed.

Current revision chain:
```
001 (down=None)
  └── 002 "002" (down="001")
         └── [BROKEN] — 003's down_revision is "002_expand_global_instrument_type" which doesn't exist
             003 "003_goldilocks"
               └── 004 "004_healing_log"
                    └── 005 "005_cron_run"
                         └── 006 "006_embedding_384"
                              └── 007 "007_purchase_mode" (new, this build)
                                   └── 008 "008_indicators_v2" (new, this build)
```

## Impact on production
**None today.** Production RDS already has all migrations 001–006 applied (out-of-band column additions also happened — see the migration drift in `de_equity_technical_daily` which now has 46 columns vs 10 defined in migration 001). The chain break only prevents *new* deployments from running `alembic upgrade head` from scratch.

## Impact on this build
IND-C2 was verified by extracting the raw SQL from migration 008 and executing it directly against a throwaway Postgres container, bypassing `alembic upgrade`. This proved:
- Migration 008 SQL is valid
- All 5 tables create correctly with 104/104/104/93/67 columns
- Generated columns compute correctly
- FKs resolve

The alembic drift does NOT invalidate 008 — it just means we need to fix the drift before running the full upgrade sequence on any new environment.

## Additional drift discovered 2026-04-14 (during IND-C3c sidetask)
Production `alembic_version.version_num` = `003_goldilocks`, but the schema clearly has migrations 004/005/006 applied:
- `de_healing_log` table exists (migration 004)
- `de_cron_run` table exists (migration 005)
- `de_qual_documents.embedding` is `vector(384)` with HNSW index (migration 006)

**Actual prod state after today**: migrations 001–006 applied to schema, migration 009 (`ix_de_mf_holdings_asof_instr` index) manually applied today to prod via SSH+psql, but `alembic_version` still reads `003_goldilocks`. Migrations 007 (purchase_mode) and 008 (v2 technical tables) not yet applied to prod — those will land in IND-C5/C6/C7 cutover phases.

**Reconciliation plan** (when this ticket is picked up):
1. Fix the 002→003 revision chain (one-line rename in migration 002)
2. Verify `alembic_version` reflects actual schema state; `alembic stamp 006_embedding_384` if needed
3. Separate stamp for 009 once 007/008 have been properly applied

## Fix
**Option A (recommended)**: rename migration 002 to match the down_revision that 003 expects. One-line change:
```python
# alembic/versions/002_expand_global_instrument_type.py
revision = "002_expand_global_instrument_type"  # was: "002"
```
Zero runtime impact if production's `alembic_version` table currently has `"002"` as a past state — since migrations 003-006 are already applied, Alembic doesn't re-check historical revision IDs.

**Option B**: rename 003's down_revision to `"002"`. Also works but less descriptive.

## Verification after fix
```bash
docker run --rm jip-data-engine:ind-c2 python -c "
from alembic.config import Config
from alembic.script import ScriptDirectory
cfg = Config('alembic.ini')
cfg.set_main_option('script_location', 'alembic')
sd = ScriptDirectory.from_config(cfg)
for r in sd.walk_revisions():
    print(r.revision, '<-', r.down_revision)
"
```
Should print the full chain from 008 back to 001 without errors.

## Deferred to
Separate follow-up PR. Not blocking IND-C3 (engine core) or subsequent chunks since they're all SQL/Python code that runs against whatever schema is already in production.
