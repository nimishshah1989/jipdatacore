# Chunk 12 — Cleanup: delete old code after 7-day soak

**Complexity**: S
**Blocks**: —
**Blocked by**: chunk-11 + 7 consecutive green nights

## Goal
Delete the legacy hand-rolled indicator code once the new engine has been stable in production for a week. Archive rollback dumps to S3. File follow-up tickets for deferred work.

## Prerequisites (all must be true before this chunk starts)
- ✅ `compute_indicators_v2` pipeline has run successfully for 7 consecutive nights
- ✅ Observatory dashboard green for 7 days
- ✅ No alerts on `de_equity_technical_daily` / `de_index_technical_daily` / etc. freshness
- ✅ `breadth.py` + downstream consumers still working (verified via `test_breadth.py` + `test_runner.py` green)
- ✅ No rollback events logged
- ✅ User sign-off to proceed

## Files
- **Delete**: `app/computation/technicals.py` (the old 734-line hand-rolled file)
- **Delete**: `tests/computation/test_technicals.py` (superseded by `test_indicators_v2_golden.py`)
- **Modify**: `app/computation/runner.py` — remove imports of the old module; verify no lingering references
- **Modify**: `app/computation/__init__.py` — remove `technicals` export if present
- **Create**: `reports/indicators_v2_cutover_report.md`
  - Summary: chunks shipped, instruments processed, row counts per table, performance metrics, gotchas found
  - Archive: table dumps from chunks 6 and 7 (total ~3–5 GB compressed)
  - Next steps: breadth rollups, MF NAV backfill, additional indicator families if needed
- **S3 upload** (manual step, document in report):
  - `aws s3 cp /var/backups/jip/indicators_cutover/ s3://jsl-wealth-backups/jip-indicators-v2-cutover/ --recursive --storage-class GLACIER`
  - After successful upload: `rm -rf /var/backups/jip/indicators_cutover/` (free EC2 disk)
- **File GitHub issues** (JIP Data Core repo):
  1. "MF NAV backfill gap: only 1,255 / 13,380 funds have NAV history" (P0, follow-up)
  2. "Sector-level breadth rollups from de_index_technical_daily" (P1, new feature)
  3. "Wire up the deferred indicator families (Hilbert Transform, Ichimoku, Parabolic SAR edge cases)" (P2, nice-to-have)
  4. "Fundamentals rollup — sector P/E / P/B / dividend yield weighted by market cap" (P1, analytical unlock)

## Acceptance criteria
- `app/computation/technicals.py` deleted
- `tests/computation/test_technicals.py` deleted
- `grep -r "from app.computation.technicals" app/ tests/` returns zero hits
- `grep -r "import technicals" app/ tests/` returns zero hits
- Full test suite still green: `pytest tests/ -v --tb=short`
- `ruff`, `mypy` clean
- Dumps uploaded to S3 Glacier, local copies deleted
- 4 follow-up issues filed
- Cutover report committed to repo

## Verification commands
```bash
pytest tests/ -v --tb=short
ruff check . --select E,F,W
mypy . --ignore-missing-imports
grep -r "technicals.py" app/ tests/ || echo "clean"
aws s3 ls s3://jsl-wealth-backups/jip-indicators-v2-cutover/
```

## Exit criteria for the whole project
Once this chunk is done, the indicators overhaul is complete:
- 5 asset classes have ~130 indicators computed daily
- One shared engine, one shared library (pandas-ta-classic + empyrical-reloaded)
- Zero hand-rolled formula code
- Full rollback plan archived for 90 days (then Glacier forever)
- Follow-up tickets tracking the known gaps
