#!/usr/bin/env bash
#
# Cutover script for indicators v2 tables.
#
# Given an asset class, dumps the existing v1 technical table to a timestamped
# .sql.gz (for rollback), drops it, and renames the v2 twin into the old name.
# Also renames indexes and PK constraints to match the final table name so
# downstream tooling (alembic, pg_dump, etc.) sees a clean schema.
#
# Usage (run from EC2 jumpbox or anywhere with psql + the right env):
#
#     DATABASE_URL_SYNC="postgresql://jip_admin:...@host:5432/data_engine" \
#         bash scripts/cutover_indicators_v2.sh equity
#
# Supported assets: equity, etf, global
# (index and mf are NEW tables with no v1 counterpart — no cutover needed)
#
# SAFETY: refuses to run if v2 has fewer than 1000 rows, guards against
# an accidental cutover before the backfill is done.
#
set -euo pipefail

ASSET="${1:-}"
if [[ -z "$ASSET" ]]; then
    echo "Usage: $0 {equity|etf|global}" >&2
    exit 1
fi

case "$ASSET" in
    equity)
        V1_TABLE="de_equity_technical_daily"
        V2_TABLE="de_equity_technical_daily_v2"
        ID_COL="instrument_id"
        ;;
    etf)
        V1_TABLE="de_etf_technical_daily"
        V2_TABLE="de_etf_technical_daily_v2"
        ID_COL="ticker"
        ;;
    global)
        V1_TABLE="de_global_technical_daily"
        V2_TABLE="de_global_technical_daily_v2"
        ID_COL="ticker"
        ;;
    *)
        echo "Unknown asset: $ASSET" >&2
        exit 1
        ;;
esac

if [[ -z "${DATABASE_URL_SYNC:-}" ]]; then
    echo "DATABASE_URL_SYNC env var required" >&2
    exit 1
fi

BACKUP_DIR="${BACKUP_DIR:-/tmp/jip_cutover_backups}"
mkdir -p "$BACKUP_DIR"
TS=$(date +%Y%m%d_%H%M%S)
DUMP_FILE="$BACKUP_DIR/${V1_TABLE}_pre_v2_${TS}.dump"

echo "== Cutover plan =="
echo "  asset    = $ASSET"
echo "  v1 table = $V1_TABLE"
echo "  v2 table = $V2_TABLE"
echo "  dump to  = $DUMP_FILE"
echo

V2_ROWS=$(psql "$DATABASE_URL_SYNC" -tAc "SELECT COUNT(*) FROM $V2_TABLE")
echo "v2 row count: $V2_ROWS"
if [[ "$V2_ROWS" -lt 1000 ]]; then
    echo "REFUSING: $V2_TABLE has fewer than 1000 rows" >&2
    exit 1
fi

V1_ROWS=$(psql "$DATABASE_URL_SYNC" -tAc "SELECT COUNT(*) FROM $V1_TABLE")
echo "v1 row count: $V1_ROWS"

if [[ "${ASSUME_YES:-}" != "1" ]]; then
    echo
    read -r -p "Type YES to proceed with destructive cutover: " CONFIRM
    if [[ "$CONFIRM" != "YES" ]]; then
        echo "Aborted."
        exit 1
    fi
fi

echo
echo "== Dumping $V1_TABLE =="
pg_dump "$DATABASE_URL_SYNC" \
    --table="public.${V1_TABLE}" \
    --no-owner --no-privileges \
    -Fc -f "$DUMP_FILE"

DUMP_SIZE=$(stat -c%s "$DUMP_FILE" 2>/dev/null || stat -f%z "$DUMP_FILE")
DUMP_SHA=$(shasum -a 256 "$DUMP_FILE" | awk '{print $1}')
echo "dump size: $DUMP_SIZE bytes"
echo "dump sha256: $DUMP_SHA"

echo
echo "== DROP $V1_TABLE + RENAME $V2_TABLE -> $V1_TABLE =="
psql "$DATABASE_URL_SYNC" -v ON_ERROR_STOP=1 <<SQL
BEGIN;
DROP TABLE IF EXISTS ${V1_TABLE} CASCADE;
ALTER TABLE ${V2_TABLE} RENAME TO ${V1_TABLE};
ALTER INDEX ${V2_TABLE}_pkey RENAME TO ${V1_TABLE}_pkey;
ALTER INDEX ix_${V2_TABLE}_${ID_COL} RENAME TO ix_${V1_TABLE}_${ID_COL};
ALTER INDEX ix_${V2_TABLE}_${ID_COL}_date RENAME TO ix_${V1_TABLE}_${ID_COL}_date;
COMMIT;
SQL

echo
echo "== Verification =="
psql "$DATABASE_URL_SYNC" -c "SELECT COUNT(*) AS rows_after_rename FROM ${V1_TABLE}"

echo
echo "== Cutover complete =="
echo "Dump preserved at: $DUMP_FILE"
