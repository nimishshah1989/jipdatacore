#!/usr/bin/env bash
# Parallel autonomous backfill — one concurrent job per pipeline, each
# walking its own 90-day windows. Total wall time = time for the slowest
# pipeline to complete, instead of SUM across pipelines.
#
# Usage on EC2:
#   cd /home/ubuntu/jip-data-engine
#   nohup bash scripts/parallel_backfill.sh > backfill.log 2>&1 &
#   disown
#   tail -f backfill.log
set -euo pipefail

API_BASE=${API_BASE:-http://localhost:8010}
PIPELINE_KEY=${PIPELINE_KEY:-}
YEARS_BACK=${YEARS_BACK:-1}
WINDOW_DAYS=${WINDOW_DAYS:-90}
POLL_SECONDS=${POLL_SECONDS:-20}
CONTAINER=${CONTAINER:-jip-data-engine-data-engine-1}
LOG_DIR=${LOG_DIR:-./backfill_logs}

# Skip fo_bhavcopy, bulk_block_deals, gsec_yields — their sources are
# broken/anti-bot-blocked and they waste ~2 min per date on timeouts.
# They graceful-fail, so including them just slows the run without
# adding any data. Run them separately once URLs are fixed.
PIPELINES=(
  rbi_fx_rates
  rbi_policy_rates
  participant_oi
  fo_ban_list
  insider_trades
  shareholding_pattern
  bse_ownership
)

if [ -z "$PIPELINE_KEY" ]; then
  PIPELINE_KEY=$(docker exec "$CONTAINER" sh -c 'echo -n "$PIPELINE_API_KEY"' 2>/dev/null || true)
fi
if [ -z "$PIPELINE_KEY" ]; then
  echo "ERROR: PIPELINE_KEY env var not set and cannot read from container"
  exit 1
fi

mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

api() {
  local method=$1 path=$2
  shift 2
  curl -fsS -X "$method" "${API_BASE}${path}" \
    -H "X-Pipeline-Key: ${PIPELINE_KEY}" \
    -H "Content-Type: application/json" \
    "$@"
}

log "Checking /health on $API_BASE"
for i in $(seq 1 30); do
  if curl -fsS "${API_BASE}/health" >/dev/null 2>&1; then
    log "API is healthy"
    break
  fi
  sleep 2
done

TODAY=$(date +%Y-%m-%d)
OLDEST=$(date -d "$TODAY - ${YEARS_BACK} years" +%Y-%m-%d)
log "Plan: $OLDEST -> $TODAY (${YEARS_BACK}y), ${WINDOW_DAYS}-day windows"
log "Pipelines (parallel): ${PIPELINES[*]}"

run_pipeline_windows() {
  local pipeline=$1
  local plog="${LOG_DIR}/${pipeline}.log"
  : > "$plog"

  local window_start=$OLDEST
  local window_count=0
  while [ "$window_start" \< "$TODAY" ] || [ "$window_start" = "$TODAY" ]; do
    window_count=$((window_count + 1))
    local window_end
    window_end=$(date -d "$window_start + $((WINDOW_DAYS - 1)) days" +%Y-%m-%d)
    if [ "$window_end" \> "$TODAY" ]; then
      window_end=$TODAY
    fi

    echo "[$(date '+%H:%M:%S')] [${pipeline}] window ${window_count}: ${window_start}..${window_end}" >> "$plog"

    local body
    body=$(python3 -c "
import json
print(json.dumps({
  'pipeline_names': ['${pipeline}'],
  'start_date': '${window_start}',
  'end_date': '${window_end}',
}))")

    local response job_id
    response=$(api POST /api/v1/pipeline/trigger/backfill -d "$body" 2>>"$plog" || echo '{}')
    job_id=$(echo "$response" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("job_id",""))' 2>/dev/null || echo "")

    if [ -z "$job_id" ]; then
      echo "[$(date '+%H:%M:%S')] [${pipeline}] FAILED to queue window — skipping" >> "$plog"
    else
      echo "[$(date '+%H:%M:%S')] [${pipeline}] queued ${job_id}" >> "$plog"
      while true; do
        sleep "$POLL_SECONDS"
        local status_resp status done_d total_d
        status_resp=$(api GET "/api/v1/pipeline/trigger/status/${job_id}" 2>/dev/null || echo '{}')
        status=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("status",""))' 2>/dev/null || echo "")
        done_d=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("dates_done",""))' 2>/dev/null || echo "")
        total_d=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("dates_total",""))' 2>/dev/null || echo "")
        echo "[$(date '+%H:%M:%S')] [${pipeline}] ${job_id} status=${status} ${done_d}/${total_d}" >> "$plog"
        if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
          break
        fi
      done
    fi

    window_start=$(date -d "$window_end + 1 day" +%Y-%m-%d)
  done

  echo "[$(date '+%H:%M:%S')] [${pipeline}] DONE (${window_count} windows)" >> "$plog"
}

# Launch all pipelines in parallel
for p in "${PIPELINES[@]}"; do
  run_pipeline_windows "$p" &
  log "Launched background worker for ${p} (pid $!)"
done

log "All ${#PIPELINES[@]} pipelines running in parallel — check ${LOG_DIR}/<pipeline>.log"
log "This shell will block until all complete. Aggregate progress below:"

# Show live progress roll-up every 30s
while jobs -r | grep -q .; do
  sleep 30
  log "--- progress ---"
  for p in "${PIPELINES[@]}"; do
    last_line=$(tail -n 1 "${LOG_DIR}/${p}.log" 2>/dev/null || echo "no log yet")
    echo "  ${p}: ${last_line}"
  done
done

wait
log "All workers finished. Row-count summary:"

docker exec "$CONTAINER" python3 -c "
import asyncio
from sqlalchemy import text
from app.db.session import async_session_factory

TABLES = [
    'de_participant_oi', 'de_rbi_fx_rate', 'de_rbi_policy_rate',
    'de_fo_ban_list', 'de_insider_trades', 'de_shareholding_pattern',
    'de_bse_shareholding', 'de_bse_pledge_history',
    'de_bse_insider_trades', 'de_bse_sast_disclosures',
    'de_fo_bhavcopy', 'de_bulk_block_deals', 'de_gsec_yield',
]

async def main():
    async with async_session_factory() as s:
        for t in TABLES:
            try:
                r = await s.execute(text(f'SELECT count(*) FROM {t}'))
                print(f'{t:40s} {r.scalar_one():>10,d}')
            except Exception as e:
                print(f'{t:40s} ERROR: {e}')

asyncio.run(main())
"

log "Backfill run complete."
