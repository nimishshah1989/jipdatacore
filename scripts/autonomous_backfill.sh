#!/usr/bin/env bash
# Autonomous multi-year backfill for Atlas + BSE ownership pipelines.
#
# Runs ON EC2 (not locally). After `git pull` on the host, kick this off
# under nohup:
#   cd /home/ubuntu/jip-data-engine
#   nohup bash scripts/autonomous_backfill.sh > backfill.log 2>&1 &
#   tail -f backfill.log
#
# Walks 90-day windows from YEARS_BACK years ago to today, calling the
# /trigger/backfill endpoint per window and polling /trigger/status until
# the job completes. Each window runs all pipelines; failures are
# tolerated (pipelines graceful-fail or are skipped) so the loop never
# gets stuck. Final section dumps row counts per table.
set -euo pipefail

API_BASE=${API_BASE:-http://localhost:8010}
PIPELINE_KEY=${PIPELINE_KEY:-}
YEARS_BACK=${YEARS_BACK:-3}
WINDOW_DAYS=${WINDOW_DAYS:-90}
POLL_SECONDS=${POLL_SECONDS:-15}
CONTAINER=${CONTAINER:-jip-data-engine-data-engine-1}

# Pipelines to backfill (Atlas + BSE ownership set).
PIPELINES=(
  rbi_fx_rates
  rbi_policy_rates
  gsec_yields
  participant_oi
  fo_bhavcopy
  fo_ban_list
  insider_trades
  bulk_block_deals
  shareholding_pattern
  bse_ownership
)

if [ -z "$PIPELINE_KEY" ]; then
  # Try to pull it from container .env
  PIPELINE_KEY=$(docker exec "$CONTAINER" sh -c 'echo -n "$PIPELINE_API_KEY"' 2>/dev/null || true)
fi
if [ -z "$PIPELINE_KEY" ]; then
  echo "ERROR: PIPELINE_KEY env var not set and cannot read from container"
  exit 1
fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

api() {
  local method=$1 path=$2
  shift 2
  curl -fsS -X "$method" "${API_BASE}${path}" \
    -H "X-Pipeline-Key: ${PIPELINE_KEY}" \
    -H "Content-Type: application/json" \
    "$@"
}

# --- wait for API health ---
log "Checking /health on $API_BASE"
for i in $(seq 1 30); do
  if curl -fsS "${API_BASE}/health" >/dev/null 2>&1; then
    log "API is healthy"
    break
  fi
  sleep 2
done

# --- build window list ---
TODAY=$(date +%Y-%m-%d)
OLDEST=$(date -d "$TODAY - ${YEARS_BACK} years" +%Y-%m-%d)
log "Backfill plan: $OLDEST -> $TODAY in ${WINDOW_DAYS}-day windows"

pipelines_json=$(printf '%s\n' "${PIPELINES[@]}" | python3 -c 'import sys,json;print(json.dumps([l.strip() for l in sys.stdin if l.strip()]))')

window_start=$OLDEST
window_count=0
while [ "$window_start" \< "$TODAY" ] || [ "$window_start" = "$TODAY" ]; do
  window_count=$((window_count + 1))
  window_end=$(date -d "$window_start + $((WINDOW_DAYS - 1)) days" +%Y-%m-%d)
  # Clamp to today
  if [ "$window_end" \> "$TODAY" ]; then
    window_end=$TODAY
  fi

  log "Window #${window_count}: ${window_start} .. ${window_end}"

  body=$(python3 -c "
import json, sys
print(json.dumps({
  'pipeline_names': ${pipelines_json},
  'start_date': '${window_start}',
  'end_date': '${window_end}',
}))")

  # Queue backfill
  response=$(api POST /api/v1/pipeline/trigger/backfill -d "$body" || true)
  job_id=$(echo "$response" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("job_id",""))' 2>/dev/null || true)

  if [ -z "$job_id" ]; then
    log "ERROR: backfill trigger failed — response: $response"
    log "Skipping window ${window_start}..${window_end} and moving on"
  else
    log "Queued job $job_id — polling"
    while true; do
      sleep "$POLL_SECONDS"
      status_resp=$(api GET "/api/v1/pipeline/trigger/status/$job_id" 2>/dev/null || echo '{}')
      status=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("status",""))' 2>/dev/null || echo "")
      done_d=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("dates_done",""))' 2>/dev/null || echo "")
      total_d=$(echo "$status_resp" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("dates_total",""))' 2>/dev/null || echo "")
      log "  job $job_id: status=$status progress=${done_d}/${total_d}"
      if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
        break
      fi
    done
  fi

  # Advance window
  window_start=$(date -d "$window_end + 1 day" +%Y-%m-%d)
done

log "All windows queued. Dumping row counts."

docker exec "$CONTAINER" python3 -c "
import asyncio
from sqlalchemy import text
from app.db.session import async_session_factory

TABLES = [
    'de_fo_bhavcopy', 'de_fo_ban_list', 'de_participant_oi',
    'de_gsec_yield', 'de_rbi_fx_rate', 'de_rbi_policy_rate',
    'de_insider_trades', 'de_bulk_block_deals', 'de_shareholding_pattern',
    'de_bse_shareholding', 'de_bse_pledge_history',
    'de_bse_insider_trades', 'de_bse_sast_disclosures',
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
