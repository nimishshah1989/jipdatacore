#!/bin/bash
# Agent 3 self-healing loop — deterministic (no LLM).
# Replaces the markdown "agent" with a real cron-invokable script.
#
# Flow (per docs/agents/agent-3-health-check.md):
#   1) GET /observatory/health-action -> list of stale streams
#   2) For each (respecting weekend skip list and retries<max_retries),
#      POST /pipeline/trigger/single/{pipeline_to_fix}
#   3) Wait 30s, GET /observatory/pulse, see if stream turned fresh
#   4) POST /observatory/healing-result to log the attempt
# All results land in de_healing_log + agent3.log and show up on the dashboard.

set -uo pipefail

ENV_FILE="/home/ubuntu/jip-data-engine/.env"
LOG_DIR="/home/ubuntu/jip-data-engine/logs"
LOG_FILE="$LOG_DIR/agent3.log"
mkdir -p "$LOG_DIR"
# shellcheck disable=SC1090
. "$ENV_FILE"

API="http://localhost:8010/api/v1"
KEY_HDR="X-Pipeline-Key: $PIPELINE_API_KEY"

DOW=$(date +%u)  # 6=Sat 7=Sun
IS_WEEKEND=0
[[ "$DOW" -ge 6 ]] && IS_WEEKEND=1

WEEKEND_SKIP=(
  equity_ohlcv equity_technicals rs_scores market_breadth market_regime
  mf_nav mf_derived mf_holdings mf_flows
  corporate_actions institutional_flows index_prices
)

_is_weekend_skipped() {
  local sid="$1"
  for s in "${WEEKEND_SKIP[@]}"; do [[ "$s" == "$sid" ]] && return 0; done
  return 1
}

TS="$(date -u +%FT%TZ)"
echo "[$TS] agent3 start weekend=$IS_WEEKEND" >> "$LOG_FILE"

ACTIONS=$(curl -s "$API/observatory/health-action")
TOTAL=$(echo "$ACTIONS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('actions_needed',0))")
echo "[$TS] actions_needed=$TOTAL" >> "$LOG_FILE"

FIXED=0
FAILED=0
SKIPPED=0
SUMMARY=""

# Parse each action row
while IFS=$'\t' read -r SID STATUS PIPELINE RETRIES SHOULD_FIX; do
  [[ -z "$SID" ]] && continue
  if [[ "$IS_WEEKEND" -eq 1 ]] && _is_weekend_skipped "$SID"; then
    SKIPPED=$((SKIPPED+1))
    continue
  fi
  if [[ "$SHOULD_FIX" != "True" ]]; then
    SKIPPED=$((SKIPPED+1))
    continue
  fi

  echo "[$(date -u +%FT%TZ)] fixing $SID via $PIPELINE (retries_today=$RETRIES)" >> "$LOG_FILE"
  BD=$(TZ=Asia/Kolkata date +%Y-%m-%d)

  TRIG_BODY=$(mktemp)
  TRIG_CODE=$(curl -s -o "$TRIG_BODY" -w "%{http_code}" \
    -X POST "$API/pipeline/trigger/single/$PIPELINE?business_date=$BD" \
    -H "$KEY_HDR" --max-time 1800)
  TRIG_STATUS="failed"
  [[ "$TRIG_CODE" =~ ^2 ]] && TRIG_STATUS="success"

  sleep 30
  # Verify via pulse
  NEW_STATUS=$(curl -s "$API/observatory/pulse" | python3 -c "
import json,sys
sid='$SID'
d=json.load(sys.stdin)
for s in d['streams']:
  if s['stream_id']==sid:
    print(s.get('status','unknown')); break
")
  RESULT="failed"
  [[ "$NEW_STATUS" == "fresh" ]] && RESULT="success"
  ERR=""
  [[ "$RESULT" == "failed" ]] && ERR=$(head -c 400 "$TRIG_BODY" | tr -d '\n' | sed 's/"/\\"/g')

  # Log the healing attempt
  curl -s -X POST "$API/observatory/healing-result" \
    -H "Content-Type: application/json" \
    -d "{\"stream_id\":\"$SID\",\"pipeline_triggered\":\"$PIPELINE\",\"action\":\"trigger\",\"result\":\"$RESULT\",\"retries\":$((RETRIES+1)),\"error_detail\":\"$ERR\"}" \
    >/dev/null 2>&1

  if [[ "$RESULT" == "success" ]]; then
    FIXED=$((FIXED+1))
    SUMMARY+="✅ $SID → fresh
"
  else
    FAILED=$((FAILED+1))
    SUMMARY+="❌ $SID (via $PIPELINE) still $NEW_STATUS (http=$TRIG_CODE)
"
  fi
  rm -f "$TRIG_BODY"
done < <(echo "$ACTIONS" | python3 -c "
import json,sys
for a in json.load(sys.stdin)['actions']:
  print('\t'.join([
    a['stream_id'], a['status'], str(a['pipeline_to_fix']),
    str(a['retries_today']), str(a['should_fix'])
  ]))
")

DONE_TS="$(date -u +%FT%TZ)"
echo "[$DONE_TS] agent3 done fixed=$FIXED failed=$FAILED skipped=$SKIPPED" >> "$LOG_FILE"
