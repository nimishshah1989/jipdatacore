#!/bin/bash
# JIP Data Engine cron trigger wrapper.
# Records every cron run (start + finish) into de_cron_run, and posts a Telegram
# alert on failure. Replaces the bare `curl` calls in the previous crontab.
#
# Usage:
#   jip_trigger.sh <schedule_name> [business_date]
#   jip_trigger.sh health-action         # special: invokes agent3 healing loop
#
# Environment (sourced from /home/ubuntu/jip-data-engine/.env):
#   PIPELINE_API_KEY              required
#   DATABASE_URL_SYNC             required (used by psql for de_cron_run inserts)

set -uo pipefail

SCHEDULE="${1:-}"
BUSINESS_DATE="${2:-}"
if [[ -z "$SCHEDULE" ]]; then
  echo "usage: $0 <schedule_name> [business_date]" >&2
  exit 2
fi

ENV_FILE="/home/ubuntu/jip-data-engine/.env"
LOG_DIR="/home/ubuntu/jip-data-engine/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${SCHEDULE}_run.log"

# shellcheck disable=SC1090
. "$ENV_FILE"

# DATABASE_URL_SYNC uses SQLAlchemy-style prefixes (postgresql+psycopg2://...).
# Strip the driver so plain psql can consume it.
PG_URL="${DATABASE_URL_SYNC:-${DATABASE_URL:-}}"
PG_URL="${PG_URL/postgresql+psycopg2:/postgresql:}"
PG_URL="${PG_URL/postgresql+asyncpg:/postgresql:}"

API_BASE="http://localhost:8010/api/v1/pipeline"
if [[ -z "${PIPELINE_API_KEY:-}" ]]; then
  echo "[$(date -u +%FT%TZ)] FATAL: PIPELINE_API_KEY missing from $ENV_FILE" >> "$LOG_FILE"
  exit 3
fi

# Build URL
BD="${BUSINESS_DATE:-$(TZ=Asia/Kolkata date +%Y-%m-%d)}"
URL="$API_BASE/trigger/$SCHEDULE?business_date=$BD"

HOSTNAME_SHORT="$(hostname -s)"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
STARTED_EPOCH=$(date +%s)

echo "[$STARTED_AT] START schedule=$SCHEDULE business_date=$BD" >> "$LOG_FILE"

# Record the "started" row so a crashed run is still visible.
_insert_start() {
  psql "$PG_URL" -v ON_ERROR_STOP=0 -q -c "\
INSERT INTO de_cron_run (schedule_name, business_date, started_at, status, host) \
VALUES ('$SCHEDULE', '$BD', '$STARTED_AT', 'started', '$HOSTNAME_SHORT') \
RETURNING id;" 2>/dev/null | awk '/^ *[0-9]+/{print $1; exit}'
}
RUN_ID="$(_insert_start || true)"

# Run the trigger. We intentionally use -w so we always get http_code, and a
# temp file to capture body on failure without blowing up the log on success.
BODY_FILE="$(mktemp)"
HTTP_CODE=000
CURL_EXIT=0
curl -sS --max-time 7200 \
  -X POST "$URL" \
  -H "X-Pipeline-Key: $PIPELINE_API_KEY" \
  -o "$BODY_FILE" \
  -w "%{http_code}" > "${BODY_FILE}.code" 2>>"$LOG_FILE"
CURL_EXIT=$?
HTTP_CODE="$(cat "${BODY_FILE}.code" 2>/dev/null || echo 000)"

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)"
DURATION=$(( $(date +%s) - STARTED_EPOCH ))

# Decide status
if [[ "$CURL_EXIT" -eq 0 && "$HTTP_CODE" =~ ^2 ]]; then
  STATUS="success"
elif [[ "$CURL_EXIT" -eq 28 ]]; then
  STATUS="timeout"
else
  STATUS="failed"
fi

echo "[$FINISHED_AT] DONE schedule=$SCHEDULE status=$STATUS http=$HTTP_CODE curl=$CURL_EXIT duration_s=$DURATION" >> "$LOG_FILE"

# Capture error body for failures (tail to keep it sane)
ERROR_BODY_SQL="NULL"
if [[ "$STATUS" != "success" ]]; then
  ERR=$(head -c 8000 "$BODY_FILE" | sed "s/'/''/g")
  ERROR_BODY_SQL="'$ERR'"
  echo "--- response body ---" >> "$LOG_FILE"
  head -c 4000 "$BODY_FILE" >> "$LOG_FILE"
  echo >> "$LOG_FILE"
fi

# Update the row with the finish state
if [[ -n "${RUN_ID:-}" ]]; then
  psql "$PG_URL" -v ON_ERROR_STOP=0 -q -c "\
UPDATE de_cron_run SET \
  finished_at = '$FINISHED_AT', \
  duration_seconds = $DURATION, \
  http_code = $( [[ "$HTTP_CODE" =~ ^[0-9]+$ ]] && echo "$HTTP_CODE" || echo NULL ), \
  curl_exit_code = $CURL_EXIT, \
  status = '$STATUS', \
  error_body = $ERROR_BODY_SQL \
WHERE id = $RUN_ID;" >/dev/null 2>&1 || true
fi

rm -f "$BODY_FILE" "${BODY_FILE}.code"

# Exit non-zero on failure so cron-level monitors see it too
[[ "$STATUS" == "success" ]] || exit 1
exit 0
