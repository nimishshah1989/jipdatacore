#!/bin/bash
# Nightly computation pipeline — unified trigger API execution
# Cron: 0 0 * * 1-5 /home/ubuntu/jip-data-engine/scripts/nightly_compute.sh >> /var/log/jip-nightly.log 2>&1
#
# ALL steps go through the trigger API — single execution path.
# No more direct SQL, no more docker exec.
#
# Steps (via nightly_compute schedule):
#   1. __validate_ohlcv__      — validate raw OHLCV (raw → validated)
#   2. equity_technicals_sql   — SMA50/200
#   3. equity_technicals_pandas — EMA/RSI/MACD/ADX/Bollinger
#   4. relative_strength       — RS scores (equity/MF/sector)
#   5. market_breadth          — advance/decline, % above DMA
#   6. regime_detection        — BULL/BEAR/SIDEWAYS regime
#   7. mf_derived              — Sharpe/Sortino/MaxDD/Beta/Alpha
#   8. etf_technicals          — ETF technical indicators
#   9. etf_rs                  — ETF relative strength
#  10. global_technicals       — Global instrument technicals
#  11. global_rs               — Global relative strength
#  12. __goldilocks_compute__  — Goldilocks scraper + PDF + LLM extraction

set -euo pipefail

LOG_PREFIX="[$(date -u +%Y-%m-%dT%H:%M:%SZ)]"

# IST date (today)
BUSINESS_DATE=$(TZ=Asia/Kolkata date +%Y-%m-%d)

# API config — reads from env or defaults to localhost
API_BASE="${JIP_API_BASE:-http://localhost:8010}"
API_KEY="${JIP_PIPELINE_KEY:?JIP_PIPELINE_KEY must be set}"

echo "$LOG_PREFIX === Nightly compute starting for $BUSINESS_DATE ==="

# Trigger the full nightly_compute schedule via API
echo "$LOG_PREFIX Triggering nightly_compute schedule..."
# Use curl config to avoid exposing API key in process list (ps aux)
RESPONSE=$(curl -sf -X POST \
  "${API_BASE}/api/v1/pipeline/trigger/nightly_compute?business_date=${BUSINESS_DATE}" \
  --config <(printf 'header = "X-Pipeline-Key: %s"\n' "${API_KEY}") \
  -H "Content-Type: application/json" \
  --max-time 7200)

# Parse response
STATUS=$(echo "$RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
pipelines = data.get('pipelines', [])
failed = [p['pipeline_name'] for p in pipelines if p.get('status') == 'failed']
success = [p['pipeline_name'] for p in pipelines if p.get('status') == 'success']
print(f'Success: {len(success)}/{len(pipelines)}')
if failed:
    print(f'Failed: {", ".join(failed)}')
    for p in pipelines:
        if p.get('status') == 'failed':
            print(f'  {p[\"pipeline_name\"]}: {p.get(\"error\", \"unknown\")}')
    sys.exit(1)
")

echo "$LOG_PREFIX $STATUS"
echo "$LOG_PREFIX === Nightly compute finished for $BUSINESS_DATE ==="
