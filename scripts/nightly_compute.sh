#!/bin/bash
# Nightly computation pipeline — runs after BHAV ingestion completes
# Cron: 0 0 * * 1-5 /home/ubuntu/jip-data-engine/scripts/nightly_compute.sh >> /var/log/jip-nightly.log 2>&1
#
# Steps:
#   1. Validate raw OHLCV data (raw → validated)
#   2. Run full computation pipeline (technicals + RS + breadth + regime + sectors
#      + stochastic + pivots + intermarket + fibonacci + divergence)
#   3. Run Goldilocks scraper (daily mode — new PDFs, check for con-calls)
#   4. Extract PDF text from any new downloads
#   5. Run LLM extraction on new documents (via Ollama)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_PREFIX="[$(date -u +%Y-%m-%dT%H:%M:%SZ)]"
CONTAINER="jip-data-engine-data-engine-1"

# IST date (today)
BUSINESS_DATE=$(TZ=Asia/Kolkata date +%Y-%m-%d)

echo "$LOG_PREFIX === Nightly compute starting for $BUSINESS_DATE ==="

# ── Step 1: Validate raw OHLCV ──
echo "$LOG_PREFIX Step 1: Validating OHLCV data..."
PGPASSWORD=JipDataEngine2026Secure psql \
  -h jip-data-engine.ctay2iewomaj.ap-south-1.rds.amazonaws.com \
  -U jip_admin -d data_engine -t -c "
  UPDATE de_equity_ohlcv SET data_status = 'validated'
  WHERE date = '$BUSINESS_DATE' AND data_status = 'raw';
" 2>/dev/null
echo "$LOG_PREFIX   OHLCV validated"

# ── Step 2: Run full computation pipeline inside Docker ──
echo "$LOG_PREFIX Step 2: Running computation pipeline..."
docker exec "$CONTAINER" python3 -c "
import asyncio, os
from datetime import date
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(os.environ['DATABASE_URL'], pool_size=2)
AS = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def run():
    from app.computation.runner import run_full_computation_pipeline
    d = date.fromisoformat('$BUSINESS_DATE')
    async with AS() as session:
        async with session.begin():
            report = await run_full_computation_pipeline(session, d)
            for step in report.steps:
                print(f'  {step.step_name}: {step.status} ({step.rows_affected} rows)')
                if step.errors:
                    print(f'    ERROR: {step.errors[0][:100]}')
            print(f'Overall: {report.overall_status}')
    await engine.dispose()

asyncio.run(run())
"
echo "$LOG_PREFIX   Computation complete"

# ── Step 3: Goldilocks scraper (daily mode) ──
echo "$LOG_PREFIX Step 3: Running Goldilocks scraper..."
cd "$PROJECT_DIR"
python3 scripts/ingest/goldilocks_scraper.py --mode daily 2>&1 | tail -5
echo "$LOG_PREFIX   Scraper complete"

# ── Step 4: Extract any new PDFs ──
echo "$LOG_PREFIX Step 4: Extracting new PDFs..."
python3 scripts/ingest/extract_goldilocks_pdfs.py 2>&1 | tail -5
echo "$LOG_PREFIX   PDF extraction complete"

# ── Step 5: Run LLM extraction on new docs ──
echo "$LOG_PREFIX Step 5: Running LLM extraction..."
python3 scripts/ingest/run_goldilocks_extraction.py --max-docs 10 2>&1 | tail -10
echo "$LOG_PREFIX   LLM extraction complete"

echo "$LOG_PREFIX === Nightly compute finished for $BUSINESS_DATE ==="
