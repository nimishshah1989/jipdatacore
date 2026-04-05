import asyncio
import argparse
from datetime import datetime
from app.orchestrator.dag import run_eod_dag
from app.logging import get_logger

logger = get_logger(__name__)

async def run(date_str=None):
    if date_str:
        business_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        business_date = datetime.now().date()
        
    logger.info(f"Triggering manual DAG execution for {business_date}")
    await run_eod_dag(business_date)
    logger.info(f"DAG execution completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run JIP Data Engine DAG")
    parser.add_argument("--date", type=str, help="Business date in YYYY-MM-DD format (defaults to today)")
    args = parser.parse_args()
    
    asyncio.run(run(args.date))
