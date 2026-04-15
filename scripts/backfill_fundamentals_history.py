"""Backfill de_equity_fundamentals_history from screener.in.

Fetches the same HTML pages as GAP-08's snapshot pipeline, but only
writes to the history table. Safe to re-run (ON CONFLICT upsert).
"""

import asyncio
import os
import sys
from decimal import Decimal, InvalidOperation
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.session import async_session_factory
from app.models.fundamentals_history import DeEquityFundamentalsHistory
from app.models.instruments import DeInstrument
from app.pipelines.fundamentals.screener_fetcher import (
    build_http_client,
    extract_fundamentals_history,
    fetch_company_html,
)

DELAY_SECONDS = 1.2
MAX_CONSECUTIVE_FAILURES = 5


def _to_decimal(val, precision: int = 2) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return round(Decimal(str(val)), precision)
    except (InvalidOperation, ValueError, TypeError):
        return None


async def backfill():
    cookie = os.environ.get("SCREENER_SESSION_COOKIE", "")

    async with async_session_factory() as session:
        # Check how many instruments already have history
        existing_count = await session.scalar(
            select(func.count(func.distinct(DeEquityFundamentalsHistory.instrument_id)))
        )
        print(f"Existing instruments with history: {existing_count}")

        result = await session.execute(
            select(DeInstrument.id, DeInstrument.current_symbol)
            .where(DeInstrument.is_active.is_(True), DeInstrument.exchange == "NSE")
            .order_by(DeInstrument.current_symbol)
        )
        instruments = [{"id": row.id, "symbol": row.current_symbol} for row in result.all()]
        print(f"Universe: {len(instruments)} active NSE instruments")

        # Skip instruments that already have history (for resumability)
        if existing_count and existing_count > 0:
            existing_ids_result = await session.execute(
                select(func.distinct(DeEquityFundamentalsHistory.instrument_id))
            )
            existing_ids = {row[0] for row in existing_ids_result.all()}
            remaining = [i for i in instruments if i["id"] not in existing_ids]
            print(f"Skipping {len(existing_ids)} already-backfilled, {len(remaining)} remaining")
            instruments = remaining

        rows_ok = 0
        rows_fail = 0
        consecutive_failures = 0
        history_rows_total = 0

        async with build_http_client(cookie) as client:
            for i, inst in enumerate(instruments):
                symbol = inst["symbol"]
                instrument_id = inst["id"]

                html = await fetch_company_html(client, symbol)

                if html is None:
                    consecutive_failures += 1
                    rows_fail += 1
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                        print(f"ABORT: {MAX_CONSECUTIVE_FAILURES} consecutive failures — cookie expired?")
                        break
                    await asyncio.sleep(DELAY_SECONDS)
                    continue

                consecutive_failures = 0

                try:
                    history_rows = extract_fundamentals_history(html)
                    async with session.begin_nested():
                        for row in history_rows:
                            row["instrument_id"] = instrument_id
                            for field in list(row.keys()):
                                if field in ("instrument_id", "fiscal_period_end", "period_type", "source"):
                                    continue
                                row[field] = _to_decimal(
                                    row.get(field),
                                    4 if field in ("opm_pct", "tax_pct", "eps") else 2,
                                )
                            row["source"] = "screener"
                            stmt = pg_insert(DeEquityFundamentalsHistory).values(**row)
                            update_cols = {
                                c: stmt.excluded[c]
                                for c in row
                                if c not in ("instrument_id", "fiscal_period_end", "period_type", "created_at")
                            }
                            stmt = stmt.on_conflict_do_update(
                                constraint=DeEquityFundamentalsHistory.__table__.primary_key,
                                set_=update_cols,
                            )
                            await session.execute(stmt)

                    history_rows_total += len(history_rows)
                    rows_ok += 1
                except Exception as e:
                    print(f"FAIL {symbol}: {str(e)[:100]}")
                    rows_fail += 1

                await asyncio.sleep(DELAY_SECONDS)

                if (i + 1) % 50 == 0:
                    await session.commit()
                    print(
                        f"Progress: {i+1}/{len(instruments)} instruments, "
                        f"{history_rows_total} history rows, {rows_fail} failures"
                    )

        await session.commit()
        print(f"\nDONE: {rows_ok} instruments OK, {rows_fail} failed, {history_rows_total} total history rows")


if __name__ == "__main__":
    asyncio.run(backfill())
