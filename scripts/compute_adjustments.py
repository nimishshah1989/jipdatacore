"""Corporate action adjustments — compute cumulative adj_factor and backfill close_adj.

Logic:
1. For splits: adj_factor = ratio_from / ratio_to (e.g., 1:10 split → 1/10 = 0.1)
2. For bonuses: adj_factor = ratio_from / (ratio_from + ratio_to) (e.g., 1:1 bonus → 1/2 = 0.5)
3. Build cumulative adjustment chain per instrument, ordered by ex_date DESC
4. close_adj = close * cumulative_adj_factor (all dates BEFORE the corporate action)

Usage:
    cd /app && PYTHONPATH=/app python scripts/compute_adjustments.py
"""

import asyncio
import sys
import time
from collections import defaultdict
from datetime import date
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert


async def main() -> None:
    from app.db.session import async_session_factory

    t_start = time.time()

    async with async_session_factory() as session:
        # =====================================================================
        # STEP 1: Load all corporate actions (splits + bonuses) with ratios
        # =====================================================================
        print("=== LOADING CORPORATE ACTIONS ===")

        r = await session.execute(sa.text(
            "SELECT instrument_id::text, ex_date, action_type, "
            "CAST(ratio_from AS FLOAT) AS ratio_from, "
            "CAST(ratio_to AS FLOAT) AS ratio_to, "
            "CAST(adj_factor AS FLOAT) AS existing_adj "
            "FROM de_corporate_actions "
            "WHERE action_type IN ('split', 'bonus') "
            "AND (ratio_from IS NOT NULL AND ratio_to IS NOT NULL) "
            "ORDER BY instrument_id, ex_date"
        ))
        actions = r.fetchall()
        print(f"  Actions with ratios: {len(actions)} (splits + bonuses)")

        # Group by instrument
        inst_actions: dict[str, list] = defaultdict(list)
        for row in actions:
            inst_actions[row.instrument_id].append({
                "ex_date": row.ex_date,
                "action_type": row.action_type,
                "ratio_from": row.ratio_from,
                "ratio_to": row.ratio_to,
                "existing_adj": row.existing_adj,
            })

        print(f"  Instruments with actions: {len(inst_actions)}")

        # =====================================================================
        # STEP 2: Compute adj_factor for each action
        # =====================================================================
        print("\n=== COMPUTING ADJUSTMENT FACTORS ===")

        adj_by_instrument: dict[str, list[tuple[date, float]]] = {}

        for iid, acts in inst_actions.items():
            factors = []
            for a in acts:
                rf = a["ratio_from"]
                rt = a["ratio_to"]

                if a["existing_adj"] is not None and a["existing_adj"] > 0:
                    # Use pre-computed adj_factor if available
                    factor = a["existing_adj"]
                elif a["action_type"] == "split":
                    # Split: 1:10 means each share becomes 10, price divides by 10
                    # adj_factor = old/new = ratio_from / ratio_to
                    if rt > 0:
                        factor = rf / rt
                    else:
                        continue
                elif a["action_type"] == "bonus":
                    # Bonus: 1:1 means 1 free for every 1 held, price halves
                    # adj_factor = old / (old + new) = ratio_from / (ratio_from + ratio_to)
                    if rf + rt > 0:
                        factor = rf / (rf + rt)
                    else:
                        continue
                else:
                    continue

                if factor > 0 and factor != 1.0:
                    factors.append((a["ex_date"], factor))

            if factors:
                # Sort by ex_date ascending
                factors.sort(key=lambda x: x[0])
                adj_by_instrument[iid] = factors

        total_factors = sum(len(f) for f in adj_by_instrument.values())
        print(f"  Valid adjustment factors: {total_factors} across {len(adj_by_instrument)} instruments")

        # Sample
        for iid, factors in list(adj_by_instrument.items())[:3]:
            print(f"    {iid[:8]}...: {len(factors)} actions")
            for ex_date, factor in factors[:3]:
                print(f"      {ex_date}: factor={factor:.4f}")

        # =====================================================================
        # STEP 3: Build cumulative adjustment and update close_adj
        # =====================================================================
        print("\n=== BUILDING CUMULATIVE ADJUSTMENTS ===")

        # For each instrument:
        # - Start from the most recent action, work backwards
        # - cumulative_factor starts at 1.0 (most recent prices are unadjusted)
        # - For each action going back in time, multiply by that action's factor
        # - All prices BEFORE that ex_date get multiplied by cumulative_factor

        total_updated = 0
        instruments_processed = 0

        for iid, factors in adj_by_instrument.items():
            # Sort factors by ex_date descending (most recent first)
            factors_desc = sorted(factors, key=lambda x: x[0], reverse=True)

            # Build cumulative factor for each date range
            # After the last action: factor = 1.0 (no adjustment)
            # Between action N and N-1: factor = product of all actions from N onwards
            cum_factor = 1.0
            date_ranges: list[tuple[date, float]] = []

            for ex_date, factor in factors_desc:
                cum_factor *= factor
                date_ranges.append((ex_date, cum_factor))

            # Update de_equity_ohlcv: for each date range, set close_adj = close * cum_factor
            for ex_date, cum in date_ranges:
                r = await session.execute(sa.text(
                    "UPDATE de_equity_ohlcv SET "
                    "close_adj = CAST(close AS NUMERIC(18,4)) * :factor, "
                    "open_adj = CAST(open AS NUMERIC(18,4)) * :factor, "
                    "high_adj = CAST(high AS NUMERIC(18,4)) * :factor, "
                    "low_adj = CAST(low AS NUMERIC(18,4)) * :factor "
                    "WHERE instrument_id = :iid::uuid AND date < :ex_date"
                ), {
                    "factor": Decimal(str(round(cum, 6))),
                    "iid": iid,
                    "ex_date": ex_date,
                })
                total_updated += r.rowcount

            # Set close_adj = close for dates ON or AFTER the last action (no adjustment needed)
            last_ex = factors_desc[0][0]
            r = await session.execute(sa.text(
                "UPDATE de_equity_ohlcv SET "
                "close_adj = close, open_adj = open, high_adj = high, low_adj = low "
                "WHERE instrument_id = :iid::uuid AND date >= :ex_date AND close_adj IS NULL"
            ), {"iid": iid, "ex_date": last_ex})
            total_updated += r.rowcount

            instruments_processed += 1
            if instruments_processed % 50 == 0:
                await session.flush()
                print(f"  Processed {instruments_processed}/{len(adj_by_instrument)} instruments, {total_updated:,} rows updated")
                sys.stdout.flush()

        # Set close_adj = close for all instruments WITHOUT any corporate actions
        print("\n=== FILLING UNADJUSTED STOCKS ===")
        r = await session.execute(sa.text(
            "UPDATE de_equity_ohlcv SET "
            "close_adj = close, open_adj = open, high_adj = high, low_adj = low "
            "WHERE close_adj IS NULL"
        ))
        unadj_count = r.rowcount
        print(f"  Set close_adj = close for {unadj_count:,} unadjusted rows")

        total_updated += unadj_count

        await session.commit()

        # Verify
        r = await session.execute(sa.text(
            "SELECT COUNT(*) FROM de_equity_ohlcv WHERE close_adj IS NULL"
        ))
        still_null = r.scalar_one()

        r = await session.execute(sa.text(
            "SELECT COUNT(*) FROM de_equity_ohlcv WHERE close_adj != close"
        ))
        actually_adjusted = r.scalar_one()

        print(f"\n=== COMPLETE in {time.time()-t_start:.0f}s ===")
        print(f"  Total rows updated: {total_updated:,}")
        print(f"  Rows still NULL: {still_null}")
        print(f"  Rows where close_adj != close (actually adjusted): {actually_adjusted:,}")
        print(f"  Instruments with adjustments: {len(adj_by_instrument)}")


if __name__ == "__main__":
    asyncio.run(main())
