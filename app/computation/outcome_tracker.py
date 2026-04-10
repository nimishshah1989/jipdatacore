"""Automated outcome tracking for Goldilocks stock ideas."""

from __future__ import annotations

import re
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)


def parse_timeframe(timeframe: str) -> int:
    """Parse timeframe string to maximum number of days.

    Examples:
        "2-6 Weeks" → 42
        "12-18 months" → 540
        "3 months" → 90
        "short term" → 42
    """
    if not timeframe:
        return 90

    tf = timeframe.lower().strip()
    numbers = re.findall(r"\d+", tf)
    if not numbers:
        if "short" in tf:
            return 42
        if "long" in tf:
            return 365
        return 90

    max_num = max(int(n) for n in numbers)

    if "week" in tf:
        return max_num * 7
    if "month" in tf:
        return max_num * 30
    if "year" in tf:
        return max_num * 365
    if "day" in tf:
        return max_num

    return 90


async def track_goldilocks_outcomes(session: AsyncSession) -> dict:
    """Check all active Goldilocks stock ideas against actual prices.

    For each active idea:
    - If high since publish >= target_1 → target_1_hit
    - If high since publish >= target_2 → target_2_hit
    - If low since publish <= stop_loss → sl_hit
    - If timeframe expired → expired

    Returns summary: {"checked": N, "updated": N, "details": [...]}.
    """
    result = await session.execute(text("""
        SELECT id, symbol, published_date, entry_price, entry_zone_low,
               entry_zone_high, target_1, target_2, stop_loss, timeframe, status
        FROM de_goldilocks_stock_ideas
        WHERE status = 'active'
    """))
    active_ideas = result.mappings().all()

    if not active_ideas:
        logger.info("outcome_tracker_no_active_ideas")
        return {"checked": 0, "updated": 0, "details": []}

    updated = 0
    details = []

    for idea in active_ideas:
        symbol = idea["symbol"]
        if not symbol:
            continue

        # Find instrument
        inst = await session.execute(text("""
            SELECT id FROM de_instrument
            WHERE current_symbol = :sym AND is_active = TRUE LIMIT 1
        """), {"sym": symbol})
        inst_row = inst.fetchone()
        if not inst_row:
            continue

        published = idea["published_date"]
        if not published:
            continue

        # Price extremes since published
        prices = await session.execute(text("""
            SELECT MAX(high) AS max_high, MIN(low) AS min_low
            FROM de_equity_ohlcv
            WHERE instrument_id = :iid AND date >= :pub
        """), {"iid": str(inst_row[0]), "pub": published})
        price_row = prices.fetchone()
        if not price_row or price_row[0] is None:
            continue

        # Latest close
        latest = await session.execute(text("""
            SELECT close, date FROM de_equity_ohlcv
            WHERE instrument_id = :iid ORDER BY date DESC LIMIT 1
        """), {"iid": str(inst_row[0])})
        latest_row = latest.fetchone()
        latest_close = latest_row[0] if latest_row else None
        latest_date = latest_row[1] if latest_row else None

        max_high = price_row[0]
        min_low = price_row[1]
        entry = idea["entry_price"] or idea["entry_zone_high"] or idea["entry_zone_low"]
        if not entry:
            continue

        target_1 = idea["target_1"]
        target_2 = idea["target_2"]
        stop_loss = idea["stop_loss"]
        new_status: Optional[str] = None
        was_correct: Optional[bool] = None

        # Check target hits
        if target_2 and max_high >= target_2:
            new_status = "target_2_hit"
            was_correct = True
        elif target_1 and max_high >= target_1:
            new_status = "target_1_hit"
            was_correct = True

        # Check SL (only if no target hit)
        if new_status is None and stop_loss and min_low <= stop_loss:
            new_status = "sl_hit"
            was_correct = False

        # Check expiry
        if new_status is None:
            tf_days = parse_timeframe(idea["timeframe"] or "")
            if (date.today() - published).days > tf_days:
                new_status = "expired"
                was_correct = (latest_close > entry) if latest_close else None

        if new_status:
            actual_move = None
            if latest_close and entry:
                actual_move = ((latest_close - entry) / entry) * 100

            await session.execute(text("""
                UPDATE de_goldilocks_stock_ideas
                SET status = :s, status_updated_at = NOW(), updated_at = NOW()
                WHERE id = :iid
            """), {"s": new_status, "iid": str(idea["id"])})

            await session.execute(text("""
                INSERT INTO de_qual_outcomes
                    (id, extract_id, outcome_date, was_correct, actual_move_pct,
                     entity_ref, notes, recorded_at, created_at, updated_at)
                VALUES (:oid, :eid, :odt, :wc, :amp, :er, :n, NOW(), NOW(), NOW())
            """), {
                "oid": str(uuid.uuid4()),
                "eid": str(idea["id"]),
                "odt": str(latest_date) if latest_date else None,
                "wc": was_correct,
                "amp": str(actual_move) if actual_move is not None else None,
                "er": symbol,
                "n": f"Auto: {new_status}",
            })

            updated += 1
            details.append({"symbol": symbol, "status": new_status, "was_correct": was_correct})
            logger.info("outcome_updated", symbol=symbol, status=new_status, correct=was_correct)

    logger.info("outcome_tracker_done", checked=len(active_ideas), updated=updated)
    return {"checked": len(active_ideas), "updated": updated, "details": details}


async def get_goldilocks_scorecard(session: AsyncSession) -> dict:
    """Compute accuracy scorecard for Goldilocks stock ideas."""
    result = await session.execute(text("""
        SELECT status, COUNT(*) AS cnt
        FROM de_goldilocks_stock_ideas
        GROUP BY status
    """))
    rows = {r[0]: r[1] for r in result.fetchall()}

    total = sum(rows.values())
    active = rows.get("active", 0)
    t1 = rows.get("target_1_hit", 0)
    t2 = rows.get("target_2_hit", 0)
    target_hit = t1 + t2
    sl_hit = rows.get("sl_hit", 0)
    expired = rows.get("expired", 0)
    decided = target_hit + sl_hit

    return {
        "total_ideas": total,
        "active": active,
        "target_hit": target_hit,
        "sl_hit": sl_hit,
        "expired": expired,
        "hit_rate": round(Decimal(str(target_hit)) / Decimal(str(decided)) * 100, 2) if decided > 0 else None,
    }
