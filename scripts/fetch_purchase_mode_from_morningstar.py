"""One-off: fetch PurchaseMode from Morningstar for all active MFs.

IND-C9 — populates ``de_mf_master.purchase_mode`` for the existing universe
so the MF technical-indicators eligibility filter (IND-C10) can run. Uses
the existing ``MorningstarClient`` with its built-in rate limiting and
retry. Daily cap: 10K calls → ~4,234 active funds fit comfortably.

Runs a single datapoint fetch per fund (``PurchaseMode``) via the weekly
pipeline's client. A full fund-master refresh (which fetches 9 datapoints)
is wasteful if you only need the new column.

Usage (from inside the project docker image):

    docker run --rm --add-host host.docker.internal:host-gateway \\
        -e DATABASE_URL=postgresql+asyncpg://... \\
        -e MORNINGSTAR_ACCESS_CODE=... \\
        -e MORNINGSTAR_BASE_URL=... \\
        -e PYTHONPATH=/app \\
        jip-data-engine:<tag> \\
        python scripts/fetch_purchase_mode_from_morningstar.py [--limit N]

Exit: non-zero if the error rate exceeds 5% or the daily cap is hit
before the universe is exhausted.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.logging import get_logger
from app.models.instruments import DeMfMaster
from app.pipelines.morningstar.client import MorningstarClient, RateLimitExceeded

logger = get_logger(__name__)


def _safe_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


async def run(limit: int | None) -> int:
    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url, pool_pre_ping=True)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    # Load universe — active, non-ETF, non-index-fund MFs
    async with SessionLocal() as session:
        stmt = (
            sa.select(DeMfMaster.mstar_id)
            .where(
                DeMfMaster.is_active.is_(True),
                DeMfMaster.is_etf.is_(False),
                DeMfMaster.is_index_fund.is_(False),
            )
            .order_by(DeMfMaster.mstar_id.asc())
        )
        result = await session.execute(stmt)
        mstar_ids = [row[0] for row in result.fetchall()]

    if limit:
        mstar_ids = mstar_ids[:limit]
    print(f"universe: {len(mstar_ids)} mstar_ids", flush=True)

    processed = 0
    updated = 0
    not_found = 0
    rate_limited = False
    started = datetime.now()

    async with MorningstarClient(max_per_second=5, max_per_day=10000) as client:
        for i, mid in enumerate(mstar_ids, 1):
            try:
                data = await client.fetch(
                    id_type="FundId",
                    identifier=mid,
                    datapoints=["PurchaseMode"],
                )
            except RateLimitExceeded:
                rate_limited = True
                print(f"[{i}/{len(mstar_ids)}] DAILY CAP HIT — stopping", flush=True)
                break
            except Exception as exc:
                print(f"[{i}/{len(mstar_ids)}] FAIL {mid}: {exc}", flush=True)
                continue

            processed += 1
            if not data:
                not_found += 1
                continue

            pm = _safe_int(data.get("PurchaseMode"))
            if pm is None:
                continue

            async with SessionLocal() as session:
                await session.execute(
                    sa.update(DeMfMaster)
                    .where(DeMfMaster.mstar_id == mid)
                    .values(purchase_mode=pm, updated_at=sa.func.now())
                )
                await session.commit()
            updated += 1

            if i % 50 == 0 or i == len(mstar_ids):
                elapsed = (datetime.now() - started).total_seconds()
                rate = i / max(elapsed, 1)
                eta = (len(mstar_ids) - i) / max(rate, 0.01)
                print(
                    f"[{i}/{len(mstar_ids)}] processed={processed} updated={updated} "
                    f"not_found={not_found} rate={rate:.1f}/s eta={eta:.0f}s",
                    flush=True,
                )

    print(
        f"\nDONE. processed={processed}, updated={updated}, "
        f"not_found={not_found}, rate_limited={rate_limited}"
    )
    await engine.dispose()
    error_rate = (processed - updated - not_found) / max(processed, 1)
    return 1 if error_rate > 0.05 or rate_limited else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=None, help="Smoke test with N funds")
    args = p.parse_args()
    return asyncio.run(run(args.limit))


if __name__ == "__main__":
    sys.exit(main())
