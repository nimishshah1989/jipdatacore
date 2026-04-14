"""Backfill indicators v2 tables from the full OHLCV history.

Usage:
    python scripts/backfill_indicators_v2.py --asset equity \\
        [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--instrument-id UUID] \\
        [--limit N] [--resume]

Per-instrument streaming: loads one instrument's full OHLCV at a time,
computes indicators, upserts to the v2 table, frees memory, and moves on.
Resumable via the ``backfill_cursor`` table (auto-created on first run).
Per-instrument try/except isolation (Fix 7) — one bad instrument does not
kill the run. Errors are aggregated into reports/backfill_errors_*.md.

Cursor ordering (Fix 8): instruments are iterated ORDER BY id ASC; the
cursor stores the last completed instrument_id as a UUID string so resume
skips everything <= cursor position without duplication.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import pathlib
import sys
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


REPORTS_DIR = pathlib.Path("reports")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asset", required=True, choices=["equity", "etf", "global", "index"], help="Asset class")
    p.add_argument("--from", dest="from_date", type=date.fromisoformat, default=None)
    p.add_argument("--to", dest="to_date", type=date.fromisoformat, default=None)
    p.add_argument(
        "--instrument-id", default=None, help="Run a single instrument only (UUID)"
    )
    p.add_argument(
        "--limit", type=int, default=None, help="Cap instrument count (smoke testing)"
    )
    p.add_argument(
        "--resume", action="store_true", help="Continue from backfill_cursor position"
    )
    return p.parse_args()


async def ensure_cursor_table(session: Any) -> None:
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS backfill_cursor (
            asset_class VARCHAR(20) PRIMARY KEY,
            last_id TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))


async def get_cursor(session: Any, asset: str) -> str | None:
    result = await session.execute(
        text("SELECT last_id FROM backfill_cursor WHERE asset_class = :a"),
        {"a": asset},
    )
    row = result.first()
    return row[0] if row else None


async def set_cursor(session: Any, asset: str, last_id: str) -> None:
    await session.execute(
        text("""
            INSERT INTO backfill_cursor (asset_class, last_id, updated_at)
            VALUES (:a, :id, NOW())
            ON CONFLICT (asset_class) DO UPDATE
                SET last_id = :id, updated_at = NOW()
        """),
        {"a": asset, "id": last_id},
    )


async def run_equity_backfill(args: argparse.Namespace) -> int:
    import uuid

    from app.computation.indicators_v2.assets.equity import (
        compute_equity_indicators,
        load_active_equity_ids,
    )

    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url, pool_pre_ping=True)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as session:
        await ensure_cursor_table(session)
        await session.commit()

        # Build instrument list
        if args.instrument_id:
            ids: list[Any] = [uuid.UUID(args.instrument_id)]
        else:
            ids = await load_active_equity_ids(session)
            print(f"loaded {len(ids)} active equity instruments", flush=True)

            if args.resume:
                cursor = await get_cursor(session, "equity")
                if cursor:
                    cursor_uuid = uuid.UUID(cursor)
                    # Fix 8: instruments ordered by id ASC; skip up to and including cursor
                    ids = [i for i in ids if i > cursor_uuid]
                    print(
                        f"resume: cursor={cursor}, remaining={len(ids)}", flush=True
                    )

            if args.limit:
                ids = ids[: args.limit]

    total_instruments = len(ids)
    total_processed = 0
    total_rows = 0
    total_errors = 0
    errors: list[dict] = []
    started = datetime.now()

    # Fix 7: per-instrument isolation — each gets its own session; errors do not cascade
    for i, iid in enumerate(ids, 1):
        async with SessionLocal() as session:
            try:
                result = await compute_equity_indicators(
                    session,
                    instrument_ids=[iid],
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await session.commit()
                total_processed += result.instruments_processed
                total_rows += result.rows_written
                total_errors += result.instruments_errored
                errors.extend(result.errors)
                # Update cursor after each successful instrument (Fix 8)
                await set_cursor(session, "equity", str(iid))
                await session.commit()
            except Exception as exc:
                total_errors += 1
                errors.append(
                    {
                        "instrument_id": str(iid),
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:500],
                    }
                )
                print(f"[{i}/{total_instruments}] FAIL {iid}: {exc}", flush=True)
                continue

        if i % 10 == 0 or i == total_instruments:
            elapsed = (datetime.now() - started).total_seconds()
            rate = i / max(elapsed, 1)
            eta = (total_instruments - i) / max(rate, 0.01)
            print(
                f"[{i}/{total_instruments}] processed={total_processed} "
                f"rows={total_rows} errors={total_errors} "
                f"rate={rate:.2f}/s eta={eta:.0f}s",
                flush=True,
            )

    # Write error report
    REPORTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"backfill_errors_equity_{ts}.md"
    with open(report_path, "w") as f:
        f.write(
            f"# Backfill errors — equity — {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
        )
        f.write(f"- instruments total: {total_instruments}\n")
        f.write(f"- processed: {total_processed}\n")
        f.write(f"- rows written: {total_rows}\n")
        f.write(f"- errors: {total_errors}\n\n")
        if errors:
            f.write("## Details\n\n")
            for e in errors:
                f.write(
                    f"- `{e['instrument_id']}` {e['error_type']}: {e['error_message']}\n"
                )

    print(
        f"\nDONE. processed={total_processed} rows={total_rows} errors={total_errors}"
    )
    print(f"Error report: {report_path}")

    # Fix 7: exit non-zero if error rate > 0.5% or > 10 absolute
    threshold = max(10, total_instruments // 200)
    await engine.dispose()
    return 1 if total_errors > threshold else 0


async def _run_generic_backfill(
    args: argparse.Namespace,
    asset: str,
    load_ids_fn,
    compute_fn,
    id_cast=str,
) -> int:
    """Shared backfill loop parameterized by per-asset load/compute functions.

    Used for ETF, global, and index. Equity has its own function because
    its ID column is UUID and requires ``uuid.UUID`` coercion for the
    resume cursor.
    """
    db_url = os.environ["DATABASE_URL"]
    engine = create_async_engine(db_url, pool_pre_ping=True)
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as session:
        await ensure_cursor_table(session)
        await session.commit()

        if args.instrument_id:
            ids: list[Any] = [id_cast(args.instrument_id)]
        else:
            ids = await load_ids_fn(session)
            print(f"loaded {len(ids)} {asset} instruments", flush=True)
            if args.resume:
                cursor = await get_cursor(session, asset)
                if cursor:
                    ids = [i for i in ids if str(i) > cursor]
                    print(f"resume: cursor={cursor}, remaining={len(ids)}", flush=True)
            if args.limit:
                ids = ids[: args.limit]

    total_instruments = len(ids)
    total_processed = 0
    total_rows = 0
    total_errors = 0
    errors: list[dict] = []
    started = datetime.now()

    for i, iid in enumerate(ids, 1):
        async with SessionLocal() as session:
            try:
                result = await compute_fn(
                    session,
                    [iid],
                    from_date=args.from_date,
                    to_date=args.to_date,
                )
                await session.commit()
                total_processed += result.instruments_processed
                total_rows += result.rows_written
                total_errors += result.instruments_errored
                errors.extend(result.errors)
                await set_cursor(session, asset, str(iid))
                await session.commit()
            except Exception as exc:
                total_errors += 1
                errors.append(
                    {
                        "instrument_id": str(iid),
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:500],
                    }
                )
                print(f"[{i}/{total_instruments}] FAIL {iid}: {exc}", flush=True)
                continue

        if i % 10 == 0 or i == total_instruments:
            elapsed = (datetime.now() - started).total_seconds()
            rate = i / max(elapsed, 1)
            eta = (total_instruments - i) / max(rate, 0.01)
            print(
                f"[{i}/{total_instruments}] processed={total_processed} "
                f"rows={total_rows} errors={total_errors} "
                f"rate={rate:.2f}/s eta={eta:.0f}s",
                flush=True,
            )

    REPORTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"backfill_errors_{asset}_{ts}.md"
    with open(report_path, "w") as f:
        f.write(f"# Backfill errors — {asset} — {datetime.now():%Y-%m-%d %H:%M:%S}\n\n")
        f.write(f"- instruments total: {total_instruments}\n")
        f.write(f"- processed: {total_processed}\n")
        f.write(f"- rows written: {total_rows}\n")
        f.write(f"- errors: {total_errors}\n\n")
        if errors:
            f.write("## Details\n\n")
            for e in errors:
                f.write(
                    f"- `{e['instrument_id']}` {e['error_type']}: {e['error_message']}\n"
                )

    print(
        f"\nDONE. processed={total_processed} rows={total_rows} errors={total_errors}"
    )
    print(f"Error report: {report_path}")

    threshold = max(10, total_instruments // 200)
    await engine.dispose()
    return 1 if total_errors > threshold else 0


async def run_etf_backfill(args):
    from app.computation.indicators_v2.assets.etf import (
        compute_etf_indicators,
        load_active_etf_tickers,
    )

    async def compute(session, ids, **kw):
        return await compute_etf_indicators(session, tickers=ids, **kw)

    return await _run_generic_backfill(
        args, "etf", load_active_etf_tickers, compute, id_cast=str
    )


async def run_global_backfill(args):
    from app.computation.indicators_v2.assets.global_ import (
        compute_global_indicators,
        load_active_global_tickers,
    )

    async def compute(session, ids, **kw):
        return await compute_global_indicators(session, tickers=ids, **kw)

    return await _run_generic_backfill(
        args, "global", load_active_global_tickers, compute, id_cast=str
    )


async def run_index_backfill(args):
    from app.computation.indicators_v2.assets.index_ import (
        compute_index_indicators,
        load_index_codes,
    )

    async def compute(session, ids, **kw):
        return await compute_index_indicators(session, index_codes=ids, **kw)

    return await _run_generic_backfill(
        args, "index", load_index_codes, compute, id_cast=str
    )


def main() -> int:
    args = parse_args()
    if args.asset == "equity":
        return asyncio.run(run_equity_backfill(args))
    if args.asset == "etf":
        return asyncio.run(run_etf_backfill(args))
    if args.asset == "global":
        return asyncio.run(run_global_backfill(args))
    if args.asset == "index":
        return asyncio.run(run_index_backfill(args))
    raise NotImplementedError(f"asset={args.asset} not wired yet")


if __name__ == "__main__":
    sys.exit(main())
