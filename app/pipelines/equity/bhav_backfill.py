"""15-year BHAV copy backfill — downloads NSE BHAV files and ingests into RDS.

Features:
  - PARALLEL workers (5 concurrent, split by year) — ~30 min for full 15yr backfill
  - Resume from last checkpoint (queries de_source_files for completed dates)
  - Built-in HTTP monitoring server on port 8098
  - Rate-limited NSE downloads (shared semaphore across workers)
  - Direct RDS insertion via DATABASE_URL from .env
  - Handles all 3 BHAV formats (Pre-2010, Standard, UDiFF)

Usage:
    python -m app.pipelines.equity.bhav_backfill
    python -m app.pipelines.equity.bhav_backfill --start-date 2011-04-01 --end-date 2026-04-04
    python -m app.pipelines.equity.bhav_backfill --workers 5
    python -m app.pipelines.equity.bhav_backfill --force  # re-download even if already ingested

Monitor progress at http://localhost:8098
"""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Reuse existing pipeline components
from app.pipelines.equity.bhav import (
    BhavFormat,
    NSE_BHAV_URL_PRE2010,
    NSE_BHAV_URL_STANDARD,
    NSE_BHAV_URL_UDIFF,
    NSE_HEADERS,
    UDIFF_START_DATE,
    _compute_checksum,
    _extract_zip_csv,
    detect_bhav_format,
    parse_bhav_csv,
)
from app.models.instruments import DeInstrument
from app.models.pipeline import DeSourceFiles
from app.models.prices import DeEquityOhlcv

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_WORKERS = 5  # concurrent download workers
DELAY_BETWEEN_REQUESTS = 1.5  # seconds — shared across all workers via semaphore
DOWNLOAD_TIMEOUT = 60.0
DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_BASE = 3.0
MONITOR_PORT = 8098
MIN_ROW_COUNT_BACKFILL = 100
MAX_CONSECUTIVE_FAILURES_PER_WORKER = 15

# Checkpoint file for instant resume (no DB query needed on restart)
CHECKPOINT_FILE = Path(__file__).parent.parent.parent.parent / "bhav_backfill_checkpoint.json"

# ---------------------------------------------------------------------------
# Global progress state (thread-safe via GIL for simple dict updates)
# ---------------------------------------------------------------------------
_progress_lock = threading.Lock()
_progress: dict[str, Any] = {
    "status": "initializing",
    "started_at": None,
    "total_dates": 0,
    "completed_dates": 0,
    "skipped_dates": 0,
    "failed_dates": 0,
    "current_date": None,
    "last_completed_date": None,
    "last_error": None,
    "total_rows_inserted": 0,
    "dates_per_minute": 0.0,
    "eta_minutes": None,
    "failed_date_list": [],
    "phase": "querying existing data",
    "workers": {},
}


# ---------------------------------------------------------------------------
# Checkpoint file — instant resume without DB query
# ---------------------------------------------------------------------------
def _load_checkpoint() -> set[str]:
    """Load completed dates from checkpoint file. Returns set of ISO date strings."""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        return set(data.get("completed_dates", []))
    except (json.JSONDecodeError, KeyError):
        return set()


def _save_checkpoint(completed_date: date) -> None:
    """Append a completed date to the checkpoint file (thread-safe)."""
    with _progress_lock:
        dates = _load_checkpoint()
        dates.add(completed_date.isoformat())
        CHECKPOINT_FILE.write_text(json.dumps({
            "completed_dates": sorted(dates),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "count": len(dates),
        }, indent=2))


def _update_progress(**kwargs: Any) -> None:
    """Update global progress and recalculate speed/ETA."""
    with _progress_lock:
        _progress.update(kwargs)
        if _progress["started_at"] and _progress["completed_dates"] > 0:
            elapsed = time.time() - _progress["started_at"]
            rate = _progress["completed_dates"] / (elapsed / 60) if elapsed > 0 else 0
            _progress["dates_per_minute"] = round(rate, 2)
            remaining = (
                _progress["total_dates"]
                - _progress["completed_dates"]
                - _progress["skipped_dates"]
                - _progress["failed_dates"]
            )
            _progress["eta_minutes"] = round(remaining / rate, 1) if rate > 0 else None


def _update_worker(worker_id: str, **kwargs: Any) -> None:
    """Update per-worker progress."""
    with _progress_lock:
        if "workers" not in _progress:
            _progress["workers"] = {}
        if worker_id not in _progress["workers"]:
            _progress["workers"][worker_id] = {}
        _progress["workers"][worker_id].update(kwargs)


# ---------------------------------------------------------------------------
# Atomic counters for thread-safe increment from async tasks
# ---------------------------------------------------------------------------
_completed_counter = 0
_failed_counter = 0
_rows_counter = 0
_counter_lock = threading.Lock()


def _inc_completed(rows: int) -> int:
    global _completed_counter, _rows_counter
    with _counter_lock:
        _completed_counter += 1
        _rows_counter += rows
        return _completed_counter


def _inc_failed() -> int:
    global _failed_counter
    with _counter_lock:
        _failed_counter += 1
        return _failed_counter


# ---------------------------------------------------------------------------
# Monitor HTTP server (runs in background thread)
# ---------------------------------------------------------------------------
_MONITOR_HTML_PATH = Path(__file__).parent.parent.parent.parent / "dashboard" / "bhav_backfill_monitor.html"


class MonitorHandler(SimpleHTTPRequestHandler):
    """Tiny HTTP handler serving progress JSON and the monitor HTML page."""

    def do_GET(self) -> None:
        if self.path == "/api/progress":
            with _progress_lock:
                body = json.dumps(_progress, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/" or self.path == "/index.html":
            if _MONITOR_HTML_PATH.exists():
                body = _MONITOR_HTML_PATH.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                body = b"<html><body><h1>Monitor HTML not found</h1><p>Check /api/progress for JSON</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        else:
            self.send_error(404)

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Suppress access logs


def _start_monitor_server() -> None:
    """Start the monitoring HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", MONITOR_PORT), MonitorHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("monitor_server_started", port=MONITOR_PORT, url=f"http://localhost:{MONITOR_PORT}")


# ---------------------------------------------------------------------------
# Download helper with retries
# ---------------------------------------------------------------------------
async def _download_with_retry(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    retries: int = DOWNLOAD_RETRIES,
) -> bytes:
    """Download URL with exponential backoff, respecting rate-limit semaphore."""
    last_exc: Exception | None = None
    for attempt in range(retries):
        async with semaphore:
            try:
                resp = await client.get(url, follow_redirects=True)
                resp.raise_for_status()
                # Delay AFTER download to space out requests globally
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
                return resp.content
            except (httpx.HTTPError, httpx.TimeoutException) as exc:
                last_exc = exc
                if attempt < retries - 1:
                    wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                    await asyncio.sleep(wait)
    raise last_exc or RuntimeError(f"Failed after {retries} retries: {url}")


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def _generate_weekdays(start: date, end: date) -> list[date]:
    """Generate all weekdays (Mon-Fri) between start and end inclusive."""
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _split_into_chunks(dates: list[date], n_workers: int) -> list[list[date]]:
    """Split dates into n roughly equal chunks for parallel workers."""
    if not dates:
        return []
    chunk_size = max(1, len(dates) // n_workers)
    chunks: list[list[date]] = []
    for i in range(0, len(dates), chunk_size):
        chunks.append(dates[i : i + chunk_size])
    # If we ended up with more chunks than workers, merge the last two
    while len(chunks) > n_workers and len(chunks) > 1:
        last = chunks.pop()
        chunks[-1].extend(last)
    return chunks


async def _load_completed_dates(session: AsyncSession) -> set[date]:
    """Query de_source_files for dates already ingested for nse_bhav."""
    result = await session.execute(
        select(DeSourceFiles.file_date).where(
            DeSourceFiles.source_name == "nse_bhav",
            DeSourceFiles.file_date.isnot(None),
        )
    )
    return {row[0] for row in result.fetchall() if row[0] is not None}


async def _load_symbol_map(session: AsyncSession) -> dict[str, uuid.UUID]:
    """Load every known ticker -> instrument_id mapping.

    Includes BOTH `de_instrument.current_symbol` AND every historical
    `de_symbol_history.old_symbol`. Without the historical entries, BHAV
    rows for a stock that was later renamed (the original ticker still
    appears in old BHAV files) get silently dropped at ingestion time --
    that was the cause of the partial pre-rename history we saw in the
    Atlas-M0 universe coverage audit.
    """
    # Current symbols
    current = await session.execute(
        select(DeInstrument.current_symbol, DeInstrument.id).where(
            DeInstrument.is_active == True,  # noqa: E712
        )
    )
    mapping: dict[str, uuid.UUID] = {row[0].upper(): row[1] for row in current}

    # Historical aliases (old_symbol from de_symbol_history). When a
    # historical alias collides with a current symbol owned by a different
    # instrument, the current_symbol wins (we processed it first above).
    from app.models.instruments import DeSymbolHistory

    history = await session.execute(
        select(DeSymbolHistory.old_symbol, DeSymbolHistory.instrument_id)
    )
    for old_symbol, instrument_id in history:
        if not old_symbol:
            continue
        key = old_symbol.upper()
        mapping.setdefault(key, instrument_id)

    return mapping


NSE_BHAV_URL_HISTORICAL_ZIP = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/"
    "{year}/{month}/cm{dd}{month}{yyyy}bhav.csv.zip"
)

# sec_bhavdata_full format only works from 2020 onwards; older dates need
# the historical zip URL  (cm{DD}{MON}{YYYY}bhav.csv.zip)
STANDARD_CSV_START_DATE = date(2020, 1, 1)


def _bhav_url_for_date(d: date) -> tuple[str, BhavFormat]:
    """Return (download_url, expected_format) for a given date."""
    if d >= UDIFF_START_DATE:
        url = NSE_BHAV_URL_UDIFF.format(date_str=d.strftime("%Y%m%d"))
        return url, BhavFormat.UDIFF
    elif d >= STANDARD_CSV_START_DATE:
        url = NSE_BHAV_URL_STANDARD.format(date_str=d.strftime("%d%m%Y"))
        return url, BhavFormat.STANDARD
    elif d.year < 2010:
        url = NSE_BHAV_URL_PRE2010.format(date_str=d.strftime("%d%m%Y"))
        return url, BhavFormat.PRE2010
    else:
        # 2010-2019: historical zip format
        url = NSE_BHAV_URL_HISTORICAL_ZIP.format(
            year=d.year,
            month=d.strftime("%b").upper(),
            dd=d.strftime("%d"),
            yyyy=d.year,
        )
        return url, BhavFormat.STANDARD


async def _ingest_single_date(
    client: httpx.AsyncClient,
    session: AsyncSession,
    business_date: date,
    symbol_map: dict[str, uuid.UUID],
    semaphore: asyncio.Semaphore,
) -> tuple[int, int]:
    """Download and ingest BHAV for one date. Returns (rows_inserted, rows_failed)."""
    url, expected_fmt = _bhav_url_for_date(business_date)

    # Download (rate-limited via semaphore)
    raw_bytes = await _download_with_retry(client, url, semaphore)
    checksum = _compute_checksum(raw_bytes)

    # Extract CSV
    if url.endswith(".zip"):
        csv_text = _extract_zip_csv(raw_bytes)
    else:
        csv_text = raw_bytes.decode("utf-8", errors="replace")

    # Detect format
    first_line = csv_text.strip().splitlines()[0] if csv_text.strip() else ""
    detected_fmt = detect_bhav_format(first_line)

    # Parse
    parsed_rows = parse_bhav_csv(csv_text, detected_fmt)
    if len(parsed_rows) < MIN_ROW_COUNT_BACKFILL:
        raise ValueError(
            f"Only {len(parsed_rows)} rows for {business_date} (min {MIN_ROW_COUNT_BACKFILL})"
        )

    # Register source file
    source_file_id = uuid.uuid4()
    sf_stmt = pg_insert(DeSourceFiles).values(
        id=source_file_id,
        source_name="nse_bhav",
        file_name=url.split("/")[-1],
        file_date=business_date,
        checksum=checksum,
        file_size_bytes=len(raw_bytes),
        row_count=len(parsed_rows),
        format_version=detected_fmt.value,
    )
    sf_stmt = sf_stmt.on_conflict_do_update(
        constraint="uq_source_files_dedup",
        set_={
            "file_name": sf_stmt.excluded.file_name,
            "file_size_bytes": sf_stmt.excluded.file_size_bytes,
            "row_count": sf_stmt.excluded.row_count,
            "format_version": sf_stmt.excluded.format_version,
        },
    ).returning(DeSourceFiles.id)
    result = await session.execute(sf_stmt)
    source_file_id = result.scalar_one()  # Get actual ID (new or existing)

    # Build insert rows — deduplicate by (date, instrument_id) to avoid
    # CardinalityViolationError. BHAV files contain multiple series (EQ, BE, BZ)
    # for same stock; we keep the first EQ row (or the first row if no EQ).
    rows_failed = 0
    seen: dict[tuple[date, uuid.UUID], dict[str, Any]] = {}

    for row in parsed_rows:
        symbol = row["symbol"]
        instrument_id = symbol_map.get(symbol)
        if instrument_id is None:
            rows_failed += 1
            continue

        row_date = row["date"] or business_date
        key = (row_date, instrument_id)

        # Prefer EQ series rows over others
        if key in seen:
            if row["series"] == "EQ" and seen[key].get("_series") != "EQ":
                pass  # overwrite below
            else:
                continue  # skip duplicate

        seen[key] = {
            "date": row_date,
            "instrument_id": instrument_id,
            "symbol": symbol,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "trades": row["trades"],
            "data_status": "raw",
            "source_file_id": source_file_id,
            "_series": row["series"],  # internal, stripped before insert
        }

    insert_rows: list[dict[str, Any]] = [
        {k: v for k, v in r.items() if k != "_series"} for r in seen.values()
    ]

    # Batch upsert in chunks of 500
    rows_inserted = 0
    for i in range(0, len(insert_rows), 500):
        chunk = insert_rows[i : i + 500]
        stmt = pg_insert(DeEquityOhlcv).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["date", "instrument_id"],
            set_={
                "symbol": stmt.excluded.symbol,
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
                "trades": stmt.excluded.trades,
                "source_file_id": stmt.excluded.source_file_id,
            },
        )
        await session.execute(stmt)
        rows_inserted += len(chunk)

    return rows_inserted, rows_failed


# ---------------------------------------------------------------------------
# Worker coroutine — each worker processes its assigned date chunk
# ---------------------------------------------------------------------------
async def _worker(
    worker_id: str,
    dates: list[date],
    async_session: async_sessionmaker,
    symbol_map: dict[str, uuid.UUID],
    semaphore: asyncio.Semaphore,
    total_dates: int,
    skipped_count: int,
) -> tuple[int, int, int]:
    """Process a chunk of dates. Returns (completed, failed, rows_inserted)."""
    completed = 0
    failed = 0
    total_rows = 0
    consecutive_failures = 0
    failed_dates: list[str] = []

    _update_worker(worker_id, status="running", total=len(dates), completed=0, failed=0)

    async with httpx.AsyncClient(headers=NSE_HEADERS, timeout=DOWNLOAD_TIMEOUT) as client:
        for business_date in dates:
            _update_worker(worker_id, current_date=business_date.isoformat())

            try:
                async with async_session() as session:
                    async with session.begin():
                        rows_inserted, rows_failed = await _ingest_single_date(
                            client, session, business_date, symbol_map, semaphore,
                        )

                completed += 1
                total_rows += rows_inserted
                consecutive_failures = 0

                # Update global counters
                global_completed = _inc_completed(rows_inserted)
                _save_checkpoint(business_date)  # Instant resume on restart
                _update_progress(
                    completed_dates=global_completed,
                    total_rows_inserted=_rows_counter,
                    last_completed_date=business_date.isoformat(),
                )
                _update_worker(worker_id, completed=completed, last_date=business_date.isoformat())

                logger.info(
                    "date_ingested",
                    worker=worker_id,
                    date=business_date.isoformat(),
                    rows=rows_inserted,
                    progress=f"{global_completed + skipped_count}/{total_dates}",
                )

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    # 404 = non-trading day (holiday). Not a real failure.
                    logger.debug("date_skipped_holiday", worker=worker_id, date=business_date.isoformat())
                    completed += 1
                    global_completed = _inc_completed(0)
                    _save_checkpoint(business_date)  # Save so we skip on restart
                    _update_progress(completed_dates=global_completed)
                    _update_worker(worker_id, completed=completed)
                    consecutive_failures = 0
                    continue

                failed += 1
                consecutive_failures += 1
                global_failed = _inc_failed()
                error_msg = f"HTTP {e.response.status_code} for {business_date}"
                failed_dates.append(f"{business_date} (HTTP {e.response.status_code})")

                _update_progress(
                    failed_dates=global_failed,
                    last_error=error_msg,
                    failed_date_list=failed_dates[-50:],
                )
                _update_worker(worker_id, failed=failed, last_error=error_msg)
                logger.warning(
                    "date_failed_http", worker=worker_id,
                    date=business_date.isoformat(), status=e.response.status_code,
                )

                if e.response.status_code in (403, 429):
                    await asyncio.sleep(30)

            except Exception as e:
                failed += 1
                consecutive_failures += 1
                global_failed = _inc_failed()
                error_msg = f"{business_date}: {str(e)[:200]}"
                failed_dates.append(error_msg)

                _update_progress(
                    failed_dates=global_failed,
                    last_error=error_msg,
                    failed_date_list=failed_dates[-50:],
                )
                _update_worker(worker_id, failed=failed, last_error=error_msg)
                logger.warning("date_failed", worker=worker_id, date=business_date.isoformat(), error=str(e)[:200])

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES_PER_WORKER:
                logger.error(
                    "worker_stopped_consecutive_failures",
                    worker=worker_id,
                    consecutive=consecutive_failures,
                )
                _update_worker(worker_id, status="stopped", reason="too many consecutive failures")
                break

    _update_worker(worker_id, status="done", current_date=None)
    return completed, failed, total_rows


# ---------------------------------------------------------------------------
# Main backfill orchestrator
# ---------------------------------------------------------------------------
async def run_backfill(
    start_date: date,
    end_date: date,
    force: bool = False,
    n_workers: int = DEFAULT_WORKERS,
) -> None:
    """Main backfill entry point — spawns parallel workers."""
    # Load DB URL
    try:
        from app.config import get_settings
        db_url = get_settings().database_url
    except Exception:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.environ["DATABASE_URL"]

    engine = create_async_engine(
        db_url,
        pool_size=n_workers + 5,
        max_overflow=n_workers + 5,
        pool_pre_ping=True,
    )
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    # Generate all target weekdays
    all_weekdays = _generate_weekdays(start_date, end_date)

    # Load checkpoint file for instant resume (no slow DB query)
    _update_progress(phase="loading checkpoint", status="initializing")
    checkpoint_dates = _load_checkpoint()

    # Also query DB for dates not in checkpoint (first run or checkpoint lost)
    _update_progress(phase="querying existing data")
    async with async_session() as session:
        db_completed = await _load_completed_dates(session)
        symbol_map = await _load_symbol_map(session)

    # Merge both sources
    completed_dates = {date.fromisoformat(d) for d in checkpoint_dates} | db_completed

    logger.info(
        "backfill_init",
        total_weekdays=len(all_weekdays),
        already_completed=len(completed_dates),
        instruments=len(symbol_map),
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        workers=n_workers,
    )

    if not symbol_map:
        logger.error("no_instruments_found", hint="Run master_refresh first")
        _update_progress(status="error", last_error="No instruments in de_instrument. Run master_refresh first.")
        await engine.dispose()
        return

    # Filter to pending dates
    if force:
        pending_dates = all_weekdays
    else:
        pending_dates = [d for d in all_weekdays if d not in completed_dates]

    skipped_count = len(all_weekdays) - len(pending_dates)

    if not pending_dates:
        logger.info("backfill_nothing_to_do", skipped=skipped_count)
        _update_progress(
            status="completed",
            total_dates=len(all_weekdays),
            skipped_dates=skipped_count,
            phase=f"Nothing to do — all {skipped_count} dates already ingested",
        )
        await engine.dispose()
        return

    _update_progress(
        status="running",
        started_at=time.time(),
        total_dates=len(all_weekdays),
        completed_dates=0,
        skipped_dates=skipped_count,
        failed_dates=0,
        phase=f"ingesting with {n_workers} parallel workers",
    )

    # Split pending dates into chunks for workers
    chunks = _split_into_chunks(pending_dates, n_workers)
    actual_workers = len(chunks)

    logger.info(
        "backfill_starting",
        pending=len(pending_dates),
        skipped=skipped_count,
        workers=actual_workers,
        chunk_sizes=[len(c) for c in chunks],
    )

    # Shared semaphore limits concurrent NSE requests across all workers
    # Allow 2 concurrent downloads — the delay inside ensures spacing
    semaphore = asyncio.Semaphore(2)

    # Launch workers
    tasks = []
    for i, chunk in enumerate(chunks):
        worker_id = f"W{i+1} ({chunk[0].year}-{chunk[-1].year})"
        tasks.append(
            _worker(
                worker_id=worker_id,
                dates=chunk,
                async_session=async_session,
                symbol_map=symbol_map,
                semaphore=semaphore,
                total_dates=len(all_weekdays),
                skipped_count=skipped_count,
            )
        )

    # Run all workers concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate results
    total_completed = 0
    total_failed = 0
    total_rows = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error("worker_crashed", error=str(r))
            total_failed += 1
        else:
            c, f, rows = r
            total_completed += c
            total_failed += f
            total_rows += rows

    # Final status
    final_status = "completed" if total_failed == 0 else "completed_with_errors"
    _update_progress(
        status=final_status,
        completed_dates=total_completed,
        failed_dates=total_failed,
        total_rows_inserted=total_rows,
        phase=f"Done — {total_completed} ingested, {total_failed} failed, {skipped_count} already existed",
        current_date=None,
    )

    logger.info(
        "backfill_complete",
        completed=total_completed,
        failed=total_failed,
        skipped=skipped_count,
        total_rows=total_rows,
    )

    await engine.dispose()

    # Keep server alive so user can check final status
    logger.info("backfill_done_monitor_still_running", url=f"http://localhost:{MONITOR_PORT}")
    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        pass


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="BHAV Copy 15-Year Backfill (Parallel)")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2011, 4, 1),
        help="Start date (default: 2011-04-01, 15 years back)",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="End date (default: yesterday)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite even if date already ingested",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Disable the built-in HTTP monitoring server",
    )
    args = parser.parse_args()

    # Configure structlog
    import logging
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Start monitor server
    if not args.no_monitor:
        _start_monitor_server()
        print(f"\n  Monitor: http://localhost:{MONITOR_PORT}\n")

    print(f"  Backfill range: {args.start_date} -> {args.end_date}")
    print(f"  Workers: {args.workers}")
    print(f"  Force re-download: {args.force}\n")

    asyncio.run(run_backfill(args.start_date, args.end_date, args.force, args.workers))


if __name__ == "__main__":
    main()
