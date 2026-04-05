"""CLI entry point for running data migrations.

Usage (from project root):
  python -m app.migrations.runner --migration equity_ohlcv
  python -m app.migrations.runner --migration all
  python -m app.migrations.runner --migration mf_nav --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Type

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings
from app.logging import get_logger, setup_logging
from app.migrations.base import BaseMigration, MigrationResult
from app.migrations.equity_ohlcv import EquityOhlcvMigration
from app.migrations.index_constituents import IndexConstituentsMigration
from app.migrations.mf_holdings import MfHoldingsMigration
from app.migrations.mf_master import MfMasterMigration
from app.migrations.mf_nav import MfNavMigration

logger = get_logger(__name__)

MIGRATIONS: dict[str, Type[BaseMigration]] = {
    "equity_ohlcv": EquityOhlcvMigration,
    "mf_master": MfMasterMigration,
    "mf_nav": MfNavMigration,
    "mf_holdings": MfHoldingsMigration,
    "index_constituents": IndexConstituentsMigration,
}

# Execution order matters: mf_master must run before mf_nav and mf_holdings
MIGRATION_ORDER = [
    "mf_master",
    "equity_ohlcv",
    "mf_nav",
    "mf_holdings",
    "index_constituents",
]


async def run_migration(
    name: str,
    dry_run: bool = False,
) -> MigrationResult:
    """Execute a single named migration against the target database.

    Args:
        name: Migration name from MIGRATIONS registry.
        dry_run: If True, log what would happen but do not commit.

    Returns:
        MigrationResult with counts and status.
    """
    migration_cls = MIGRATIONS[name]
    migration = migration_cls()

    settings = get_settings()
    engine = create_async_engine(
        settings.database_url,
        echo=False,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with session_factory() as session:
            if dry_run:
                await logger.ainfo(
                    "dry_run_mode",
                    migration=name,
                    source_db=migration.source_db_name,
                    source_table=migration.source_table,
                    target_table=migration.target_table,
                    batch_size=migration.batch_size,
                )
                return MigrationResult(
                    source_db=migration.source_db_name,
                    source_table=migration.source_table,
                    target_table=migration.target_table,
                    rows_read=0,
                    rows_written=0,
                    rows_errored=0,
                    status="dry_run",
                    duration_seconds=0.0,
                )

            async with session.begin():
                result = await migration.run(session)

            return result
    finally:
        await engine.dispose()


async def run_all_migrations(dry_run: bool = False) -> list[MigrationResult]:
    """Run all migrations in dependency order."""
    results: list[MigrationResult] = []
    for name in MIGRATION_ORDER:
        await logger.ainfo("starting_migration", name=name, dry_run=dry_run)
        result = await run_migration(name, dry_run=dry_run)
        results.append(result)
        await logger.ainfo(
            "migration_done",
            name=name,
            status=result.status,
            rows_read=result.rows_read,
            rows_written=result.rows_written,
            rows_errored=result.rows_errored,
            duration_seconds=round(result.duration_seconds, 2),
        )
        if result.status == "failed":
            await logger.aerror("migration_failed_stopping", name=name)
            break
    return results


def main() -> None:
    """CLI entry point."""
    setup_logging()

    parser = argparse.ArgumentParser(description="Run data migrations for JIP Data Engine")
    parser.add_argument(
        "--migration",
        required=True,
        choices=list(MIGRATIONS.keys()) + ["all"],
        help="Migration to run, or 'all' to run all in order",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log what would happen without committing any data",
    )
    args = parser.parse_args()

    if args.migration == "all":
        results = asyncio.run(run_all_migrations(dry_run=args.dry_run))
        failed = [r for r in results if r.status == "failed"]
        if failed:
            print(f"ERROR: {len(failed)} migration(s) failed", file=sys.stderr)
            sys.exit(1)
    else:
        result = asyncio.run(run_migration(args.migration, dry_run=args.dry_run))
        if result.status == "failed":
            print(f"ERROR: migration '{args.migration}' failed", file=sys.stderr)
            sys.exit(1)

    print("All migrations completed successfully.")


if __name__ == "__main__":
    main()
