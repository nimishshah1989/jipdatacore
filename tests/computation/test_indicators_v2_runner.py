"""Unit tests for IND-C11: indicators_v2 daily runner.

All tests use AsyncMock for DB session and compute functions — no real DB calls.

pandas_ta_classic is Docker-only; stub it via sys.modules before any
indicators_v2 import (engine.py imports it at module level).
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock


def _ensure_docker_only_deps_stubbed() -> None:
    """Stub Docker-only deps so local imports work.

    pandas_ta_classic — C-extension, installed only in Docker.
    yaml (PyYAML) — may be absent in minimal local venvs.
    """
    if "pandas_ta_classic" not in sys.modules:
        stub = types.ModuleType("pandas_ta_classic")
        stub.Strategy = MagicMock()  # type: ignore[attr-defined]
        sys.modules["pandas_ta_classic"] = stub
    if "yaml" not in sys.modules:
        yaml_stub = types.ModuleType("yaml")
        yaml_stub.safe_load = MagicMock(return_value={})  # type: ignore[attr-defined]
        sys.modules["yaml"] = yaml_stub


_ensure_docker_only_deps_stubbed()

from datetime import date  # noqa: E402
from unittest.mock import AsyncMock, patch  # noqa: E402

import pytest  # noqa: E402

from app.computation.indicators_v2.engine import CompResult  # noqa: E402
from app.computation.indicators_v2.runner import (  # noqa: E402
    DAILY_LOOKBACK_DAYS,
    IndicatorsV2RunReport,
    run_indicators_v2_pipeline,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(asset_class: str, rows: int = 10, processed: int = 5) -> CompResult:
    return CompResult(
        asset_class=asset_class,
        instruments_processed=processed,
        rows_written=rows,
    )


MODULE = "app.computation.indicators_v2.runner"

ASSET_PATCH_TARGETS = [
    f"{MODULE}.compute_equity_indicators",
    f"{MODULE}.compute_index_indicators",
    f"{MODULE}.compute_etf_indicators",
    f"{MODULE}.compute_global_indicators",
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_runner_calls_all_four_assets() -> None:
    """All four compute functions are called once with correct from_date/to_date."""
    business_date = date(2026, 4, 14)
    expected_from = business_date.replace(day=14 - DAILY_LOOKBACK_DAYS)  # 2026-04-09

    session = AsyncMock()

    with (
        patch(ASSET_PATCH_TARGETS[0], new_callable=AsyncMock) as mock_equity,
        patch(ASSET_PATCH_TARGETS[1], new_callable=AsyncMock) as mock_index,
        patch(ASSET_PATCH_TARGETS[2], new_callable=AsyncMock) as mock_etf,
        patch(ASSET_PATCH_TARGETS[3], new_callable=AsyncMock) as mock_global,
    ):
        mock_equity.return_value = _make_result("equity")
        mock_index.return_value = _make_result("index")
        mock_etf.return_value = _make_result("etf")
        mock_global.return_value = _make_result("global")

        report = await run_indicators_v2_pipeline(session, business_date)

    # Each asset called exactly once
    mock_equity.assert_called_once()
    mock_index.assert_called_once()
    mock_etf.assert_called_once()
    mock_global.assert_called_once()

    # Verify date window on equity call (representative; all use same window)
    _, kwargs = mock_equity.call_args
    assert kwargs["from_date"] == expected_from
    assert kwargs["to_date"] == business_date

    # All assets recorded in report
    assert set(report.asset_results.keys()) == {"equity", "index", "etf", "global"}
    assert report.total_rows_written == 40  # 4 * 10
    assert report.failed_assets == []


@pytest.mark.asyncio
async def test_runner_partial_on_asset_failure() -> None:
    """First asset raises; overall_status is 'partial' and remaining 3 assets still run."""
    business_date = date(2026, 4, 14)
    session = AsyncMock()

    with (
        patch(ASSET_PATCH_TARGETS[0], new_callable=AsyncMock) as mock_equity,
        patch(ASSET_PATCH_TARGETS[1], new_callable=AsyncMock) as mock_index,
        patch(ASSET_PATCH_TARGETS[2], new_callable=AsyncMock) as mock_etf,
        patch(ASSET_PATCH_TARGETS[3], new_callable=AsyncMock) as mock_global,
    ):
        mock_equity.side_effect = RuntimeError("equity DB connection dropped")
        mock_index.return_value = _make_result("index")
        mock_etf.return_value = _make_result("etf")
        mock_global.return_value = _make_result("global")

        report = await run_indicators_v2_pipeline(session, business_date)

    assert report.overall_status == "partial"
    assert report.failed_assets == ["equity"]
    # Remaining assets ran
    assert set(report.asset_results.keys()) == {"index", "etf", "global"}
    assert report.total_rows_written == 30  # 3 * 10
    # Session rolled back after equity failure
    session.rollback.assert_called_once()
    # Session committed for each successful asset
    assert session.commit.call_count == 3

    # Subsequent assets still called
    mock_index.assert_called_once()
    mock_etf.assert_called_once()
    mock_global.assert_called_once()


@pytest.mark.asyncio
async def test_runner_failed_on_all_failures() -> None:
    """All four assets raise; overall_status is 'failed'."""
    business_date = date(2026, 4, 14)
    session = AsyncMock()

    with (
        patch(ASSET_PATCH_TARGETS[0], new_callable=AsyncMock) as mock_equity,
        patch(ASSET_PATCH_TARGETS[1], new_callable=AsyncMock) as mock_index,
        patch(ASSET_PATCH_TARGETS[2], new_callable=AsyncMock) as mock_etf,
        patch(ASSET_PATCH_TARGETS[3], new_callable=AsyncMock) as mock_global,
    ):
        mock_equity.side_effect = RuntimeError("equity error")
        mock_index.side_effect = RuntimeError("index error")
        mock_etf.side_effect = RuntimeError("etf error")
        mock_global.side_effect = RuntimeError("global error")

        report = await run_indicators_v2_pipeline(session, business_date)

    assert report.overall_status == "failed"
    assert set(report.failed_assets) == {"equity", "index", "etf", "global"}
    assert report.asset_results == {}
    assert report.total_rows_written == 0
    assert session.rollback.call_count == 4
    assert session.commit.call_count == 0


@pytest.mark.asyncio
async def test_runner_passed_on_clean_run() -> None:
    """All four assets succeed; overall_status is 'passed'."""
    business_date = date(2026, 4, 14)
    session = AsyncMock()

    with (
        patch(ASSET_PATCH_TARGETS[0], new_callable=AsyncMock) as mock_equity,
        patch(ASSET_PATCH_TARGETS[1], new_callable=AsyncMock) as mock_index,
        patch(ASSET_PATCH_TARGETS[2], new_callable=AsyncMock) as mock_etf,
        patch(ASSET_PATCH_TARGETS[3], new_callable=AsyncMock) as mock_global,
    ):
        mock_equity.return_value = _make_result("equity", rows=100)
        mock_index.return_value = _make_result("index", rows=20)
        mock_etf.return_value = _make_result("etf", rows=50)
        mock_global.return_value = _make_result("global", rows=15)

        report = await run_indicators_v2_pipeline(session, business_date)

    assert report.overall_status == "passed"
    assert report.failed_assets == []
    assert report.total_rows_written == 185
    assert session.commit.call_count == 4
    session.rollback.assert_not_called()


# ---------------------------------------------------------------------------
# IndicatorsV2RunReport property tests
# ---------------------------------------------------------------------------

def test_report_overall_status_passed() -> None:
    r = IndicatorsV2RunReport(business_date=date(2026, 4, 14))
    r.asset_results = {"equity": _make_result("equity")}
    assert r.overall_status == "passed"


def test_report_overall_status_partial() -> None:
    r = IndicatorsV2RunReport(business_date=date(2026, 4, 14))
    r.asset_results = {"index": _make_result("index")}
    r.failed_assets = ["equity"]
    assert r.overall_status == "partial"


def test_report_overall_status_failed_when_no_successes() -> None:
    r = IndicatorsV2RunReport(business_date=date(2026, 4, 14))
    # No asset_results, all failed
    r.failed_assets = ["equity", "index", "etf", "global"]
    assert r.overall_status == "failed"
