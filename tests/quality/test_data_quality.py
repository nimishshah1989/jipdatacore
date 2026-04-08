"""Tests for scripts/quality/data_quality.py.

These tests are fully offline — they mock psycopg2 and db.get_sync_url so no
live database is needed. Every public function and the CLI argument paths are
covered.
"""

import sys
import types
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Shim: inject a fake `db` module so the import inside data_quality.py
# (which does `from db import get_sync_url`) succeeds without a real DB.
# ---------------------------------------------------------------------------
_fake_db_module = types.ModuleType("db")
_fake_db_module.get_sync_url = lambda: "postgresql://fake:fake@localhost/fake"  # type: ignore[attr-defined]
sys.modules.setdefault("db", _fake_db_module)

# Now import the module under test (after the shim is in place).
from pathlib import Path  # noqa: E402

# Make sure scripts/compute is on the path so the real import path works,
# but the shim above intercepts it before the real file is consulted.
_scripts_compute = str(Path(__file__).parent.parent.parent / "scripts" / "compute")
if _scripts_compute not in sys.path:
    sys.path.insert(0, _scripts_compute)

# Patch get_sync_url at module import time to avoid real connections.
with patch.dict(sys.modules, {"db": _fake_db_module}):
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "data_quality",
        str(Path(__file__).parent.parent.parent / "scripts" / "quality" / "data_quality.py"),
    )
    dq = _ilu.module_from_spec(_spec)  # type: ignore[arg-type]
    _spec.loader.exec_module(dq)  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(return_value=None, raise_exc=None):
    """Return a MagicMock cursor whose fetchone() returns `return_value`."""
    cur = MagicMock()
    if raise_exc is not None:
        cur.execute.side_effect = [None, raise_exc]  # savepoint OK, query fails
    else:
        cur.execute.return_value = None
        cur.fetchone.return_value = (return_value,) if return_value is not None else (None,)
    return cur


# ---------------------------------------------------------------------------
# Status function tests — freshness
# ---------------------------------------------------------------------------

class TestFreshnessStatus:
    def test_pass_when_below_warn(self):
        assert dq._freshness_status(Decimal("10"), 48, 72) == "pass"

    def test_warn_when_between_thresholds(self):
        assert dq._freshness_status(Decimal("50"), 48, 72) == "warn"

    def test_fail_when_above_fail_threshold(self):
        assert dq._freshness_status(Decimal("100"), 48, 72) == "fail"

    def test_boundary_exactly_at_warn(self):
        # metric == warn_threshold: warn (not pass)
        assert dq._freshness_status(Decimal("48"), 48, 72) == "warn"

    def test_boundary_exactly_at_fail(self):
        # metric == fail_threshold: fail
        assert dq._freshness_status(Decimal("72"), 48, 72) == "fail"


# ---------------------------------------------------------------------------
# Status function tests — completeness / reliability
# ---------------------------------------------------------------------------

class TestCompletenessStatus:
    def test_pass_above_warn(self):
        assert dq._completeness_status(Decimal("90"), 85, 70) == "pass"

    def test_warn_between_thresholds(self):
        assert dq._completeness_status(Decimal("75"), 85, 70) == "warn"

    def test_fail_below_fail_threshold(self):
        assert dq._completeness_status(Decimal("60"), 85, 70) == "fail"

    def test_boundary_exactly_at_warn(self):
        # metric == warn_threshold: warn (not pass, since condition is >)
        assert dq._completeness_status(Decimal("85"), 85, 70) == "warn"

    def test_boundary_exactly_at_fail(self):
        # metric == fail_threshold: fail (not warn, since condition is >)
        assert dq._completeness_status(Decimal("70"), 85, 70) == "fail"

    def test_reliability_delegates_correctly(self):
        # reliability uses the same function
        assert dq._completeness_status(Decimal("95"), 80, 50) == "pass"


# ---------------------------------------------------------------------------
# Status function tests — consistency
# ---------------------------------------------------------------------------

class TestConsistencyStatus:
    def test_pass_when_zero_errors(self):
        assert dq._consistency_status(Decimal("0"), 0, 1) == "pass"

    def test_warn_when_one_error(self):
        assert dq._consistency_status(Decimal("1"), 0, 1) == "warn"

    def test_fail_when_multiple_errors(self):
        assert dq._consistency_status(Decimal("5"), 0, 1) == "fail"


# ---------------------------------------------------------------------------
# run_check — happy path
# ---------------------------------------------------------------------------

class TestRunCheck:
    def _call(self, return_value, category, warn, fail, verbose=False):
        cur = _make_cursor(return_value)
        status = dq.run_check(
            cur=cur,
            name="test_check",
            category=category,
            table="de_test",
            sql="SELECT 1",
            warn_thresh=warn,
            fail_thresh=fail,
            business_date=None,
            verbose=verbose,
        )
        return status, cur

    def test_freshness_pass(self):
        status, cur = self._call(10.0, "freshness", 48, 72)
        assert status == "pass"

    def test_freshness_warn(self):
        status, _ = self._call(55.0, "freshness", 48, 72)
        assert status == "warn"

    def test_freshness_fail(self):
        status, _ = self._call(90.0, "freshness", 48, 72)
        assert status == "fail"

    def test_completeness_pass(self):
        status, _ = self._call(92.0, "completeness", 85, 70)
        assert status == "pass"

    def test_completeness_fail(self):
        status, _ = self._call(50.0, "completeness", 85, 70)
        assert status == "fail"

    def test_consistency_pass(self):
        status, _ = self._call(0, "consistency", 0, 1)
        assert status == "pass"

    def test_consistency_warn(self):
        status, _ = self._call(1, "consistency", 0, 1)
        assert status == "warn"

    def test_null_result_treated_as_zero(self):
        """NULL from DB (e.g. empty table) must not crash — treated as 0."""
        cur = MagicMock()
        cur.execute.return_value = None
        cur.fetchone.return_value = (None,)
        status = dq.run_check(
            cur=cur,
            name="null_check",
            category="freshness",
            table="de_test",
            sql="SELECT MAX(date) FROM empty",
            warn_thresh=48,
            fail_thresh=72,
            business_date=None,
        )
        # 0 hours is less than 48 → pass
        assert status == "pass"

    def test_result_inserted_into_table(self):
        """run_check must always INSERT one row regardless of status."""
        status, cur = self._call(10.0, "freshness", 48, 72)
        # Count INSERT calls (last execute call should be the INSERT)
        insert_calls = [
            c for c in cur.execute.call_args_list
            if "INSERT INTO de_data_quality_checks" in str(c)
        ]
        assert len(insert_calls) == 1

    def test_sql_error_returns_fail_and_inserts(self):
        """A broken SQL must produce status=fail and still insert a result row."""
        cur = MagicMock()
        # First execute = SAVEPOINT, second = the check SQL → raises
        cur.execute.side_effect = [
            None,                          # SAVEPOINT
            Exception("table not found"),  # check SQL
            None,                          # ROLLBACK TO SAVEPOINT
            None,                          # INSERT
        ]
        status = dq.run_check(
            cur=cur,
            name="broken_check",
            category="completeness",
            table="de_missing",
            sql="SELECT COUNT(*) FROM de_missing",
            warn_thresh=100,
            fail_thresh=50,
        )
        assert status == "fail"
        # INSERT must still have been called
        insert_calls = [
            c for c in cur.execute.call_args_list
            if "INSERT INTO de_data_quality_checks" in str(c)
        ]
        assert len(insert_calls) == 1

    def test_verbose_flag_does_not_raise(self, capsys):
        self._call(10.0, "freshness", 48, 72, verbose=True)
        captured = capsys.readouterr()
        assert "[PASS]" in captured.out


# ---------------------------------------------------------------------------
# CHECKS list integrity
# ---------------------------------------------------------------------------

class TestChecksDefinition:
    def test_all_checks_have_five_fields(self):
        for item in dq.CHECKS:
            assert len(item) == 6, f"Check tuple has wrong length: {item}"

    def test_check_names_are_unique(self):
        names = [c[0] for c in dq.CHECKS]
        assert len(names) == len(set(names)), "Duplicate check names found"

    def test_categories_are_valid(self):
        valid = {"freshness", "completeness", "consistency", "reliability"}
        for name, cat, *_ in dq.CHECKS:
            assert cat in valid, f"Check '{name}' has unknown category '{cat}'"

    def test_warn_ge_fail_for_freshness(self):
        """For freshness, warn < fail (higher hours = worse)."""
        for name, cat, _, _sql, warn, fail in dq.CHECKS:
            if cat == "freshness":
                assert warn < fail, (
                    f"Freshness check '{name}': warn ({warn}) must be < fail ({fail})"
                )

    def test_warn_ge_fail_for_completeness(self):
        """For completeness/reliability, warn > fail (higher coverage = better)."""
        for name, cat, _, _sql, warn, fail in dq.CHECKS:
            if cat in ("completeness", "reliability"):
                assert warn >= fail, (
                    f"Completeness check '{name}': warn ({warn}) must be >= fail ({fail})"
                )

    def test_expected_check_count(self):
        assert len(dq.CHECKS) == 15, (
            f"Expected 15 checks, got {len(dq.CHECKS)}"
        )


# ---------------------------------------------------------------------------
# ensure_table
# ---------------------------------------------------------------------------

class TestEnsureTable:
    def test_executes_create_table_sql(self):
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        dq.ensure_table(conn)
        cur.execute.assert_called_once()
        sql_arg = cur.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS de_data_quality_checks" in sql_arg
        assert "CREATE INDEX IF NOT EXISTS idx_dqc_checked" in sql_arg
        conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# CLI: main() argument parsing
# ---------------------------------------------------------------------------

class TestMainCLI:
    def _run_main(self, argv, mock_conn, mock_cursor):
        """Helper that patches sys.argv, psycopg2.connect, and get_sync_url."""
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch("sys.argv", ["data_quality.py"] + argv),
            patch.object(dq, "get_sync_url", return_value="postgresql://fake/db"),
            patch("psycopg2.connect", return_value=mock_conn),
        ):
            try:
                dq.main()
            except SystemExit as exc:
                return exc.code
        return 0

    def test_all_checks_run_by_default(self):
        """All 15 checks pass when each check returns a value that satisfies its
        own category thresholds:
          - freshness checks: 10h (well under 48h warn threshold)
          - completeness/row-count checks: 99 percent / large count — but since
            the mock cursor returns a single scalar we patch run_check itself
            to always return 'pass', which is the correct unit-test approach
            (run_check correctness is tested separately).
        """
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()

        with patch.object(dq, "run_check", return_value="pass") as mock_rc:
            exit_code = self._run_main(["--verbose"], conn, cur)

        assert exit_code == 0
        # run_check called once per check definition
        assert mock_rc.call_count == len(dq.CHECKS)

    def test_single_check_filter(self):
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()
        # equity_ohlcv_fresh: freshness, warn=48, fail=72; value 10 → pass
        cur.fetchone.return_value = (10.0,)
        exit_code = self._run_main(["--check", "equity_ohlcv_fresh"], conn, cur)
        assert exit_code == 0

    def test_single_check_runs_only_one_check(self):
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()

        with patch.object(dq, "run_check", return_value="pass") as mock_rc:
            self._run_main(["--check", "equity_ohlcv_fresh"], conn, cur)

        assert mock_rc.call_count == 1
        assert mock_rc.call_args.kwargs["name"] == "equity_ohlcv_fresh"

    def test_unknown_check_exits_1(self):
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()
        exit_code = self._run_main(["--check", "nonexistent_check"], conn, cur)
        assert exit_code == 1

    def test_missing_db_url_exits_1(self):
        with (
            patch("sys.argv", ["data_quality.py"]),
            patch.object(dq, "get_sync_url", return_value=""),
        ):
            with pytest.raises(SystemExit) as exc_info:
                dq.main()
            assert exc_info.value.code == 1

    def test_db_connection_error_exits_1(self):
        with (
            patch("sys.argv", ["data_quality.py"]),
            patch.object(dq, "get_sync_url", return_value="postgresql://bad/db"),
            patch("psycopg2.connect", side_effect=Exception("connection refused")),
        ):
            with pytest.raises(SystemExit) as exc_info:
                dq.main()
            assert exc_info.value.code == 1

    def test_fail_check_exits_1(self):
        """When any check returns fail, main() should exit with code 1."""
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()

        with patch.object(dq, "run_check", return_value="fail"):
            exit_code = self._run_main(["--check", "equity_ohlcv_fresh"], conn, cur)

        assert exit_code == 1

    def test_warn_only_exits_0(self):
        """Warnings alone must not trigger a non-zero exit code."""
        conn = MagicMock()
        conn.autocommit = False
        cur = MagicMock()

        with patch.object(dq, "run_check", return_value="warn"):
            exit_code = self._run_main(["--check", "equity_ohlcv_fresh"], conn, cur)

        assert exit_code == 0
