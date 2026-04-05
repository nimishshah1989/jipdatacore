"""Cross-source data validation and reconciliation checks."""

from __future__ import annotations


from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ReconciliationResult:
    """Result of a single reconciliation check."""

    check_name: str
    passed: bool
    severity: str  # "info" | "warning" | "critical"
    message: str
    expected: str | None = None
    actual: str | None = None
    tolerance: str | None = None


class ReconciliationChecker:
    """Cross-source data reconciliation for JIP Data Engine.

    Checks:
    1. NSE vs yfinance: NIFTY 50 close price, 2% tolerance
    2. AMFI vs Morningstar NAV: top 50 funds, 0.1% tolerance
    3. Row count sanity: equity < 1000 → critical, MF < 5000 → warning
    """

    NSE_YFINANCE_TOLERANCE: Decimal = Decimal("0.02")  # 2%
    AMFI_MORNINGSTAR_TOLERANCE: Decimal = Decimal("0.001")  # 0.1%

    EQUITY_ROW_CRITICAL_THRESHOLD: int = 1000
    MF_ROW_WARNING_THRESHOLD: int = 5000

    async def check_nse_vs_yfinance(
        self,
        session: AsyncSession,
        business_date: date,
    ) -> ReconciliationResult:
        """Compare NIFTY 50 close price from NSE (de_equity_eod) vs yfinance (de_global_eod).

        Tolerance: 2% relative difference.
        """
        check_name = "nse_vs_yfinance_nifty50"

        try:
            # NSE NIFTY 50 close from indices table
            nse_result = await session.execute(
                sa.text(
                    """
                    SELECT close_value
                    FROM de_index_eod
                    WHERE index_name = 'NIFTY 50'
                      AND price_date = :bdate
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"bdate": business_date},
            )
            nse_row = nse_result.fetchone()

            # yfinance NIFTY 50 close from global EOD
            yf_result = await session.execute(
                sa.text(
                    """
                    SELECT close_price
                    FROM de_global_eod
                    WHERE symbol = '^NSEI'
                      AND price_date = :bdate
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"bdate": business_date},
            )
            yf_row = yf_result.fetchone()

            if nse_row is None:
                return ReconciliationResult(
                    check_name=check_name,
                    passed=False,
                    severity="warning",
                    message=f"NSE NIFTY 50 data not found for {business_date}",
                )

            if yf_row is None:
                return ReconciliationResult(
                    check_name=check_name,
                    passed=False,
                    severity="warning",
                    message=f"yfinance NIFTY 50 data not found for {business_date}",
                )

            nse_close = Decimal(str(nse_row[0]))
            yf_close = Decimal(str(yf_row[0]))

            if nse_close == 0:
                return ReconciliationResult(
                    check_name=check_name,
                    passed=False,
                    severity="critical",
                    message="NSE NIFTY 50 close is zero — data quality issue",
                    expected=str(nse_close),
                    actual=str(yf_close),
                )

            relative_diff = abs(nse_close - yf_close) / nse_close

            passed = relative_diff <= self.NSE_YFINANCE_TOLERANCE
            msg = (
                f"NIFTY 50 close: NSE={nse_close}, yfinance={yf_close}, "
                f"diff={relative_diff:.4%} ({'OK' if passed else 'BREACH'})"
            )

            logger.info(
                "reconciliation_nse_yfinance",
                business_date=business_date.isoformat(),
                nse_close=str(nse_close),
                yf_close=str(yf_close),
                relative_diff=str(relative_diff),
                passed=passed,
            )

            return ReconciliationResult(
                check_name=check_name,
                passed=passed,
                severity="warning" if not passed else "info",
                message=msg,
                expected=str(nse_close),
                actual=str(yf_close),
                tolerance="2%",
            )

        except Exception as exc:
            logger.error(
                "reconciliation_nse_yfinance_error",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ReconciliationResult(
                check_name=check_name,
                passed=False,
                severity="warning",
                message=f"Reconciliation check failed with exception: {exc}",
            )

    async def check_amfi_vs_morningstar(
        self,
        session: AsyncSession,
        business_date: date,
        max_funds: int = 50,
    ) -> ReconciliationResult:
        """Compare AMFI NAV vs Morningstar NAV for top funds.

        Tolerance: 0.1% relative difference.
        Returns a single aggregate result — PASS if all checked funds are within tolerance.
        """
        check_name = "amfi_vs_morningstar_nav"

        try:
            # Fetch top funds that exist in both AMFI and Morningstar
            result = await session.execute(
                sa.text(
                    """
                    SELECT
                        a.scheme_code,
                        a.nav AS amfi_nav,
                        m.nav AS ms_nav
                    FROM de_mf_nav a
                    JOIN de_morningstar_nav m ON a.scheme_code = m.scheme_code::text
                    WHERE a.nav_date = :bdate
                      AND m.nav_date = :bdate
                      AND a.nav IS NOT NULL
                      AND m.nav IS NOT NULL
                    LIMIT :max_funds
                    """
                ),
                {"bdate": business_date, "max_funds": max_funds},
            )
            rows = result.fetchall()

            if not rows:
                return ReconciliationResult(
                    check_name=check_name,
                    passed=False,
                    severity="warning",
                    message=f"No matching AMFI/Morningstar funds found for {business_date}",
                )

            breaches: list[str] = []
            for row in rows:
                amfi_nav = Decimal(str(row.amfi_nav))
                ms_nav = Decimal(str(row.ms_nav))
                if amfi_nav == 0:
                    continue
                relative_diff = abs(amfi_nav - ms_nav) / amfi_nav
                if relative_diff > self.AMFI_MORNINGSTAR_TOLERANCE:
                    breaches.append(
                        f"{row.scheme_code}: AMFI={amfi_nav}, MS={ms_nav}, diff={relative_diff:.4%}"
                    )

            passed = len(breaches) == 0
            checked = len(rows)

            msg = (
                f"AMFI vs Morningstar: checked {checked} funds, "
                f"{len(breaches)} breach(es) (tolerance 0.1%)"
            )
            if breaches:
                msg += "\nBreaches:\n" + "\n".join(breaches[:10])

            logger.info(
                "reconciliation_amfi_morningstar",
                business_date=business_date.isoformat(),
                funds_checked=checked,
                breach_count=len(breaches),
                passed=passed,
            )

            return ReconciliationResult(
                check_name=check_name,
                passed=passed,
                severity="warning" if not passed else "info",
                message=msg,
                tolerance="0.1%",
            )

        except Exception as exc:
            logger.error(
                "reconciliation_amfi_morningstar_error",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ReconciliationResult(
                check_name=check_name,
                passed=False,
                severity="warning",
                message=f"Reconciliation check failed with exception: {exc}",
            )

    async def check_equity_row_count(
        self,
        session: AsyncSession,
        business_date: date,
    ) -> ReconciliationResult:
        """Sanity check: equity EOD row count for the business date.

        < 1000 rows → critical
        """
        check_name = "equity_row_count_sanity"

        try:
            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) AS cnt FROM de_equity_eod WHERE price_date = :bdate"
                ),
                {"bdate": business_date},
            )
            row = result.fetchone()
            count = row[0] if row else 0

            passed = count >= self.EQUITY_ROW_CRITICAL_THRESHOLD
            severity = "critical" if not passed else "info"
            msg = f"Equity EOD row count for {business_date}: {count} (threshold: {self.EQUITY_ROW_CRITICAL_THRESHOLD})"

            logger.info(
                "reconciliation_equity_row_count",
                business_date=business_date.isoformat(),
                count=count,
                passed=passed,
            )

            return ReconciliationResult(
                check_name=check_name,
                passed=passed,
                severity=severity,
                message=msg,
                expected=f">= {self.EQUITY_ROW_CRITICAL_THRESHOLD}",
                actual=str(count),
            )

        except Exception as exc:
            logger.error(
                "reconciliation_equity_row_count_error",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ReconciliationResult(
                check_name=check_name,
                passed=False,
                severity="critical",
                message=f"Row count check failed: {exc}",
            )

    async def check_mf_row_count(
        self,
        session: AsyncSession,
        business_date: date,
    ) -> ReconciliationResult:
        """Sanity check: MF NAV row count for the business date.

        < 5000 rows → warning
        """
        check_name = "mf_row_count_sanity"

        try:
            result = await session.execute(
                sa.text(
                    "SELECT COUNT(*) AS cnt FROM de_mf_nav WHERE nav_date = :bdate"
                ),
                {"bdate": business_date},
            )
            row = result.fetchone()
            count = row[0] if row else 0

            passed = count >= self.MF_ROW_WARNING_THRESHOLD
            severity = "warning" if not passed else "info"
            msg = f"MF NAV row count for {business_date}: {count} (threshold: {self.MF_ROW_WARNING_THRESHOLD})"

            logger.info(
                "reconciliation_mf_row_count",
                business_date=business_date.isoformat(),
                count=count,
                passed=passed,
            )

            return ReconciliationResult(
                check_name=check_name,
                passed=passed,
                severity=severity,
                message=msg,
                expected=f">= {self.MF_ROW_WARNING_THRESHOLD}",
                actual=str(count),
            )

        except Exception as exc:
            logger.error(
                "reconciliation_mf_row_count_error",
                error=str(exc),
                business_date=business_date.isoformat(),
            )
            return ReconciliationResult(
                check_name=check_name,
                passed=False,
                severity="warning",
                message=f"Row count check failed: {exc}",
            )

    async def run_all(
        self,
        session: AsyncSession,
        business_date: date,
    ) -> list[ReconciliationResult]:
        """Run all reconciliation checks for the given date.

        Returns a list of ReconciliationResult objects.
        """
        results: list[ReconciliationResult] = []

        checks = [
            self.check_equity_row_count(session, business_date),
            self.check_mf_row_count(session, business_date),
            self.check_nse_vs_yfinance(session, business_date),
            self.check_amfi_vs_morningstar(session, business_date),
        ]

        for coro in checks:
            result = await coro
            results.append(result)

        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)

        logger.info(
            "reconciliation_run_complete",
            business_date=business_date.isoformat(),
            total=len(results),
            passed=passed,
            failed=failed,
        )

        return results
