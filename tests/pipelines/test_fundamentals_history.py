"""Tests for historical fundamentals parsers (GAP-17)."""

from datetime import date

from app.pipelines.fundamentals.screener_fetcher import (
    _extract_history_section,
    _PL_ROW_MAP,
    _BS_ROW_MAP,
    _CF_ROW_MAP,
    extract_fundamentals_history,
)


MINIMAL_PL_HTML = """
<section id="profit-loss">
<table class="data-table">
<thead><tr><th></th><th>Mar 2022</th><th>Mar 2023</th><th>Mar 2024</th><th>TTM</th></tr></thead>
<tbody>
<tr><td>Sales</td><td>100,000</td><td>120,000</td><td>140,000</td><td>150,000</td></tr>
<tr><td>Expenses</td><td>80,000</td><td>95,000</td><td>110,000</td><td>115,000</td></tr>
<tr><td>Operating Profit</td><td>20,000</td><td>25,000</td><td>30,000</td><td>35,000</td></tr>
<tr><td>OPM %</td><td>20%</td><td>21%</td><td>21%</td><td>23%</td></tr>
<tr><td>Other Income</td><td>1,000</td><td>1,200</td><td>1,500</td><td>1,600</td></tr>
<tr><td>Interest</td><td>500</td><td>600</td><td>700</td><td>750</td></tr>
<tr><td>Depreciation</td><td>2,000</td><td>2,200</td><td>2,500</td><td>2,600</td></tr>
<tr><td>Profit before tax</td><td>18,500</td><td>23,400</td><td>28,300</td><td>33,250</td></tr>
<tr><td>Tax %</td><td>25%</td><td>25%</td><td>26%</td><td>25%</td></tr>
<tr><td>Net Profit</td><td>13,875</td><td>17,550</td><td>20,942</td><td>24,937</td></tr>
<tr><td>EPS in Rs</td><td>45.50</td><td>57.50</td><td>68.60</td><td>81.70</td></tr>
<tr><td>Dividend Payout %</td><td>30%</td><td>28%</td><td>29%</td><td>--</td></tr>
</tbody>
</table>
</section>
"""

MINIMAL_QUARTERS_HTML = """
<section id="quarters">
<table class="data-table">
<thead><tr><th></th><th>Jun 2023</th><th>Sep 2023</th><th>Dec 2023</th><th>Mar 2024</th></tr></thead>
<tbody>
<tr><td>Sales</td><td>33,000</td><td>35,000</td><td>36,000</td><td>36,000</td></tr>
<tr><td>Net Profit</td><td>4,800</td><td>5,200</td><td>5,400</td><td>5,542</td></tr>
<tr><td>EPS in Rs</td><td>15.70</td><td>17.00</td><td>17.70</td><td>18.20</td></tr>
</tbody>
</table>
</section>
"""

MINIMAL_BS_HTML = """
<section id="balance-sheet">
<table class="data-table">
<thead><tr><th></th><th>Mar 2022</th><th>Mar 2023</th><th>Mar 2024</th></tr></thead>
<tbody>
<tr><td>Equity Capital</td><td>1,000</td><td>1,000</td><td>1,000</td></tr>
<tr><td>Reserves</td><td>50,000</td><td>60,000</td><td>72,000</td></tr>
<tr><td>Borrowings</td><td>10,000</td><td>12,000</td><td>14,000</td></tr>
<tr><td>Other Liabilities</td><td>8,000</td><td>9,000</td><td>10,000</td></tr>
<tr><td>Fixed Assets</td><td>30,000</td><td>35,000</td><td>40,000</td></tr>
<tr><td>CWIP</td><td>5,000</td><td>6,000</td><td>7,000</td></tr>
<tr><td>Investments</td><td>20,000</td><td>25,000</td><td>30,000</td></tr>
<tr><td>Other Assets</td><td>14,000</td><td>16,000</td><td>20,000</td></tr>
<tr><td>Total Assets</td><td>69,000</td><td>82,000</td><td>97,000</td></tr>
</tbody>
</table>
</section>
"""

MINIMAL_CF_HTML = """
<section id="cash-flow">
<table class="data-table">
<thead><tr><th></th><th>Mar 2022</th><th>Mar 2023</th><th>Mar 2024</th></tr></thead>
<tbody>
<tr><td>Cash from Operating Activity</td><td>15,000</td><td>18,000</td><td>22,000</td></tr>
<tr><td>Cash from Investing Activity</td><td>-10,000</td><td>-12,000</td><td>-15,000</td></tr>
<tr><td>Cash from Financing Activity</td><td>-3,000</td><td>-4,000</td><td>-5,000</td></tr>
</tbody>
</table>
</section>
"""

FULL_HTML = MINIMAL_PL_HTML + MINIMAL_QUARTERS_HTML + MINIMAL_BS_HTML + MINIMAL_CF_HTML


class TestExtractHistorySection:
    def test_annual_pl_rows(self):
        rows = _extract_history_section(FULL_HTML, "profit_loss", _PL_ROW_MAP, "annual")
        assert len(rows) == 3  # TTM excluded
        dates = sorted(r["fiscal_period_end"] for r in rows)
        assert dates == [date(2022, 3, 31), date(2023, 3, 31), date(2024, 3, 31)]

    def test_annual_pl_values(self):
        rows = _extract_history_section(FULL_HTML, "profit_loss", _PL_ROW_MAP, "annual")
        mar_2024 = next(r for r in rows if r["fiscal_period_end"] == date(2024, 3, 31))
        assert mar_2024["revenue_cr"] == 140_000.0
        assert mar_2024["net_profit_cr"] == 20_942.0
        assert mar_2024["eps"] == 68.6
        assert mar_2024["opm_pct"] == 21.0
        assert mar_2024["tax_pct"] == 26.0

    def test_quarterly_rows(self):
        rows = _extract_history_section(FULL_HTML, "quarters", _PL_ROW_MAP, "quarterly")
        assert len(rows) == 4
        assert all(r["period_type"] == "quarterly" for r in rows)

    def test_balance_sheet_rows(self):
        rows = _extract_history_section(FULL_HTML, "balance_sheet", _BS_ROW_MAP, "annual")
        assert len(rows) == 3
        mar_2024 = next(r for r in rows if r["fiscal_period_end"] == date(2024, 3, 31))
        assert mar_2024["total_assets_cr"] == 97_000.0
        assert mar_2024["borrowings_cr"] == 14_000.0

    def test_cash_flow_rows(self):
        rows = _extract_history_section(FULL_HTML, "cash_flow", _CF_ROW_MAP, "annual")
        assert len(rows) == 3
        mar_2024 = next(r for r in rows if r["fiscal_period_end"] == date(2024, 3, 31))
        assert mar_2024["cfo_cr"] == 22_000.0
        assert mar_2024["cfi_cr"] == -15_000.0
        assert mar_2024["cff_cr"] == -5_000.0

    def test_empty_html(self):
        rows = _extract_history_section("<html></html>", "profit_loss", _PL_ROW_MAP, "annual")
        assert rows == []


class TestExtractFundamentalsHistory:
    def test_merges_annual_sections(self):
        rows = extract_fundamentals_history(FULL_HTML)
        annual = [r for r in rows if r["period_type"] == "annual"]
        quarterly = [r for r in rows if r["period_type"] == "quarterly"]
        assert len(annual) == 3
        assert len(quarterly) == 4

    def test_annual_has_all_fields(self):
        rows = extract_fundamentals_history(FULL_HTML)
        mar_2024 = next(
            r for r in rows
            if r["period_type"] == "annual" and r["fiscal_period_end"] == date(2024, 3, 31)
        )
        assert mar_2024["revenue_cr"] == 140_000.0
        assert mar_2024["total_assets_cr"] == 97_000.0
        assert mar_2024["cfo_cr"] == 22_000.0
        assert mar_2024["net_profit_cr"] == 20_942.0

    def test_quarterly_has_pl_only(self):
        rows = extract_fundamentals_history(FULL_HTML)
        q = next(
            r for r in rows
            if r["period_type"] == "quarterly" and r["fiscal_period_end"] == date(2024, 3, 31)
        )
        assert q["revenue_cr"] == 36_000.0
        assert "total_assets_cr" not in q
        assert "cfo_cr" not in q

    def test_ttm_excluded(self):
        rows = extract_fundamentals_history(FULL_HTML)
        dates = [r["fiscal_period_end"] for r in rows]
        for d in dates:
            assert d is not None

    def test_dividend_payout_ignored(self):
        rows = extract_fundamentals_history(FULL_HTML)
        for r in rows:
            assert "dividend_payout_pct" not in r
