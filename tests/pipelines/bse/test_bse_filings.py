"""Tests for BSE filings pipeline (GAP-18a)."""

from datetime import date
from decimal import Decimal

from app.pipelines.bse.filings import (
    _classify_action,
    _extract_amount,
    _parse_bse_date,
    _parse_bse_datetime,
    _sha256,
)
from tests.pipelines.bse.fixtures import (
    ACTIONS_FIXTURE,
    ANNOUNCEMENTS_FIXTURE,
    RESULT_CALENDAR_FIXTURE,
)


class TestParseBseDate:
    def test_dd_mmm_yyyy(self):
        assert _parse_bse_date("17 Apr 2026") == date(2026, 4, 17)

    def test_yyyymmdd(self):
        assert _parse_bse_date("20260417") == date(2026, 4, 17)

    def test_iso(self):
        assert _parse_bse_date("2026-04-17") == date(2026, 4, 17)

    def test_none(self):
        assert _parse_bse_date(None) is None

    def test_empty(self):
        assert _parse_bse_date("") is None

    def test_invalid(self):
        assert _parse_bse_date("not-a-date") is None


class TestParseBseDatetime:
    def test_iso_with_millis(self):
        result = _parse_bse_datetime("2026-04-14T10:30:00.000")
        assert result is not None
        assert result.year == 2026
        assert result.month == 4
        assert result.day == 14
        assert result.hour == 10
        assert result.minute == 30

    def test_iso_without_millis(self):
        result = _parse_bse_datetime("2026-04-14T10:30:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_none(self):
        assert _parse_bse_datetime(None) is None

    def test_empty(self):
        assert _parse_bse_datetime("") is None


class TestClassifyAction:
    def test_dividend(self):
        assert _classify_action("Dividend - Rs 10.00 Per Share") == "dividend"

    def test_split(self):
        assert _classify_action("Stock Split From Rs.10/- to Rs.2/-") == "split"

    def test_bonus(self):
        assert _classify_action("Bonus 1:1") == "bonus"

    def test_buyback(self):
        assert _classify_action("Buy Back of Shares") == "buyback"

    def test_rights(self):
        assert _classify_action("Rights Issue 1:5") == "rights"

    def test_demerger(self):
        assert _classify_action("Scheme of Arrangement - Demerger") == "demerger"

    def test_income_distribution(self):
        assert _classify_action("Income Distribution (InvIT)") == "dividend"

    def test_unknown(self):
        assert _classify_action("Annual General Meeting") == "other"

    def test_empty(self):
        assert _classify_action("") == "other"

    def test_none(self):
        assert _classify_action(None) == "other"


class TestExtractAmount:
    def test_standard(self):
        assert _extract_amount("Dividend - Rs 10.00 Per Share") == Decimal("10.00")

    def test_with_dot(self):
        assert _extract_amount("Dividend - Rs. 19.50 Per Share") == Decimal("19.50")

    def test_no_amount(self):
        assert _extract_amount("Buy Back of Shares") is None

    def test_none(self):
        assert _extract_amount(None) is None


class TestSha256:
    def test_deterministic(self):
        h1 = _sha256("500325", "2026-04-17", "dividend")
        h2 = _sha256("500325", "2026-04-17", "dividend")
        assert h1 == h2
        assert len(h1) == 64

    def test_different_inputs(self):
        h1 = _sha256("500325", "2026-04-17", "dividend")
        h2 = _sha256("500180", "2026-04-17", "dividend")
        assert h1 != h2


class TestFixtureShapes:
    def test_announcements_has_required_fields(self):
        for item in ANNOUNCEMENTS_FIXTURE["Table"]:
            assert "SCRIP_CD" in item
            assert "DT_TM" in item
            assert "HEADLINE" in item

    def test_announcements_count(self):
        assert len(ANNOUNCEMENTS_FIXTURE["Table"]) == 10

    def test_actions_has_required_fields(self):
        for item in ACTIONS_FIXTURE:
            assert "scrip_code" in item
            assert "Ex_date" in item
            assert "Purpose" in item

    def test_actions_count(self):
        assert len(ACTIONS_FIXTURE) == 5

    def test_result_calendar_has_required_fields(self):
        for item in RESULT_CALENDAR_FIXTURE:
            assert "scrip_Code" in item
            assert "meeting_date" in item

    def test_result_calendar_count(self):
        assert len(RESULT_CALENDAR_FIXTURE) == 5


class TestDedupHashing:
    """Verify that re-ingesting same data produces identical hashes (dedup)."""

    def test_announcement_dedup_hash_stable(self):
        item = ANNOUNCEMENTS_FIXTURE["Table"][0]
        sc = str(item["SCRIP_CD"])
        dt_str = item["DT_TM"]
        headline = item["HEADLINE"]
        h1 = _sha256(sc, dt_str, headline)
        h2 = _sha256(sc, dt_str, headline)
        assert h1 == h2

    def test_action_dedup_hash_stable(self):
        act = ACTIONS_FIXTURE[0]
        sc = str(act["scrip_code"])
        ex = str(_parse_bse_date(act["Ex_date"]))
        atype = _classify_action(act["Purpose"])
        h1 = _sha256(sc, ex, atype)
        h2 = _sha256(sc, ex, atype)
        assert h1 == h2

    def test_calendar_dedup_hash_stable(self):
        entry = RESULT_CALENDAR_FIXTURE[0]
        sc = str(entry["scrip_Code"])
        rd = str(_parse_bse_date(entry["meeting_date"]))
        h1 = _sha256(sc, rd)
        h2 = _sha256(sc, rd)
        assert h1 == h2

    def test_all_announcement_hashes_unique(self):
        hashes = set()
        for item in ANNOUNCEMENTS_FIXTURE["Table"]:
            sc = str(item["SCRIP_CD"])
            dt_str = item["DT_TM"]
            headline = item["HEADLINE"]
            hashes.add(_sha256(sc, dt_str, headline))
        assert len(hashes) == len(ANNOUNCEMENTS_FIXTURE["Table"])


class TestScripcodeMapping:
    """Verify unmapped scripcode is handled (999999 in fixtures has no instrument)."""

    def test_fixture_has_unmapped_announcement(self):
        scripcodes = {str(item["SCRIP_CD"]) for item in ANNOUNCEMENTS_FIXTURE["Table"]}
        assert "999999" in scripcodes

    def test_fixture_has_unmapped_action(self):
        scripcodes = {str(item["scrip_code"]) for item in ACTIONS_FIXTURE}
        assert "999999" in scripcodes

    def test_fixture_has_unmapped_calendar(self):
        scripcodes = {str(item["scrip_Code"]) for item in RESULT_CALENDAR_FIXTURE}
        assert "999999" in scripcodes
