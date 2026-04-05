"""Tests for post-ingestion validation framework."""

from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.pipelines.validation import (
    AnomalyRecord,
    apply_data_status,
    check_freshness,
    check_quarantine_threshold,
    record_anomalies,
)


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


@pytest.fixture
def sample_anomalies() -> list[AnomalyRecord]:
    return [
        AnomalyRecord(
            entity_type="equity",
            anomaly_type="price_spike",
            severity="high",
            expected_range="100-110",
            actual_value="250",
            instrument_id=uuid.uuid4(),
        ),
        AnomalyRecord(
            entity_type="mf",
            anomaly_type="nav_deviation",
            severity="medium",
            mstar_id="F00000XYZ1",
        ),
    ]


# --- record_anomalies ---

@pytest.mark.asyncio
async def test_record_anomalies_inserts_all_records(
    mock_session: AsyncMock,
    sample_anomalies: list[AnomalyRecord],
) -> None:
    """record_anomalies inserts the correct number of anomaly records."""
    mock_session.execute.return_value = MagicMock()

    count = await record_anomalies(mock_session, "equity_bhav", date(2025, 1, 15), sample_anomalies)

    assert count == 2
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_record_anomalies_empty_list_returns_zero(mock_session: AsyncMock) -> None:
    """record_anomalies returns 0 without hitting the DB when list is empty."""
    count = await record_anomalies(mock_session, "equity_bhav", date(2025, 1, 15), [])

    assert count == 0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_record_anomalies_assigns_unique_ids(
    mock_session: AsyncMock,
) -> None:
    """record_anomalies assigns a unique UUID to each anomaly record."""
    captured_rows = []

    async def capture_execute(stmt, *args, **kwargs):
        # Capture the insert values
        has_compile = hasattr(stmt, "compile")
        captured_rows.extend(
            stmt.compile(compile_kwargs={"literal_binds": False}).string if has_compile else []
        )
        return MagicMock()

    mock_session.execute.return_value = MagicMock()

    anomalies = [
        AnomalyRecord(entity_type="equity", anomaly_type="price_spike", severity="high", instrument_id=uuid.uuid4()),
        AnomalyRecord(entity_type="equity", anomaly_type="zero_volume", severity="low", instrument_id=uuid.uuid4()),
    ]
    count = await record_anomalies(mock_session, "equity_bhav", date(2025, 1, 15), anomalies)

    assert count == 2


# --- check_quarantine_threshold ---

@pytest.mark.asyncio
async def test_quarantine_threshold_below_5pct_does_not_halt(mock_session: AsyncMock) -> None:
    """check_quarantine_threshold returns should_halt=False when below 5%."""
    result_mock = MagicMock()
    result_mock.scalar_one.return_value = 4  # 4 out of 100 = 4%
    mock_session.execute.return_value = result_mock

    should_halt, pct = await check_quarantine_threshold(mock_session, "equity_bhav", date(2025, 1, 15), 100)

    assert should_halt is False
    assert pct == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_quarantine_threshold_exactly_5pct_does_not_halt(mock_session: AsyncMock) -> None:
    """check_quarantine_threshold returns should_halt=False at exactly 5% (threshold is exclusive)."""
    result_mock = MagicMock()
    result_mock.scalar_one.return_value = 5  # exactly 5 out of 100 = 5.0%
    mock_session.execute.return_value = result_mock

    should_halt, pct = await check_quarantine_threshold(mock_session, "equity_bhav", date(2025, 1, 15), 100)

    assert should_halt is False
    assert pct == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_quarantine_threshold_above_5pct_halts(mock_session: AsyncMock) -> None:
    """check_quarantine_threshold returns should_halt=True when above 5%."""
    result_mock = MagicMock()
    result_mock.scalar_one.return_value = 6  # 6 out of 100 = 6% > 5%
    mock_session.execute.return_value = result_mock

    should_halt, pct = await check_quarantine_threshold(mock_session, "equity_bhav", date(2025, 1, 15), 100)

    assert should_halt is True
    assert pct == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_quarantine_threshold_zero_total_rows_no_halt(mock_session: AsyncMock) -> None:
    """check_quarantine_threshold handles zero total_rows gracefully (returns False, 0.0)."""
    should_halt, pct = await check_quarantine_threshold(mock_session, "equity_bhav", date(2025, 1, 15), 0)

    assert should_halt is False
    assert pct == 0.0
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_quarantine_threshold_100pct_halts(mock_session: AsyncMock) -> None:
    """check_quarantine_threshold halts when all rows are quarantined."""
    result_mock = MagicMock()
    result_mock.scalar_one.return_value = 500  # 500 out of 500 = 100%
    mock_session.execute.return_value = result_mock

    should_halt, pct = await check_quarantine_threshold(mock_session, "equity_bhav", date(2025, 1, 15), 500)

    assert should_halt is True
    assert pct == pytest.approx(100.0)


# --- check_freshness ---

@pytest.mark.asyncio
async def test_check_freshness_returns_true_when_no_prior_ingestion(mock_session: AsyncMock) -> None:
    """check_freshness returns (True, reason) when no matching record exists."""
    result_mock = MagicMock()
    result_mock.first.return_value = None
    mock_session.execute.return_value = result_mock

    is_fresh, reason = await check_freshness(mock_session, "bhav_copy", date(2025, 1, 15), checksum="abc123")

    assert is_fresh is True
    assert "No prior ingestion" in reason


@pytest.mark.asyncio
async def test_check_freshness_returns_false_when_duplicate_found(mock_session: AsyncMock) -> None:
    """check_freshness returns (False, reason) when matching record already exists."""
    existing_id = uuid.uuid4()
    result_mock = MagicMock()
    result_mock.first.return_value = (existing_id, 1000)  # (id, row_count)
    mock_session.execute.return_value = result_mock

    is_fresh, reason = await check_freshness(mock_session, "bhav_copy", date(2025, 1, 15), checksum="abc123")

    assert is_fresh is False
    assert "Duplicate detected" in reason
    assert str(existing_id) in reason


@pytest.mark.asyncio
async def test_check_freshness_works_without_checksum(mock_session: AsyncMock) -> None:
    """check_freshness works when no checksum is provided (falls back to source_name+date)."""
    result_mock = MagicMock()
    result_mock.first.return_value = None
    mock_session.execute.return_value = result_mock

    is_fresh, reason = await check_freshness(mock_session, "bhav_copy", date(2025, 1, 15))

    assert is_fresh is True


@pytest.mark.asyncio
async def test_check_freshness_duplicate_includes_row_count_in_reason(mock_session: AsyncMock) -> None:
    """check_freshness includes the existing row count in the duplicate reason."""
    existing_id = uuid.uuid4()
    result_mock = MagicMock()
    result_mock.first.return_value = (existing_id, 2500)
    mock_session.execute.return_value = result_mock

    is_fresh, reason = await check_freshness(mock_session, "bhav_copy", date(2025, 1, 15), checksum="xyz")

    assert is_fresh is False
    assert "2500" in reason


# --- apply_data_status ---

@pytest.mark.asyncio
async def test_apply_data_status_returns_validated_and_quarantined_counts(mock_session: AsyncMock) -> None:
    """apply_data_status returns (validated_count, quarantined_count) correctly."""
    instrument_id = uuid.uuid4()
    # First execute: quarantine by instrument_id (rowcount=1)
    # Second execute: validate remaining raw rows (rowcount=99)
    quarantine_result = MagicMock()
    quarantine_result.rowcount = 1
    validate_result = MagicMock()
    validate_result.rowcount = 99

    mock_session.execute.side_effect = [quarantine_result, validate_result]

    validated, quarantined = await apply_data_status(
        mock_session,
        "de_equity_prices",
        date(2025, 1, 15),
        pipeline_run_id=42,
        anomaly_instrument_ids={instrument_id},
    )

    assert quarantined == 1
    assert validated == 99
    assert mock_session.execute.call_count == 2


@pytest.mark.asyncio
async def test_apply_data_status_no_anomalies_validates_all(mock_session: AsyncMock) -> None:
    """apply_data_status marks all rows as validated when no anomaly IDs are provided."""
    validate_result = MagicMock()
    validate_result.rowcount = 200
    mock_session.execute.return_value = validate_result

    validated, quarantined = await apply_data_status(
        mock_session,
        "de_equity_prices",
        date(2025, 1, 15),
        pipeline_run_id=42,
    )

    assert quarantined == 0
    assert validated == 200
    mock_session.execute.assert_called_once()


@pytest.mark.asyncio
async def test_apply_data_status_mstar_id_quarantine(mock_session: AsyncMock) -> None:
    """apply_data_status handles quarantine by mstar_id."""
    quarantine_result = MagicMock()
    quarantine_result.rowcount = 3
    validate_result = MagicMock()
    validate_result.rowcount = 97

    mock_session.execute.side_effect = [quarantine_result, validate_result]

    validated, quarantined = await apply_data_status(
        mock_session,
        "de_mf_nav",
        date(2025, 1, 15),
        pipeline_run_id=7,
        anomaly_mstar_ids={"F00000ABC1", "F00000ABC2", "F00000ABC3"},
    )

    assert quarantined == 3
    assert validated == 97
