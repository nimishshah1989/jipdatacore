"""Tests for the Mutual Fund EOD Orchestrator Pipeline."""

import pytest
from datetime import date
from unittest.mock import AsyncMock, patch

from app.pipelines.mf.eod import MfEodPipeline
from app.pipelines.framework import ExecutionResult


@pytest.fixture
def mock_amfi_response():
    return (
        "Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date\r\n"
        "120503;INF205K01UP5;-;Aditya Birla Sun Life Frontline Equity Fund;579.2;05-Apr-2026\r\n"
        "100373;INF200K01130;-;SBI Bluechip Fund;421.5;05-Apr-2026\r\n"
        "119598;INF204K01VR0;-;Nippon India Growth Fund;3142.1;05-Apr-2026\r\n"
        "999999;INVALID;-;Invalid Bad NAV Fund;N.A.;05-Apr-2026\r\n"
    )

@pytest.mark.asyncio
async def test_mf_amfi_parser():
    """Verify that the AMFI parser correctly skips headers and N.A. values."""
    from app.pipelines.mf.amfi import parse_amfi_nav
    
    mock_text = (
        "Scheme Code;ISIN;Scheme Name;NAV;Date\n"
        "120503;INF205K01UP5;Fund A;100.0;05-Apr-2026\n"
        "120504;INF205K01UP6;Fund B;N.A.;05-Apr-2026\n"
        "Open Ended Schemes (Equity Scheme - Large Cap Fund)\n"
    )
    
    parsed = parse_amfi_nav(mock_text)
    assert len(parsed) == 1
    assert parsed[0]["amfi_code"] == "120503"
    assert parsed[0]["nav"] == 100.0


@pytest.mark.asyncio
async def test_mf_validation_gates(mocker):
    """Test GSD quality loops: enforce quarantine rule for zero or negative NAVs."""
    pipeline = MfEodPipeline()
    bus_date = date(2026, 4, 5)
    
    class MockSession:
        async def execute(self, *args, **kwargs):
            return self
        
        def scalars(self):
            class MockRow:
                def __init__(self, val, mstar_id):
                    self.nav = val
                    self.mstar_id = mstar_id
                    self.data_status = "raw"
                    
            return self
            
        def all(self):
            return [MockRow(400.0, "F0GBR04Q4O"), MockRow(-10.0, "F0GBR04DOP")]
            
        def add(self, obj):
            pass
            
        async def commit(self):
            pass

    log_mock = AsyncMock()
    log_mock.id = 1
    
    anomalies = await pipeline.validate(bus_date, MockSession(), log_mock)
    
    assert len(anomalies) == 1
    assert anomalies[0].severity == "critical"
    assert anomalies[0].anomaly_type == "negative_value"
    assert anomalies[0].mstar_id == "F0GBR04DOP"
