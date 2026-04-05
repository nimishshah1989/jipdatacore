"""Unit tests for the Equity Ingestion Pipeline."""

import pytest
from datetime import date
from decimal import Decimal
from uuid import uuid4

from app.pipelines.equity.bhav import detect_bhav_format, parse_bhav_content
from app.pipelines.equity.eod import EquityEodPipeline

def test_detect_bhav_format():
    """Test format detection across NSE historical, standard, and UDiFF formats."""
    # Pre-2010 format
    legacy_header = ["SYMBOL", "SERIES", "OPEN", "HIGH", "LOW", "CLOSE", "LAST", "PREVCLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES"]
    assert detect_bhav_format(legacy_header) == "pre-2010"
    
    # Standard 2010-2024 format
    std_header = ["SYMBOL", "SERIES", "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE", "CLOSE_PRICE", "PREV_CLOSE", "TOTTRDQTY", "TOTTRDVAL", "TIMESTAMP", "TOTALTRADES", "ISIN"]
    assert detect_bhav_format(std_header) == "standard"
    
    # Modern UDiFF format
    udiff_header = ["BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN", "TckrSymb", "SctySrs", "OpnPric", "HghPric", "LwPric", "ClsPric", "TtlTradgVol", "TotNoOfTrds", "TrdStsInd"]
    assert detect_bhav_format(udiff_header) == "udiff"


def test_parse_bhav_content_udiff():
    """Validate parsing logic correctly maps modern UDiFF columns."""
    sample_csv = b"""BizDt,TckrSymb,SctySrs,OpnPric,HghPric,LwPric,ClsPric,TtlTradgVol,TotNoOfTrds,TrdStsInd
2024-07-01,RELIANCE,EQ,3000.00,3050.00,2980.50,3025.15,1500000,56000,Active
2024-07-01,TCS,EQ,4000.00,4100.00,3950.00,4050.50,1200000,45000,Active
2024-07-01,WIPRO,BE,450.00,460.00,445.00,452.00,500000,12000,Active
"""
    parsed = parse_bhav_content(sample_csv)
    assert len(parsed) == 3
    
    # Verify mapping
    rel = next(r for r in parsed if r["symbol"] == "RELIANCE")
    assert rel["series"] == "EQ"
    assert rel["close"] == 3025.15
    assert rel["volume"] == 1500000
    assert rel["trades"] == 56000


@pytest.mark.asyncio
async def test_eod_pipeline_validation(mocker):
    """Test validation logic quarantines bad data strictly."""
    # Mock the run_log
    mock_run_log = mocker.MagicMock()
    mock_run_log.id = 1
    
    # Create fake anomalous row
    mock_row = mocker.MagicMock()
    mock_row.instrument_id = uuid4()
    mock_row.close = Decimal("-10.0")  # Negative value violation
    mock_row.high = Decimal("100.0")
    mock_row.low = Decimal("120.0")    # High < Low violation
    mock_row.open = Decimal("110.0")
    mock_row.data_status = "raw"
    
    # Mock session
    mock_session = mocker.AsyncMock()
    mock_result = mocker.MagicMock()
    mock_result.scalars.return_value.all.return_value = [mock_row]
    mock_session.execute.return_value = mock_result
    
    pipeline = EquityEodPipeline()
    anomalies = await pipeline.validate(date.today(), mock_session, mock_run_log)
    
    assert len(anomalies) == 2
    assert anomalies[0].severity == "critical"
    assert anomalies[0].anomaly_type == "negative_value"
    assert anomalies[1].anomaly_type == "invalid_ratio"
    
    # Assert row was quarantined
    assert mock_row.data_status == "quarantined"
    mock_session.add.assert_called_with(mock_row)
