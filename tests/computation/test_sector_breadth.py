"""Unit tests for sector breadth rollups.

Golden test on a 3-stock, 2-sector fixture verifying count and percentage logic.
"""

from __future__ import annotations

from decimal import Decimal

import pytest


def _compute_sector_breadth(stocks: list[dict]) -> dict:
    """Pure-Python replica of the SQL aggregation logic for testing."""
    from collections import defaultdict

    sectors: dict[str, list[dict]] = defaultdict(list)
    for s in stocks:
        sectors[s.get("sector") or "Unclassified"].append(s)

    results = {}
    for sector, members in sectors.items():
        total = len(members)
        above_50 = sum(1 for m in members if m.get("above_50dma"))
        above_200 = sum(1 for m in members if m.get("above_200dma"))
        above_20 = sum(1 for m in members if m.get("above_20ema"))
        rsi_ob = sum(1 for m in members if m.get("rsi_overbought"))
        rsi_os = sum(1 for m in members if m.get("rsi_oversold"))
        macd_b = sum(1 for m in members if m.get("macd_bullish"))

        pct_50 = round(Decimal(above_50) / Decimal(total) * 100, 2) if total else Decimal(0)
        pct_200 = round(Decimal(above_200) / Decimal(total) * 100, 2) if total else Decimal(0)
        pct_20 = round(Decimal(above_20) / Decimal(total) * 100, 2) if total else Decimal(0)

        if pct_50 > 70:
            regime = "bullish"
        elif pct_50 < 30:
            regime = "bearish"
        else:
            regime = "neutral"

        results[sector] = {
            "stocks_total": total,
            "stocks_above_50dma": above_50,
            "stocks_above_200dma": above_200,
            "stocks_above_20ema": above_20,
            "pct_above_50dma": pct_50,
            "pct_above_200dma": pct_200,
            "pct_above_20ema": pct_20,
            "stocks_rsi_overbought": rsi_ob,
            "stocks_rsi_oversold": rsi_os,
            "stocks_macd_bullish": macd_b,
            "breadth_regime": regime,
        }
    return results


FIXTURE_STOCKS = [
    {
        "symbol": "RELIANCE",
        "sector": "Energy",
        "above_50dma": True,
        "above_200dma": True,
        "above_20ema": True,
        "rsi_overbought": True,
        "rsi_oversold": False,
        "macd_bullish": True,
    },
    {
        "symbol": "ONGC",
        "sector": "Energy",
        "above_50dma": True,
        "above_200dma": False,
        "above_20ema": True,
        "rsi_overbought": False,
        "rsi_oversold": False,
        "macd_bullish": False,
    },
    {
        "symbol": "INFY",
        "sector": "IT",
        "above_50dma": False,
        "above_200dma": False,
        "above_20ema": False,
        "rsi_overbought": False,
        "rsi_oversold": True,
        "macd_bullish": False,
    },
]


def test_sector_breadth_golden_energy() -> None:
    result = _compute_sector_breadth(FIXTURE_STOCKS)
    energy = result["Energy"]

    assert energy["stocks_total"] == 2
    assert energy["stocks_above_50dma"] == 2
    assert energy["stocks_above_200dma"] == 1
    assert energy["stocks_above_20ema"] == 2
    assert energy["pct_above_50dma"] == Decimal("100.00")
    assert energy["pct_above_200dma"] == Decimal("50.00")
    assert energy["pct_above_20ema"] == Decimal("100.00")
    assert energy["stocks_rsi_overbought"] == 1
    assert energy["stocks_rsi_oversold"] == 0
    assert energy["stocks_macd_bullish"] == 1
    assert energy["breadth_regime"] == "bullish"


def test_sector_breadth_golden_it() -> None:
    result = _compute_sector_breadth(FIXTURE_STOCKS)
    it = result["IT"]

    assert it["stocks_total"] == 1
    assert it["stocks_above_50dma"] == 0
    assert it["stocks_above_200dma"] == 0
    assert it["pct_above_50dma"] == Decimal("0.00")
    assert it["pct_above_200dma"] == Decimal("0.00")
    assert it["stocks_rsi_overbought"] == 0
    assert it["stocks_rsi_oversold"] == 1
    assert it["stocks_macd_bullish"] == 0
    assert it["breadth_regime"] == "bearish"


def test_sector_breadth_two_sectors() -> None:
    result = _compute_sector_breadth(FIXTURE_STOCKS)
    assert len(result) == 2
    assert set(result.keys()) == {"Energy", "IT"}


def test_sector_breadth_neutral_regime() -> None:
    stocks = [
        {"sector": "Banking", "above_50dma": True, "above_200dma": True,
         "above_20ema": True, "rsi_overbought": False, "rsi_oversold": False,
         "macd_bullish": True},
        {"sector": "Banking", "above_50dma": False, "above_200dma": False,
         "above_20ema": False, "rsi_overbought": False, "rsi_oversold": False,
         "macd_bullish": False},
    ]
    result = _compute_sector_breadth(stocks)
    assert result["Banking"]["pct_above_50dma"] == Decimal("50.00")
    assert result["Banking"]["breadth_regime"] == "neutral"


def test_sector_breadth_unclassified() -> None:
    stocks = [
        {"sector": None, "above_50dma": True, "above_200dma": False,
         "above_20ema": True, "rsi_overbought": False, "rsi_oversold": False,
         "macd_bullish": True},
    ]
    result = _compute_sector_breadth(stocks)
    assert "Unclassified" in result
    assert result["Unclassified"]["stocks_total"] == 1
