"""AssetSpec — frozen dataclass binding an asset class to its source/output models.

Row-position semantics (Fix 6, binding per eng-review addendum):
    Indicator windows are based on row position in the DataFrame. Calendar gaps
    (trading halts, suspensions, weekends) are NOT backfilled. A 10-day suspension
    gap in OHLCV means the SMA_50 value immediately after the gap will average the
    50 rows that appear in the table, spanning both sides of the gap. This matches
    how most Indian market data vendors report historical prices. The engine enforces
    that input dates are monotonically increasing and raises AssertionError if not.
    It does NOT reorder, reindex, or fill missing calendar dates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Type


@dataclass(frozen=True)
class AssetSpec:
    """Describes one asset class's source and output tables for the indicators engine.

    All fields are read-only after construction (frozen=True) — specs are safe
    to share across coroutines without copying.

    Attributes:
        asset_class_name: One of {"equity", "etf", "index", "mf", "global"}.
            Used to filter strategy.yaml entries and select the rename map.
        source_model: SQLAlchemy model class for the OHLCV source table.
        output_model: SQLAlchemy model class for the technical indicator target table.
        id_column: Column name for the instrument identifier in both source and output
            tables (e.g. "instrument_id" for equity, "ticker" for ETF/global,
            "index_code" for index, "mstar_id" for MF).
        date_column: Column name for the date/nav_date in both source and output
            tables (e.g. "date" for most, "nav_date" for MF).
        close_col: Column name for the close price in the source model
            (e.g. "close_adj" for equity, "nav" for MF).
        open_col: Column name for open price; None for MF (single-price asset).
        high_col: Column name for high price; None for MF.
        low_col: Column name for low price; None for MF.
        volume_col: Column name for volume; None for index and MF (Fix 12/13).
            When None, all strategy.yaml entries with requires_volume=true are skipped.
        min_history_days: Minimum number of rows an instrument must have before the
            engine computes indicators for it. Instruments with fewer rows are skipped
            and counted in CompResult.instruments_skipped_insufficient_history.
            Default 250 (just over one trading year).
    """

    asset_class_name: str
    source_model: Type
    output_model: Type
    id_column: str
    date_column: str
    close_col: str
    open_col: Optional[str]
    high_col: Optional[str]
    low_col: Optional[str]
    volume_col: Optional[str]
    min_history_days: int = 250
