"""Unit tests for the index asset wrapper (IND-C8).

Purely structural — no DB connection needed.
Verifies INDEX_SPEC is wired to the correct models, columns, and that
Fix 12 invariant (volume_col=None) holds.
"""

from __future__ import annotations


def test_index_spec_no_volume() -> None:
    """Fix 12 invariant: INDEX_SPEC.volume_col must be None.

    Sectoral/broad/thematic indices have no meaningful aggregate volume.
    Setting volume_col=None causes the strategy loader to filter out all
    volume-dependent indicators (OBV, CMF, MFI, VWAP, etc.).
    """
    from app.computation.indicators_v2.assets.index_ import INDEX_SPEC

    assert INDEX_SPEC.volume_col is None, (
        "Fix 12 violated: INDEX_SPEC.volume_col must be None to exclude volume "
        "indicators for index asset class"
    )


def test_index_spec_output_model_tablename() -> None:
    """INDEX_SPEC must point at 'de_index_technical_daily' (no V2 suffix).

    This is a greenfield table — the naming convention deliberately omits
    the _v2 suffix unlike the equity/ETF tables which have legacy counterparts.
    """
    from app.computation.indicators_v2.assets.index_ import INDEX_SPEC

    assert INDEX_SPEC.output_model.__tablename__ == "de_index_technical_daily", (
        f"Expected 'de_index_technical_daily', got '{INDEX_SPEC.output_model.__tablename__}'"
    )


def test_index_spec_id_column_is_index_code() -> None:
    """INDEX_SPEC must use 'index_code' as id_column and 'date' as date_column."""
    from app.computation.indicators_v2.assets.index_ import INDEX_SPEC

    assert INDEX_SPEC.id_column == "index_code"
    assert INDEX_SPEC.date_column == "date"
    assert INDEX_SPEC.asset_class_name == "index"
