"""Unit tests for the equity asset wrapper (IND-C5).

These tests are purely structural — no DB connection needed.
They verify that EQUITY_SPEC is wired to the correct models, columns,
and that the public interface is importable and correctly typed.
"""

from __future__ import annotations


def test_equity_spec_close_col_is_close_adj() -> None:
    """EQUITY_SPEC must prefer adjusted columns with raw fallback via COALESCE."""
    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC

    # COALESCE tuples — adjusted first, raw fallback second.
    assert EQUITY_SPEC.close_col == ("close_adj", "close")
    assert EQUITY_SPEC.open_col == ("open_adj", "open")
    assert EQUITY_SPEC.high_col == ("high_adj", "high")
    assert EQUITY_SPEC.low_col == ("low_adj", "low")
    assert EQUITY_SPEC.volume_col == ("volume_adj", "volume")
    assert EQUITY_SPEC.id_column == "instrument_id"
    assert EQUITY_SPEC.date_column == "date"


def test_equity_spec_output_model_is_v2() -> None:
    """EQUITY_SPEC must point at the v2 output table, not the legacy one."""
    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC
    from app.models.indicators_v2 import DeEquityTechnicalDailyV2

    assert EQUITY_SPEC.output_model is DeEquityTechnicalDailyV2
    assert EQUITY_SPEC.output_model.__tablename__ == "de_equity_technical_daily_v2"


def test_equity_spec_source_model_is_ohlcv() -> None:
    """EQUITY_SPEC source must be the partitioned equity OHLCV table."""
    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC
    from app.models.prices import DeEquityOhlcv

    assert EQUITY_SPEC.source_model is DeEquityOhlcv
    assert EQUITY_SPEC.source_model.__tablename__ == "de_equity_ohlcv"


def test_equity_spec_asset_class_name() -> None:
    """asset_class_name must be 'equity' — strategy.yaml filtering depends on this."""
    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC

    assert EQUITY_SPEC.asset_class_name == "equity"


def test_equity_spec_min_history_days() -> None:
    """Min history must be 250 rows (~1 trading year) to skip thin-history instruments."""
    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC

    assert EQUITY_SPEC.min_history_days == 250


def test_equity_spec_is_frozen() -> None:
    """AssetSpec is a frozen dataclass — should raise on mutation attempt."""
    from dataclasses import FrozenInstanceError

    from app.computation.indicators_v2.assets.equity import EQUITY_SPEC

    try:
        EQUITY_SPEC.close_col = "close"  # type: ignore[misc]
        raise AssertionError("Expected FrozenInstanceError was not raised")
    except (FrozenInstanceError, AttributeError):
        pass  # expected


def test_public_functions_importable() -> None:
    """All public callables must import without side effects."""
    import inspect

    from app.computation.indicators_v2.assets.equity import (  # noqa: F401
        EQUITY_SPEC,
        compute_equity_indicators,
        load_active_equity_ids,
        load_nifty50_benchmark,
    )

    assert inspect.iscoroutinefunction(compute_equity_indicators)
    assert inspect.iscoroutinefunction(load_active_equity_ids)
    assert inspect.iscoroutinefunction(load_nifty50_benchmark)


def test_assets_package_re_exports_equity() -> None:
    """The assets __init__ must expose the equity module."""
    import app.computation.indicators_v2.assets as assets_pkg

    assert hasattr(assets_pkg, "equity")
