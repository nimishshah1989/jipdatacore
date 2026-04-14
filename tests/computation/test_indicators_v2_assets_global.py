"""Unit tests for the global instruments asset wrapper (IND-C7).

These tests are purely structural — no DB connection needed.
They verify that GLOBAL_SPEC is wired to the correct models, columns,
and that the public interface is importable and correctly typed.

pandas_ta_classic is Docker-only; we stub it via sys.modules before
importing any module in the indicators_v2 import chain (engine.py
imports it at module level).
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock


def _ensure_pandas_ta_classic_stubbed() -> None:
    """Stub out pandas_ta_classic (Docker-only dep) so local imports work."""
    if "pandas_ta_classic" not in sys.modules:
        stub = types.ModuleType("pandas_ta_classic")
        stub.Strategy = MagicMock()  # type: ignore[attr-defined]
        sys.modules["pandas_ta_classic"] = stub


# Stub before any indicators_v2 import happens
_ensure_pandas_ta_classic_stubbed()


def test_global_spec_columns_match_model() -> None:
    """Every column referenced in GLOBAL_SPEC must exist on DeGlobalPrices.__table__.columns."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC
    from app.models.prices import DeGlobalPrices

    table_cols = {c.name for c in DeGlobalPrices.__table__.columns}

    def check_colref(ref: object, label: str) -> None:
        if ref is None:
            return
        if isinstance(ref, tuple):
            for name in ref:
                assert name in table_cols, (
                    f"GLOBAL_SPEC.{label} references '{name}' "
                    f"which is not in DeGlobalPrices.__table__.columns"
                )
        else:
            assert ref in table_cols, (
                f"GLOBAL_SPEC.{label} references '{ref}' "
                f"which is not in DeGlobalPrices.__table__.columns"
            )

    check_colref(GLOBAL_SPEC.close_col, "close_col")
    check_colref(GLOBAL_SPEC.open_col, "open_col")
    check_colref(GLOBAL_SPEC.high_col, "high_col")
    check_colref(GLOBAL_SPEC.low_col, "low_col")
    check_colref(GLOBAL_SPEC.volume_col, "volume_col")


def test_global_spec_output_model_is_v2() -> None:
    """GLOBAL_SPEC must point at the v2 output table."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC
    from app.models.indicators_v2 import DeGlobalTechnicalDailyV2

    assert GLOBAL_SPEC.output_model is DeGlobalTechnicalDailyV2
    assert GLOBAL_SPEC.output_model.__tablename__ == "de_global_technical_daily_v2"


def test_global_spec_id_column_is_ticker() -> None:
    """id_column must be 'ticker' and date_column must be 'date'."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC

    assert GLOBAL_SPEC.id_column == "ticker"
    assert GLOBAL_SPEC.date_column == "date"


def test_global_spec_asset_class_name() -> None:
    """asset_class_name must be 'global' — strategy.yaml filtering depends on this."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC

    assert GLOBAL_SPEC.asset_class_name == "global"


def test_global_spec_source_model_is_global_prices() -> None:
    """GLOBAL_SPEC source must be DeGlobalPrices."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC
    from app.models.prices import DeGlobalPrices

    assert GLOBAL_SPEC.source_model is DeGlobalPrices
    assert GLOBAL_SPEC.source_model.__tablename__ == "de_global_prices"


def test_global_spec_min_history_days() -> None:
    """Min history must be 100 rows — lower than equity (250) to accommodate crypto/forex."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC

    assert GLOBAL_SPEC.min_history_days == 100


def test_global_spec_no_adj_columns() -> None:
    """de_global_prices has no _adj columns — specs must use plain names, not tuples."""
    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC

    assert GLOBAL_SPEC.close_col == "close"
    assert GLOBAL_SPEC.open_col == "open"
    assert GLOBAL_SPEC.high_col == "high"
    assert GLOBAL_SPEC.low_col == "low"
    assert GLOBAL_SPEC.volume_col == "volume"


def test_global_spec_is_frozen() -> None:
    """AssetSpec is a frozen dataclass — should raise on mutation attempt."""
    from dataclasses import FrozenInstanceError

    from app.computation.indicators_v2.assets.global_ import GLOBAL_SPEC

    try:
        GLOBAL_SPEC.close_col = "close_adj"  # type: ignore[misc]
        raise AssertionError("Expected FrozenInstanceError was not raised")
    except (FrozenInstanceError, AttributeError):
        pass  # expected


def test_public_functions_importable() -> None:
    """All public callables must import without side effects and be coroutines."""
    import inspect

    from app.computation.indicators_v2.assets.global_ import (  # noqa: F401
        GLOBAL_SPEC,
        compute_global_indicators,
        load_active_global_tickers,
    )

    assert inspect.iscoroutinefunction(compute_global_indicators)
    assert inspect.iscoroutinefunction(load_active_global_tickers)


def test_assets_package_re_exports_global() -> None:
    """The assets __init__ must expose the global_ module."""
    import app.computation.indicators_v2.assets as assets_pkg

    assert hasattr(assets_pkg, "global_")
