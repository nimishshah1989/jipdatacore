"""Unit tests for the index asset wrapper (IND-C8).

Purely structural — no DB connection needed.
Verifies INDEX_SPEC is wired to the correct models, columns, and that
Fix 12 invariant (volume_col=None) holds.

pandas_ta_classic and yaml are Docker-only deps; both are stubbed via
sys.modules BEFORE any indicators_v2 import (engine.py → strategy_loader
imports them at module level).
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock


def _ensure_docker_deps_stubbed() -> None:
    """Stub out Docker-only deps (pandas_ta_classic, yaml) before imports."""
    if "yaml" not in sys.modules:
        yaml_stub = types.ModuleType("yaml")
        yaml_stub.safe_load = MagicMock(return_value=[])  # type: ignore[attr-defined]
        sys.modules["yaml"] = yaml_stub

    if "pandas_ta_classic" not in sys.modules:
        ta_stub = types.ModuleType("pandas_ta_classic")
        ta_stub.Strategy = MagicMock()  # type: ignore[attr-defined]
        sys.modules["pandas_ta_classic"] = ta_stub

    # strategy_loader calls yaml.safe_load and ta.Strategy at module level;
    # stub the whole module so cached lru_cache returns a MagicMock strategy
    if "app.computation.indicators_v2.strategy_loader" not in sys.modules:
        sl_stub = types.ModuleType("app.computation.indicators_v2.strategy_loader")
        sl_stub.load_strategy_for_asset = MagicMock(  # type: ignore[attr-defined]
            return_value=MagicMock(ta=[])
        )
        sl_stub.get_rename_map = MagicMock(return_value={})  # type: ignore[attr-defined]
        sl_stub.get_schema_columns = MagicMock(return_value=[])  # type: ignore[attr-defined]
        sys.modules["app.computation.indicators_v2.strategy_loader"] = sl_stub


# Stub before any indicators_v2 import happens
_ensure_docker_deps_stubbed()


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
