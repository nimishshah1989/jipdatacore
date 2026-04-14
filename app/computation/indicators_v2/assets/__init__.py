"""Per-asset-class wrappers around the generic indicators engine.

Each module defines a single ``AssetSpec`` and a thin ``compute_*_indicators``
coroutine that binds the spec to the engine. See the parent package for
architectural notes.
"""

from app.computation.indicators_v2.assets import equity  # noqa: F401
