"""Per-asset-class wrappers around the generic indicators engine.

Each module defines a single ``AssetSpec`` and a thin ``compute_*_indicators``
coroutine that binds the spec to the engine. See the parent package for
architectural notes.
"""
