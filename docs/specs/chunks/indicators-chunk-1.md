# Chunk 1 — Dependencies & package scaffold

**Complexity**: S
**Blocks**: chunk-2
**Blocked by**: —

## Goal
Add `pandas-ta-classic` and `empyrical-reloaded` to project dependencies and create the empty `app/computation/indicators_v2/` package. Verify both imports work cleanly alongside existing deps.

## Files
- **Modify**: `pyproject.toml` — add `pandas-ta-classic>=0.3.14`, `empyrical-reloaded>=0.5.10` to main dependencies (not dev)
- **Create**: `app/computation/indicators_v2/__init__.py` — empty package marker with module docstring
- **Create**: `app/computation/indicators_v2/assets/__init__.py` — empty subpackage

## Implementation notes
- Use the project's existing dependency manager (check if `poetry lock` or `pip-compile` or plain `pip install -e .`)
- pandas-ta-classic is on PyPI under that exact name
- empyrical-reloaded is on PyPI under that exact name (maintained fork)
- Do NOT add TA-Lib to runtime deps — it's test-only and lives in a separate Dockerfile (chunk 4)

## Acceptance criteria
- `pip install -e .` (or equivalent) completes clean
- `python -c "import pandas_ta; print(pandas_ta.__version__)"` succeeds
- `python -c "import empyrical; print(empyrical.__version__)"` succeeds
- `python -c "from app.computation.indicators_v2 import assets"` succeeds
- `pytest tests/ -v --tb=short` — existing tests still all green
- `ruff check . --select E,F,W` — clean
- `mypy . --ignore-missing-imports` — clean

## Verification commands
```bash
pip install -e .
python -c "import pandas_ta, empyrical; print('ok')"
pytest tests/computation/ -v
```
