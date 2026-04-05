# Coverage backlog (`python/pydantable`)

This document snapshots **statement-line gaps** by area and complements [Testing](TESTING.md). **`mypy_plugin.py`** is omitted from measurement ([`pyproject.toml`](../pyproject.toml) `[tool.coverage.run] omit`).

## Baseline (full suite, `--cov-fail-under=0`)

Typical totals are around **~80%** statements with **`branch = true`**; exact numbers drift as tests change. Regenerate:

```bash
.venv/bin/python -m pytest -q -n auto --cov=pydantable --cov-report=term-missing:skip-covered --cov-fail-under=0
```

## Per-package focus (highest missing lines first)

| Area | Modules (examples) | Notes |
|------|-------------------|--------|
| **Facades** | `pandas.py`, `pyspark/dataframe.py`, `pyspark/sql/functions.py` | Large surface; extend `tests/third_party/` and parametrized API smoke tests. |
| **Core DataFrame** | `dataframe/_impl.py`, `dataframe_model.py` | Regression tests per feature; error paths via stub engine / invalid inputs. |
| **Schema** | `schema/_impl.py` | Narrowing, unions, edge dtypes; pairs with typing tests where helpful. |
| **I/O** | `io/__init__.py`, `io/extras.py` | Extras need optional deps (Excel, Arrow, cloud mocks); use `optional_cloud` / env gates. |
| **Async model** | `awaitable_dataframe_model.py` | Async materialization routes; mirror sync tests where possible. |
| **Expressions** | `expressions.py` | Focus on uncovered branches tied to public `Expr` APIs. |
| **Small modules** | `engine/_binding.py`, `types.py`, `redaction.py`, … | Cheap wins; keep `tests/dataframe/test_coverage_supplement.py` style tests. |

## Policy

- Prefer **`# pragma: no cover`** only for truly unreachable defensive code.
- Raise **`--cov-fail-under`** in CI only after the full suite is green at the new floor (see [`.github/workflows/_shared-ci.yml`](../.github/workflows/_shared-ci.yml)).
