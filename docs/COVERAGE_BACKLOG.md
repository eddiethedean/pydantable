# Coverage backlog (`python/pydantable`)

This document snapshots **statement-line gaps** by area and complements [Testing](TESTING.md). **`mypy_plugin.py`** is omitted from measurement ([`pyproject.toml`](../pyproject.toml) `[tool.coverage.run] omit`).

## Baseline (full suite, `--cov-fail-under=0`)

Last regeneration (full `pytest` with branch coverage): **~82%** on the `TOTAL` line in the terminal report (combined statement/branch-style percentage; exact value drifts). CI **`--cov-fail-under`** is **82** (see `Makefile` / `_shared-ci.yml`). Regenerate:

```bash
.venv/bin/python -m pytest -q -n auto --cov=pydantable --cov-report=term-missing:skip-covered --cov-fail-under=0
```

### Top files by missed statements (refresh from latest `term-missing`)

Rough ordering by **Miss** column (largest gaps first; numbers drift each run):

| Rank | Module | Miss (stmts) | Notes |
|------|--------|----------------|------|
| 1 | `pandas.py` | ~300–350 | Façade; extend `tests/third_party/` |
| 2 | `dataframe/_impl.py` | ~210–230 | Core engine paths + errors |
| 3 | `schema/_impl.py` | ~150–170 | Unions / narrowing edge cases |
| 4 | `pyspark/dataframe.py` | ~130–150 | Parity smoke tests |
| 5 | `io/__init__.py` | ~75–90 | Materialize/export branches |
| 6 | `io/extras.py` | ~40–55 | Optional SDKs + mocks (improved) |
| 7 | `dataframe_model.py` | ~120–135 | I/O and async helpers |
| 8 | `awaitable_dataframe_model.py` | ~70–80 | Async chains / group_by / join |
| 9 | `pyspark/sql/functions.py` | ~65–75 | Representative calls only |
| 10 | `expressions.py` | ~40–50 | Public `Expr` surfaces |

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
