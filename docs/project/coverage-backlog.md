# Coverage backlog (`python/pydantable`)

This document snapshots **statement-line gaps** by area and complements [Testing](/project/testing/). **`mypy_plugin.py`** is omitted from measurement ([`pyproject.toml`](https://github.com/eddiethedean/pydantable/blob/main/pyproject.toml) `[tool.coverage.run] omit`).

## Baseline (full suite, `--cov-fail-under=0`)

Last regeneration (full `pytest` with branch coverage): **~83.1%** total (`pytest-cov` reports **83.15%** on the same run; the terminal `TOTAL` line may show **83%**). About **`1393`** statements still missed (of `10084`; branch coverage enabled). CI **`--cov-fail-under`** is **83** (see `Makefile` / `_shared-ci.yml`). Regenerate:

```bash
.venv/bin/python -m pytest -q -n auto --cov=pydantable --cov-report=term-missing:skip-covered --cov-fail-under=0
```

### Top files by missed statements (refresh from latest `term-missing`)

Rough ordering by **Miss** column (largest gaps first; numbers drift each run):

| Rank | Module | Miss (stmts) | Notes |
|------|--------|----------------|------|
| 1 | `pandas.py` | ~346 | Façade; extend `tests/third_party/` |
| 2 | `dataframe/_impl.py` | ~219 | Core engine paths + errors |
| 3 | `schema/_impl.py` | ~163 | Unions / narrowing edge cases |
| 4 | `pyspark/dataframe.py` | ~139 | Parity smoke tests |
| 5 | `dataframe_model.py` | ~131 | I/O and async helpers |
| 6 | `io/__init__.py` | ~80 | Materialize/export branches |
| 7 | `pyspark/sql/functions.py` | ~53 | Representative calls only |
| 8 | `io/extras.py` | ~42 | Optional SDKs + mocks |
| 9 | `grouped.py` | ~23 | Convenience `sum`/`mean`/… and streaming |
| 10 | `expressions.py` | ~26 | Public `Expr` surfaces |

**Also high value:** `awaitable_dataframe_model.py` (~11 miss), `selectors.py` (~27), `rust_engine.py` (~19).

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
- Raise **`--cov-fail-under`** in CI only after the full suite is green at the new floor (see [`.github/workflows/_shared-ci.yml`](https://github.com/eddiethedean/pydantable/blob/main/.github/workflows/_shared-ci.yml)).
- **Next numeric gate:** increase **`83` → `84`** once **`pytest-cov` total coverage is ≥ 84.0%** on the Ubuntu + Python 3.11 leg (local numbers may differ slightly).
- **Large PRs:** run **`make test-cov`** (or download **`coverage.xml`** from CI), then **`make diff-cover`** so new/changed lines stay covered vs **`origin/main`** (see [Testing](/project/testing/)). The Makefile uses **`--fail-under=85`** on touched lines; raise that gradually as overall coverage climbs so new code stays tighter than legacy gaps.
