# PydanTable

[![CI](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/pydantable/actions/workflows/ci.yml?query=branch%3Amain)
[![Documentation](https://readthedocs.org/projects/pydantable/badge/?version=latest)](https://pydantable.readthedocs.io/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/pydantable)](https://pypi.org/project/pydantable/)
[![Python versions](https://img.shields.io/pypi/pyversions/pydantable)](https://pypi.org/project/pydantable/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Typed dataframe transformations for FastAPI and Pydantic services, backed by a Rust execution core.**

**Current release: 0.15.0** · Python **3.10+**

---

## Documentation

**The full manual lives on Read the Docs:**

### **[https://pydantable.readthedocs.io/en/latest/](https://pydantable.readthedocs.io/en/latest/)**

That site is the supported entry point for concepts, contracts, API notes, and examples. The sections below point to the same pages so you can jump straight from GitHub.

| Topic | Read the Docs |
|--------|----------------|
| **Home / overview** | [Documentation home](https://pydantable.readthedocs.io/en/latest/index.html) |
| **`DataFrameModel` contract** (inputs, transforms, collisions, materialization) | [DataFrameModel (SQLModel-like)](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) |
| **Column types** (scalars, structs, `list[T]`, nullability, unsupported cases) | [Supported data types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html) |
| **FastAPI** (routers, bodies, `collect`, responses) | [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html) |
| **Execution model** (`collect`, `to_dict`, `to_polars`, optional Python Polars, UI modules) | [Execution (Rust engine)](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) |
| **Semantics** (nulls, joins, ordering, reshaping, windows — Polars-style contract) | [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html) |
| **Roadmap** (0.5.0–0.15.0 shipped, path to v1.0.0) | [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html) |
| **Why not use Polars directly?** | [Why not just use Polars?](https://pydantable.readthedocs.io/en/latest/WHY_NOT_POLARS.html) |
| **Pandas-style imports** (`pydantable.pandas`) | [Pandas UI](https://pydantable.readthedocs.io/en/latest/PANDAS_UI.html) |
| **PySpark-style imports** (`pydantable.pyspark`) | [PySpark UI](https://pydantable.readthedocs.io/en/latest/PYSPARK_UI.html) |
| **PySpark helpers & parity** | [PySpark interface](https://pydantable.readthedocs.io/en/latest/PYSPARK_INTERFACE.html) · [PySpark API parity](https://pydantable.readthedocs.io/en/latest/PYSPARK_PARITY.html) |
| **Polars parity** (scorecard, workflows, transformation roadmap) | [Parity scorecard](https://pydantable.readthedocs.io/en/latest/PARITY_SCORECARD.html) · [Polars-style workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html) · [Transformation parity roadmap](https://pydantable.readthedocs.io/en/latest/POLARS_TRANSFORMATIONS_ROADMAP.html) |
| **Contributors** (build, test, benchmarks, releases) | [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) |
| **Plan / vision** (architecture phasing) | [Plan document](https://pydantable.readthedocs.io/en/latest/pydantable_plan.html) |
| **Python API reference** (autodoc) | [API reference](https://pydantable.readthedocs.io/en/latest/api/index.html) |

For copy-paste convenience, the site base URL is:

`https://pydantable.readthedocs.io/en/latest/`

---

## What PydanTable does

PydanTable keeps **Pydantic models** as the source of truth for:

- column types and nullability (`Optional[T]` / `T | None`)
- **typed expressions** — invalid combinations fail when the expression is built (Rust AST), not only at runtime
- **schema evolution** — chained transforms produce new model types with stable rules (e.g. `with_columns` name collisions)

The default API feels **Polars-like**; optional **`pydantable.pandas`** and **`pydantable.pyspark`** modules only change naming and imports — execution is always the native core. Details: [Execution](https://pydantable.readthedocs.io/en/latest/EXECUTION.html), [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html).

**0.15.0** adds **async materialization** (`acollect`, `ato_dict`, `ato_polars`, and `DataFrameModel` `arows` / `ato_dicts`), **FastAPI `async` + `lifespan`** examples, **PyArrow `map<utf8, …>`** ingest for **`dict[str, T]`** columns, and PySpark **`trim` / `abs` / `round` / `floor` / `ceil`**. **0.14.0** added window **`orderBy(..., nulls_last=...)`**, **`DtypeDriftWarning`**, **`validate_data`** deprecation (removal after **0.16.0**), **FastAPI `TestClient`** docs/tests, and PySpark **`dayofmonth` / `lower` / `upper`**. See [changelog](https://pydantable.readthedocs.io/en/latest/changelog.html) and [Roadmap](https://pydantable.readthedocs.io/en/latest/ROADMAP.html).

**Expression surface (current release, Rust-typed `Expr`):**

- **Globals in `select`:** `global_sum`, `global_mean`, `global_count`, `global_min`, `global_max`, and **`global_row_count()`** (row count / `COUNT(*)`). PySpark: `F.count()` with no argument for row count; `F.count(F.col(...))` for non-null column count.
- **Windows:** `row_number`, `rank`, `dense_rank`, `window_sum`, `window_mean`, **`window_min`**, **`window_max`**, `lag`, `lead` with `Window.partitionBy(...).orderBy(..., nulls_last=...)` / `.spec()`, plus framed windows (`rowsBetween`, `rangeBetween`) for supported operations.
- **Temporal:** `strptime`, `unix_timestamp`, **`cast` from `str` → `date` / `datetime`** (Polars parsing; use `strptime` for fixed formats), `dt_*` parts, `dt_nanosecond` on `datetime` / `time`.
- **Maps / binary:** `map_len`, **`map_get`**, **`map_contains_key`**, `binary_len`, including nested JSON-like map value dtypes with string keys.
- **Map utilities:** `map_keys()`, `map_values()`, `map_entries()`, and `map_from_entries()` for per-row key/value extraction and reconstruction on `dict[str, T]` columns.

PySpark-named helpers live under `pydantable.pyspark.sql.functions`. Details: [Supported types](https://pydantable.readthedocs.io/en/latest/SUPPORTED_TYPES.html), [Interface contract](https://pydantable.readthedocs.io/en/latest/INTERFACE_CONTRACT.html), [CHANGELOG](https://pydantable.readthedocs.io/en/latest/changelog.html).

---

## Install

```bash
pip install pydantable
```

Optional Python **Polars** (for `to_polars()` only):

```bash
pip install 'pydantable[polars]'
```

**From a git checkout**, the Rust extension must be built (e.g. with [maturin](https://www.maturin.rs/)):

```bash
pip install .
```

See [Developer guide — local setup](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) for `maturin develop`, release builds, and CI parity.

---

## Quick start

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int | None

df = User({"id": [1, 2], "age": [20, None]})
df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 10)

print(df4.to_dict())
```

Example output:

```text
{'age2': [40], 'id': [1]}
```

- **Materialization:** [`collect()`](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html) returns a list of Pydantic row models; [`to_dict()`](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) returns columnar `dict[str, list]`. **0.15.0** [`acollect` / `ato_dict` / `ato_polars`](https://pydantable.readthedocs.io/en/latest/EXECUTION.html) run the same work off the asyncio loop.
- **Alternate UIs:**

  ```python
  from pydantable.pandas import DataFrameModel as PandasDataFrameModel
  from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
  from pydantable import DataFrameModel as DefaultDataFrameModel
  ```

More examples: [FastAPI integration](https://pydantable.readthedocs.io/en/latest/FASTAPI.html), [Polars-style workflows](https://pydantable.readthedocs.io/en/latest/POLARS_WORKFLOWS.html).

Input quality policy (optional): constructors are strict by default, and can be
switched to best-effort ingestion with `ignore_errors=True` plus
`on_validation_errors=...` to receive failed rows (`row_index`, `row`,
validation `errors`). See [DataFrameModel docs](https://pydantable.readthedocs.io/en/latest/DATAFRAMEMODEL.html).

---

## Development

```bash
make check-full   # Ruff, mypy, Rust fmt/clippy/tests (see Makefile for `rust-test` env)
pytest -q         # or: pytest -n auto  with the [dev] extra
```

Rust + Python: see [Developer guide](https://pydantable.readthedocs.io/en/latest/DEVELOPER.html) (formatting, `maturin`, `make rust-test` for `cargo test` with the venv `PYTHONPATH`, benchmarks, contribution workflow).

---

## License

MIT
