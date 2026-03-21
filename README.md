# Pydantable

**Typed dataframe transformations for FastAPI + Pydantic services, powered by a Rust execution core.**

Pydantable keeps your Pydantic schemas as the source of truth for:

- column types + nullability (`Optional[T]`)
- expression validity (type errors fail early during AST building)
- derived schema migration through chained transforms

Execution is dispatched through a backend boundary (default: Polars-style contract), with optional interface modules for `pandas` and `pyspark`.

## What You Get

Typed, schema-safe transforms:

- `DataFrameModel.with_columns(...)`
- `DataFrameModel.select(...)`
- `DataFrameModel.filter(...)`
- `DataFrameModel.join(...)`
- `DataFrameModel.group_by(...).agg(...)`
- `DataFrameModel.collect()` for materialization into Python column data
- `DataFrameModel.rows()` and `DataFrameModel.to_dicts()` for row-wise materialization

## Backend Boundary (Polars-style by default)

Pydantable’s *default* exported interface emulates a Polars-style dataframe contract:

- join collision handling via `suffix` for right-side non-key columns
- SQL-like null propagation rules for arithmetic/comparisons/filter
- ordering is not a stable API guarantee (tests compare deterministically on keys)

### Select an interface module (import-based)

```python
from pydantable.pandas import DataFrameModel as PandasDataFrameModel
from pydantable.pyspark import DataFrameModel as PySparkDataFrameModel
from pydantable import DataFrameModel as DefaultDataFrameModel
```

### Select the backend at import time (env-var based)

```python
import os
os.environ["PYDANTABLE_BACKEND"] = "polars"  # or "pandas" / "pyspark"
```

Then:

```python
from pydantable import DataFrameModel
```

### `pandas` / `pyspark` interface modules

`pydantable.pandas` and `pydantable.pyspark` are **alternate import surfaces**
(pandas- or PySpark-style naming where applicable). The `pyspark` path uses the
Rust core for execution. When `PYDANTABLE_BACKEND=pandas`, `execute_plan` uses
the optional pandas runtime; other operations still use Rust. See `docs/BACKENDS.md`.

For PySpark-style projection helpers (`withColumn`, `withColumnRenamed`, `toDF`,
`transform`, `select_typed`), see `docs/PYSPARK_INTERFACE.md`.

See:

- `docs/BACKENDS.md`
- `docs/INTERFACE_CONTRACT.md`

## Quick Start

```python
from pydantable import DataFrameModel

class User(DataFrameModel):
    id: int
    age: int | None

df = User({"id": [1, 2], "age": [20, None]})

df2 = df.with_columns(age2=df.age * 2)
df3 = df2.select("id", "age2")
df4 = df3.filter(df3.age2 > 10)

result = df4.collect()
print(result)  # {"id": [1], "age2": [40]}
```

## Semantics Contract (high level)

Null semantics are SQL-like (`propagate_nulls`):

- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the comparison result is `NULL`
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`

Collision + ordering are explicit:

- `with_columns(...)` uses collision replacement semantics for deterministic schema evolution
- `join(..., suffix=...)` renames right-side non-key overlaps with the suffix
- `collect()` row order is not guaranteed; compare by key columns when needed

For the full contract details:

- `docs/INTERFACE_CONTRACT.md`

## Installation

Pydantable requires Python `3.10+`.

From this repo:

```bash
pip install .
```

`pip install .` builds the Rust extension via `maturin` when toolchains are
available. The current skeleton requires the Rust extension for expression
typing and `collect()`.

## Development & CI

- Lint: `ruff check .`
- Tests: `pytest -q`
- CI runs the same test suite across backend selections via `PYDANTABLE_BACKEND`.

## Docs

- `docs/DATAFRAMEMODEL.md` for the `DataFrameModel` contract/design spec
- `docs/FASTAPI.md` for end-to-end FastAPI integration examples
- `docs/WHY_NOT_POLARS.md` for positioning + trade-offs
- `docs/DEVELOPER.md` for local setup and contribution workflow
- `docs/ROADMAP.md` for project phases
- `docs/PARITY_SCORECARD.md` for parity coverage status
- `docs/POLARS_WORKFLOWS.md` for end-to-end Polars-style workflows
- `docs/PYSPARK_INTERFACE.md` for PySpark interface usage and status

## License

MIT

