# Pydantable

**Typed dataframe transformations for FastAPI + Pydantic services, powered by a Rust execution core.**

Pydantable keeps your Pydantic schemas as the source of truth for:

- column types + nullability (`Optional[T]`)
- expression validity (type errors fail early during AST building)
- derived schema migration through chained transforms

Execution always uses the Rust core; optional `pandas` / `pyspark` **UI modules** only change naming/imports (see `docs/EXECUTION.md`).

## What You Get

Typed, schema-safe transforms:

- `DataFrameModel.with_columns(...)`
- `DataFrameModel.select(...)`
- `DataFrameModel.filter(...)`
- `DataFrameModel.join(...)`
- `DataFrameModel.group_by(...).agg(...)`
- `DataFrameModel.collect()` for materialization into a list of Pydantic row models (current schema)
- `DataFrameModel.to_dict()` for columnar `dict[str, list]` data
- `DataFrameModel.rows()` (alias of `collect()`) and `DataFrameModel.to_dicts()` for row-wise workflows

## Default API (Polars-style contract)

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

Running the three import lines above prints nothing; it only binds names.

### `pandas` / `pyspark` interface modules

`pydantable.pandas` and `pydantable.pyspark` are **alternate import surfaces**
(pandas- or PySpark-style naming where applicable). Execution is always the Rust
core. See `docs/EXECUTION.md`.

For PySpark-style projection helpers (`withColumn`, `withColumnRenamed`, `toDF`,
`transform`, `select_typed`), see `docs/PYSPARK_INTERFACE.md`.

See:

- `docs/EXECUTION.md`
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

result = df4.to_dict()
print(result)
```

Output from `print(result)`:

```text
{'id': [1], 'age2': [40]}
```

## Semantics Contract (high level)

Null semantics are SQL-like (`propagate_nulls`):

- arithmetic: `NULL` + anything yields `NULL`
- comparisons: if either side is `NULL`, the comparison result is `NULL`
- `filter(condition)`: keeps rows where the condition evaluates to exactly `True`

Collision + ordering are explicit:

- `with_columns(...)` uses collision replacement semantics for deterministic schema evolution
- `join(..., suffix=...)` renames right-side non-key overlaps with the suffix
- `to_dict()` / `collect(as_lists=True)` row order is not guaranteed; compare by key columns when needed

For the full contract details:

- `docs/INTERFACE_CONTRACT.md`

## Installation

Pydantable requires Python `3.10+`.

From this repo:

```bash
pip install .
```

`pip install .` builds the Rust extension via `maturin` when toolchains are
available. The Rust extension is required for expression typing and execution.
The Python `polars` package is optional; install with `pip install 'pydantable[polars]'`
if you need `DataFrame.to_polars()`.

## Development & CI

- Format + lint: `ruff format .` and `ruff check .`
- Tests: `pytest -q`
- CI runs the full test suite against the Rust extension.
- Benchmarks vs Polars/pandas: use a **release** native build (`maturin develop --release` or `./benchmarks/run_release.sh`); see `docs/DEVELOPER.md`.

## Docs

- `docs/EXECUTION.md` for the Rust execution model and UI modules
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

