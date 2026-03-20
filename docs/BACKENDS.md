# Execution Backends

Pydantable exposes a single typed DataFrame API, but execution is dispatched
through a selectable backend boundary.

## Default (Polars-style)

Use the default exports:

```python
from pydantable import DataFrameModel
```

This default interface is backed by the Rust/Polars execution core.

## Optional interface modules

Pydantable also provides import-based interface modules:

```python
from pydantable.pandas import DataFrameModel  # pandas interface
from pydantable.pyspark import DataFrameModel  # pyspark interface
```

These interfaces keep the same typed API and contracts, while selecting a
different backend name in the Python dispatch layer.

## Pandas backend (`PYDANTABLE_BACKEND=pandas` or `pydantable.pandas`)

When the pandas backend is selected, `collect()`, joins, and group-by
aggregations are executed by a pandas-based executor that replays the logical
plan (serialized from Rust) with SQL-like null semantics aligned to
`docs/INTERFACE_CONTRACT.md`.

Install the optional dependency:

```bash
pip install "pydantable[pandas]"
```

CI installs `pandas` for the Python test job so backend equivalence tests run.

## Pandas-flavored API (`from pydantable.pandas import ...`)

The `pydantable.pandas` module adds pandas-like names on top of the same typed
engine:

- `assign(**kwargs)` — same as `with_columns` (no pandas callables/Series)
- `merge(...)` — maps to `join` (`suffixes[1]` → `suffix`; no `left_on` /
  `right_on` / `indicator` / `validate` yet)
- `columns`, `shape`, `empty`, `dtypes`, `head` / `tail` (eager), `__getitem__`
  for `str` or `list[str]`
- `group_by(...).sum(...)` / `.mean(...)` / `.count(...)` as shortcuts to
  `agg(...)`

Not supported (use typed `filter(Expr)` instead):

- string `query()`
- index / `loc` / `iloc`
- arbitrary `assign` callables or `pandas.Series` values

## PySpark interface module

`pydantable.pyspark` keeps the typed API boundary; execution still uses the
Rust/Polars core until a PySpark executor is wired.
