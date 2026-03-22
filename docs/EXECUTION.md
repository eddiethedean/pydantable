# Execution (Rust engine)

All materialization — `collect()`, joins, group-by, reshape, etc. — runs through the
**compiled Rust extension** (`pydantable._core`), which uses Polars for physical execution
inside the native extension. Python does **not** require the `polars` package for core use.

By default, `collect()` returns a **list of Pydantic models** (one per row), validated
against the current projected schema. Use **`to_dict()`** or **`collect(as_lists=True)`**
for a columnar **`dict[str, list]`**. Install **`pydantable[polars]`** and use **`to_polars()`**
if you need a Polars **`DataFrame`** in Python.

The Python module `python/pydantable/rust_engine.py` is the thin wrapper that invokes
`execute_plan`, `execute_join`, and related functions on `_core` (no alternate engines).

Optional **UI modules** (`pydantable.pandas`, `pydantable.pyspark`) only change **method
names and imports** (e.g. `assign` vs `withColumn`). They do not select a different
execution engine.

Use the default package exports for Polars-style names:

```python
from pydantable import DataFrameModel
```

Use explicit submodules for pandas- or PySpark-flavored names:

```python
from pydantable.pandas import DataFrameModel
from pydantable.pyspark import DataFrameModel
```

These import lines only load symbols; executing them in a REPL prints nothing.

See also `docs/PANDAS_UI.md` and `docs/PYSPARK_UI.md`.
