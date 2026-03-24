# Execution (Rust engine)

All materialization — `collect()`, joins, group-by, reshape, etc. — runs through the
**compiled Rust extension** (`pydantable._core`), which uses Polars for physical execution
inside the native extension. Python does **not** require the `polars` package for core use.

**Synchronous materialization (default):** **`collect()`**, **`to_dict()`**, **`collect(as_lists=True)`**, **`collect(as_numpy=True)`**, and optional **`to_polars()`** run **blocking** Rust + Polars work on the **current thread**.

**Async materialization (0.15.0+):** **`await acollect()`**, **`await ato_dict()`**, and **`await ato_polars()`** on **`DataFrame`** run the same logic in a **worker thread** via **`asyncio.to_thread`**, or in a **`concurrent.futures.Executor`** passed as **`executor=`**. **`DataFrameModel`** mirrors this with **`acollect`**, **`ato_dict`**, **`ato_polars`**, **`arows`**, and **`ato_dicts`**. Cancelling the awaiting task does **not** cancel in-flight native work. The **GIL** still serializes some Python callbacks; **`ato_polars()`** builds a Polars frame from a materialized columnar **`dict`** (extra allocation vs calling Polars alone). There is no async **file / IPC** API on the Python package yet. Service patterns: {doc}`FASTAPI` and {doc}`ROADMAP`.

By default, `collect()` returns a **list of Pydantic models** (one per row), validated
against the current projected schema. Use **`to_dict()`** or **`collect(as_lists=True)`**
for a columnar **`dict[str, list]`**. Install **`pydantable[polars]`** and use **`to_polars()`**
if you need a Polars **`DataFrame`** in Python.

The Python module `python/pydantable/rust_engine.py` is the thin wrapper that invokes
`execute_plan`, `execute_join`, and related functions on `_core` (no alternate engines).

Optional **UI modules** (`pydantable.pandas`, `pydantable.pyspark`) only change **method
names and imports** (e.g. `assign` vs `withColumn`). They do not select a different
execution engine.

**Typed expressions** (`Expr`, `Column`, PySpark `F.col(...)`) are validated in Rust
(`ExprNode`), then lowered to Polars inside the extension. The expression and window surface
has grown across releases (globals, framed windows, maps, temporal helpers, multi-key
**`rangeBetween`**, etc.). The authoritative feature list and semantics are {doc}`INTERFACE_CONTRACT`,
{doc}`WINDOW_SQL_SEMANTICS`, {doc}`SUPPORTED_TYPES`, and {doc}`changelog`.

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
