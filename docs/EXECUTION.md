# Execution (Rust engine)

All materialization — `collect()`, joins, group-by, reshape, etc. — runs through the
**compiled Rust extension** (`pydantable._core`), which uses Polars for physical execution
inside the native extension. Python does **not** require the `polars` package for core use.

**Synchronous materialization (default):** **`collect()`**, **`to_dict()`**, **`collect(as_lists=True)`**, **`collect(as_numpy=True)`**, optional **`to_polars()`**, and optional **`to_arrow()`** run **blocking** Rust + Polars work on the **current thread** ( **`to_arrow()`** then builds a PyArrow **`Table`** from the materialized columnar **`dict`** in Python).

**Async materialization (0.15.0+):** **`await acollect()`**, **`await ato_dict()`**, **`await ato_polars()`**, and **`await ato_arrow()`** on **`DataFrame`** run the same logic in a **worker thread** via **`asyncio.to_thread`**, or in a **`concurrent.futures.Executor`** passed as **`executor=`**. **`DataFrameModel`** mirrors this with **`acollect`**, **`ato_dict`**, **`ato_polars`**, **`ato_arrow`**, **`arows`**, and **`ato_dicts`**. Cancelling the awaiting task does **not** cancel in-flight native work. The **GIL** still serializes some Python callbacks; **`ato_polars()`** and **`ato_arrow()`** both build their respective outputs from a materialized columnar **`dict`** (extra allocation vs calling Polars or PyArrow alone on raw buffers).

**File / IPC helpers (0.16.0+):** **`pydantable.read_parquet`** and **`read_ipc`** are **synchronous** PyArrow readers that return **`dict[str, list]`** for constructors. They are **not** async; from **`async def`** routes, either call them directly for small files (briefly blocks the loop) or wrap with **`asyncio.to_thread`** / run on your **`executor=`** pool. Service patterns: {doc}`FASTAPI` and {doc}`ROADMAP`.

By default, `collect()` returns a **list of Pydantic models** (one per row), validated
against the current projected schema. Use **`to_dict()`** or **`collect(as_lists=True)`**
for a columnar **`dict[str, list]`**. Install **`pydantable[polars]`** and use **`to_polars()`**
if you need a Polars **`DataFrame`** in Python. Install **`pydantable[arrow]`** and use **`to_arrow()`** for a PyArrow **`Table`** (same materialization path as **`to_dict`**, then **`Table.from_pydict`**—not a zero-copy export of engine buffers).

The Python module `python/pydantable/rust_engine.py` is the thin wrapper that invokes
`execute_plan`, `execute_join`, and related functions on `_core` (no alternate engines).

**0.18.0 — Grouped execution errors:** When Polars **`collect()`** fails during **`group_by().agg()`**, the raised **`ValueError`** may include the prefix **`Polars execution error (group_by().agg()):`** so the failure is identifiable as grouped aggregation rather than a generic plan step. This does not change aggregation results or schema rules ({doc}`INTERFACE_CONTRACT`).

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

## `repr` (string form)

**`repr(df)`** on **`DataFrame`** (and **`print(df)`**, which uses the same hook when **`__str__`** is not overridden) shows a multi-line summary:

- The parameterized class name (e.g. **`DataFrame[MySchema]`**).
- The current schema type’s **`__qualname__`**.
- A column table: **name** and **dtype** string derived from Pydantic field annotations (`int`, `str`, `float | None`, `Literal[...]`, generics like **`list[int]`**, etc.).

If there are more than **32** columns, only the first **32** are listed, followed by **`… and N more`**.

**Row counts are intentionally omitted.** The logical plan may filter, join, or aggregate; the number of rows in the result is not known without running **`collect()`**, **`to_dict()`**, **`to_polars()`**, or **`to_arrow()`**. **`DataFrameModel`** delegates to the inner **`DataFrame`**; **`GroupedDataFrame`** / **`DynamicGroupedDataFrame`** (and the model wrappers) prepend a short grouping summary before the inner frame.

This is for **REPLs, logs, and tracebacks**—not a substitute for materializing data.

## Jupyter / HTML (`_repr_html_`)

In **Jupyter**, **IPython**, **VS Code** notebooks, and similar frontends, **`DataFrame`** and **`DataFrameModel`** implement **`_repr_html_()`** so the last line of a cell renders as an **HTML table** (pandas-style), without installing **`polars`**.

- **Preview only:** the table shows up to **20** rows and **40** columns (constants in **`pydantable.dataframe._impl`**). Long string or **`repr`** cell values are truncated for display.
- **Materialization:** the preview runs the same engine path as **`head()`** + **`to_dict()`**—it executes the current logical plan for the bounded slice. Large frames still pay that cost for the preview.
- **Safety:** cell text is **HTML-escaped** so arbitrary string data does not inject markup.
- **Grouped handles:** **`GroupedDataFrame`** / **`DynamicGroupedDataFrame`** (and grouped model wrappers) prepend a short label, then show the inner frame preview.
- **Look and feel:** Preview uses a **card**-style layout (rounded corners, light shadow, slate **CSS** variables), **uppercase** column headers, **zebra** row banding, **tabular** row indices, and tinted **banner** strips for **`DataFrameModel`** / grouped contexts—similar in spirit to **pandas** HTML display without depending on it.

For the full dataset, use **`to_dict()`**, **`collect()`**, **`to_polars()`**, or **`to_arrow()`** as usual.
