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

## Materialization costs (summary)

| API | Typical cost |
|-----|----------------|
| **`collect()`**, **`to_dict()`**, **`to_polars()`**, **`to_arrow()`** | Full plan execution in Rust (then Python wrappers build Polars/Arrow objects where applicable). |
| **`head()`** / **`tail()`** / **`slice()`** | Adds a lazy slice to the plan; cost hits when you materialize the result. |
| **`_repr_html_()`** / Jupyter HTML | Materializes **`head(N)`** + **`to_dict()`** for the preview bounds (see **Display options**). |
| **`describe()`** | One **`to_dict()`** on the current plan; string columns compute **`n_unique`** with a full scan of non-null values. |
| **`info()`**, **`repr()`** | Schema / root-buffer **`shape`** only; no row data materialization. |
| **Async** **`acollect`** / **`ato_dict`** / … | Same work as sync; runs in a thread pool ({doc}`FASTAPI`). |

Set **`PYDANTABLE_VERBOSE_ERRORS=1`** to append a short **`schema=…`** context line when Rust raises **`ValueError`** during **`execute_plan`** (debugging only).

## Choosing an import style (core vs pandas vs PySpark)

All three use the **same** Rust engine; only **names** and **import paths** differ.

| Style | Import | Method flavor | When it helps |
|-------|--------|---------------|---------------|
| **Default (Polars-shaped)** | **`from pydantable import DataFrame`** | **`with_columns`**, **`filter`**, **`select`** | New code and docs; matches {doc}`INTERFACE_CONTRACT` vocabulary. |
| **Pandas-shaped** | **`from pydantable.pandas import DataFrame`** | **`assign`**, **`merge`**, pandas-like **`head`** | Porting pandas tutorials or muscle memory. |
| **PySpark-shaped** | **`from pydantable.pyspark import DataFrame`** | **`withColumn`**, **`where`**, **`show`** | Spark mental model; still in-process (not a Spark cluster). |

See {doc}`PANDAS_UI`, {doc}`PYSPARK_UI`, and **Naming map (core ↔ pandas ↔ PySpark)** there.

## Copy as / interchange

| Goal | API | Extra |
|------|-----|--------|
| Columnar Python **`dict[str, list]`** | **`to_dict()`** / **`collect(as_lists=True)`** | none |
| Validated rows | **`collect()`** (default) | none |
| Polars **`DataFrame`** | **`to_polars()`** | **`pip install 'pydantable[polars]'`** |
| PyArrow **`Table`** | **`to_arrow()`** | **`pip install 'pydantable[arrow]'`** |
| File round-trip | **`read_parquet`** / **`read_ipc`** → constructors | **`[arrow]`** |

Each path that builds Polars or Arrow **first** runs the same Rust materialization as **`to_dict()`** unless documented otherwise.

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

### `Expr` **`repr`**

**`0.20.0+`:** **`Expr`**, **`ColumnRef`**, **`WhenChain`**, and pending window builder objects implement **`__repr__`** with a compact AST-style snippet (from the Rust serializable form) plus **dtype** and **referenced column** hints where available—handy in notebooks and logs without printing raw internal handles.

## `info()` and `describe()` (**0.20.0+**)

- **`info()`** returns a **multi-line string** listing logical column names, **dtype** annotations, and a **row count** aligned with **`shape[0]`** (root-buffer semantics—see {doc}`INTERFACE_CONTRACT` **Introspection**). It does **not** force a full **`collect()`** beyond what **`shape`** already implies for buffer-backed frames.
- **`describe()`** (**0.21.0+**): one **`to_dict()`** materialization, then Python-side stats for **int**, **float**, **bool**, and **str** columns (nullable forms included). Numeric: mean/min/max/std where applicable. Bool: true/false/null counts. String: row count, **`n_unique`** (full scan of non-null strings), min/max **length**, null count. Other dtypes are omitted.

## Jupyter / HTML (`_repr_html_`) and display options

In **Jupyter**, **IPython**, **VS Code** notebooks, and similar frontends, **`DataFrame`** and **`DataFrameModel`** implement **`_repr_html_()`** and **`_repr_mimebundle_()`** so the last line of a cell can render as an **HTML table** (pandas-style), without installing **`polars`**.

**Defaults:** up to **20** rows, **40** columns, **500** characters per cell (see **`pydantable.dataframe._impl`**).

**Tuning (**0.21.0+**):** set environment variables **`PYDANTABLE_REPR_HTML_MAX_ROWS`**, **`PYDANTABLE_REPR_HTML_MAX_COLS`**, **`PYDANTABLE_REPR_HTML_MAX_CELL_LEN`**, or call **`pydantable.set_display_options(...)`** / **`get_repr_html_limits()`** / **`reset_display_options()`** from {mod}`pydantable.display`.

- **Preview only:** bounded rows/columns/cell length.
- **Materialization:** the preview runs the same engine path as **`head(N)`** + **`to_dict()`** for the bounded slice.
- **Safety:** cell text is **HTML-escaped** so arbitrary string data does not inject markup.
- **Grouped handles:** **`GroupedDataFrame`** / **`DynamicGroupedDataFrame`** (and grouped model wrappers) prepend a short label, then show the inner frame preview.

For the full dataset, use **`to_dict()`**, **`collect()`**, **`to_polars()`**, or **`to_arrow()`** as usual.
