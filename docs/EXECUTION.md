# Execution (Rust engine)

All materialization — `collect()`, joins, group-by, reshape, etc. — runs through the
**compiled Rust extension** (`pydantable_native._core`, shipped by `pydantable-native`), which uses Polars for physical execution
inside the native extension. Python does **not** require the `polars` package for core use.

**Four materialization modes** (blocking sync, async await, deferred **`submit`**, chunked **`stream`** / **`astream`**) are the main ways to run terminal work on the same logical plan. See [MATERIALIZATION](/MATERIALIZATION.md) for the overview table and the **`PlanMaterialization`** enum.

**Synchronous materialization (default):** **`collect()`**, **`to_dict()`**, **`collect(as_lists=True)`**, **`collect(as_numpy=True)`**, optional **`to_polars()`**, and optional **`to_arrow()`** run **blocking** Rust + Polars work on the **current thread** ( **`to_arrow()`** then builds a PyArrow **`Table`** from the materialized columnar **`dict`** in Python).

**Async materialization (0.15.0+):** **`await acollect()`**, **`await ato_dict()`**, **`await ato_polars()`**, and **`await ato_arrow()`** on **`DataFrame`** run the same logic as sync materialization. When **`pydantable_native._core`** exposes **`async_execute_plan`**, the engine call is awaited as a **Rust coroutine** built with **`pyo3-async-runtimes`** and **Tokio** (`spawn_blocking` around **`execute_plan`**). If that symbol is absent (older wheels), work falls back to **`asyncio.to_thread`** or a **`concurrent.futures.Executor`** passed as **`executor=`**. **`DataFrameModel`** mirrors **`acollect`**, **`ato_dict`**, **`ato_polars`**, **`ato_arrow`**, **`arows`**, and **`ato_dicts`**. For a diagram of **sync lazy vs async lazy vs eager I/O**, see [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) **Three layers**.

**`aread_*`** returns **`AwaitableDataFrameModel`**: **`return await MyModel.aread_parquet(path).select(...).acollect()`** — one **`await`** on the terminal async method; transforms chain before the read is resolved. Alternatively **`df = await MyModel.aread_parquet(path)`** then **`await df.acollect()`**, or the older nested form **`await (await MyModel.aread_parquet(path)).select(...).acollect()`** (parentheses required; see [FASTAPI_ADVANCED](/FASTAPI_ADVANCED.md)).

**Fire-and-forget (1.6.0+):** **`DataFrame.submit()`** / **`DataFrameModel.submit()`** return an **`ExecutionHandle`**; **`await handle.result()`** matches **`collect()`** for the same arguments. Without **`executor=`**, a daemon thread runs **`collect`**. **`handle.cancel()`** only cancels the backing **`concurrent.futures.Future`** if work has not started; it does **not** stop in-flight Polars execution.

**Chunked iteration (1.6.0+):** **`for batch in df.stream(...)`** (sync) and **`async for batch in df.astream(...)`** (async) yield **`dict[str, list]`** chunks after **one** full engine collect (same slicing strategy as **`collect_batches`** — not Polars’ native lazy batch iterator and **not** out-of-core streaming). Requires **`pydantable[polars]`** for chunk conversion. **`stream()`** suits sync **FastAPI** **`def`** routes with **`StreamingResponse`**; **`astream()`** suits **`async def`** routes. See [FASTAPI](/FASTAPI.md).

Cancelling an **`await acollect()`** (etc.) does **not** cancel in-flight native work. The **GIL** still serializes some Python callbacks; **`ato_polars()`** and **`ato_arrow()`** both build their respective outputs from a materialized columnar **`dict`** (extra allocation vs calling Polars or PyArrow alone on raw buffers).

**File / I/O:** use **`DataFrameModel`** / **`DataFrame[Schema]`** for lazy **`read_*`** / **`aread_*`** and SQL (**`write_sqlmodel`** / **`awrite_sqlmodel`**, or deprecated **`write_sql`** / **`awrite_sql`**). Eager **`materialize_*`**, **`fetch_sqlmodel`** / **`fetch_sql_raw`**, **`iter_sqlmodel`** / **`iter_sql_raw`**, … are imported **from `pydantable`** — pass **`dict[str, list]`** into **`MyModel(...)`** for typed frames. **`ScanFileRoot`** and other untyped scan handles are internal to **`pydantable.io`** — see [IO_OVERVIEW](/IO_OVERVIEW.md). **Which entrypoint?** [IO_DECISION_TREE](/IO_DECISION_TREE.md).

- **`read_*` / `aread_*`:** return a native **`ScanFileRoot`** (local path + format). Use **`MyModel.read_parquet(...)`** / **`await MyModel.aread_parquet(...)`** so transforms run on a Polars **`LazyFrame`** without loading the whole file into **`dict[str, list]`** first. **`DataFrame.write_parquet`**, **`write_csv`**, **`write_ipc`**, and **`write_ndjson`** write the lazy result from Rust (no giant Python column dict on those paths). **`read_parquet_url`** / **`aread_parquet_url`** download HTTP(S) Parquet to a **temp file** you should delete — **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** ([IO_HTTP](/IO_HTTP.md), [DATA_IO_SOURCES](/DATA_IO_SOURCES.md)) unlink it when the block exits. For **large local NDJSON** logs, prefer **`read_ndjson`** / **`read_json`** roots and optional **`streaming=True`** on **`collect()`** / **`write_*`** — patterns in [IO_JSON](/IO_JSON.md).
-  For typed lazy reads (**`DataFrame[Schema].read_*` / `aread_*`**, **`DataFrameModel.read_*` / `aread_*`**), ingest validation options (`trusted_mode`, `fill_missing_optional`, `ignore_errors`, `on_validation_errors`) are applied at **materialization time** (after the engine produces columns, before returning dicts/rows). By default (`fill_missing_optional=True`), missing optional fields (`Optional[T]` / `T | None`) are filled with `None` values; with `fill_missing_optional=False`, missing optionals raise unless the schema field has an explicit default (in which case that default is filled).
- **`materialize_*` / `amaterialize_*`:** import from **`pydantable`**; returns **`dict[str, list]`** (**Rust** / **PyArrow** / stdlib; **PyArrow** for bytes and streaming IPC). Wrap with **`MyModel(cols, ...)`** for a typed model. See **`materialize_json`** for JSON array-of-objects files ([IO_JSON](/IO_JSON.md)). Async: **`await amaterialize_parquet(...)`** or **`executor=`**.
- **SQL (`fetch_sqlmodel` / `fetch_sql_raw`, `iter_sqlmodel` / `iter_sql_raw`, async mirrors; deprecated unprefixed names):** SQLAlchemy → **`dict[str, list]`** (or batches) via **`from pydantable import …`**; **`MyModel(cols)`** for typed frames. **`write_sqlmodel`** / deprecated **`write_sql`** on **`DataFrameModel`** delegate to the same implementation module — [IO_SQL](/IO_SQL.md).
- **`export_*` / `aexport_*`:** take column dicts and write files eagerly; install **`pydantable[polars]`** for the Rust-backed export path where documented.
- **Extension present:** lazy scans, lazy sinks, and **`execute_plan`** require a built `pydantable-native` extension. If the extension is missing, those paths may raise **`MissingRustExtensionError`** (**`NotImplementedError`** subclass) — [CHANGELOG](/CHANGELOG.md).

Service patterns: [FASTAPI](/FASTAPI.md) and [ROADMAP](/ROADMAP.md). Transport table: [DATA_IO_SOURCES](/DATA_IO_SOURCES.md).

**Optional engines (1.17.0+):** you can swap **`ExecutionEngine`** implementations while keeping the **`DataFrame`** / **`DataFrameModel`** API — SQL plans via **`pydantable[sql]`** ([SQL_ENGINE](/SQL_ENGINE.md)), Mongo collection-backed frames via **`pydantable[mongo]`** ([MONGO_ENGINE](/MONGO_ENGINE.md): **`MongoPydantableEngine`** subclasses **`NativePolarsEngine`**; the Mongo plan stack supplies **`MongoRoot`** / materialization only). Physical execution remains the **native** Rust core; the lazy-SQL bridge affects SQL compilation, not Mongo. Eager Mongo column-dict helpers (**`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`**, **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**) do **not** use **`DataFrame._engine`** — same pattern as **`fetch_sqlmodel`** (sync collections run under **`asyncio.to_thread`** in async helpers unless the collection is **`pymongo.asynchronous.AsyncCollection`** — see **PyMongo surface area** in [MONGO_ENGINE](/MONGO_ENGINE.md)).

## Streaming / engine `collect` (Polars)

**Default:** the Rust engine runs Polars **`LazyFrame.collect_with_engine(Engine::InMemory)`** (in-process).

**Streaming:** pass **`streaming=True`** to **`collect()`**, **`to_dict()`**, **`to_polars()`**, **`to_arrow()`**, **`write_parquet()`**, **`write_csv()`**, **`write_ipc()`**, **`write_ndjson()`**, **`join()`**, **`concat()`**, **`melt()`**, **`pivot()`**, **`explode()`**, **`unnest()`**, **`GroupedDataFrame.agg()`**, **`DynamicGroupedDataFrame.agg()`**, or the async mirrors; or set **`PYDANTABLE_ENGINE_STREAMING=1`** (truthy: **`1`**, **`true`**, **`yes`**) so the default is Polars’ **`Engine::Streaming`** **`collect`** where supported. Explicit **`streaming=False`** overrides the env var. This is **best-effort**: unsupported plans may error or behave like in-memory collect depending on Polars.

**`engine_streaming` alias (1.5.0+):** you may pass **`engine_streaming=True`** / **`False`** instead of **`streaming=`** on the same APIs. Passing **both** **`streaming`** and **`engine_streaming`** raises **`TypeError`**. Typed lazy **`read_*` / `aread_*`** can set **`engine_streaming=`** when opening a file root; that value becomes the frame’s default for later **`collect()`** / **`to_*`** / lazy **`write_*`** unless you override **`streaming`** / **`engine_streaming`** on the call.

**Streaming vs in-memory (executor family, high level):**

| Executor family | Honors **`streaming=`** / env on terminal collect |
|-----------------|--------------------------------------------------|
| **`execute_plan`** (filter, select, window, …) | Yes |
| **`write_*` (parquet, csv, ipc, ndjson)** | Yes |
| **`join`**, **`concat`**, **`group_by` / `agg`**, **`melt`**, **`pivot`**, **`explode`**, **`unnest`**, **`group_by_dynamic` / `agg`** | Yes (terminal **`collect_with_engine`**); **cross join** still materializes both sides before **`cross_join`**—can be memory-heavy with two lazy file roots. |

**Lazy file roots — what is safe to chain (high level):**

| Plan shape | On **`read_*` root** |
|------------|----------------------|
| **Filter, select, with_columns, simple projections** | Supported; stays lazy until **`collect`** or **`write_*`**. |
| **Join, concat, melt, pivot, explode, unnest, group_by, dynamic group** | **Supported** (Polars limits apply; some lazy combinations may fail at runtime). |

**`collect_batches()`** runs one full engine collect, then splits rows into Polars **`DataFrame`** chunks (IPC round-trip to Python). It is **not** Polars’ native lazy batch iterator; use it for bounded batch-wise work after materialization.

For **HTTP** materialization, **`fetch_*_url`** / **`read_from_object_store`** still return **`dict[str, list]`** (optional **`max_bytes`** on fetch/object-store paths — [IO_HTTP](/IO_HTTP.md)). For **lazy** HTTP Parquet, use **`read_parquet_url`** or a context manager (temp file lifecycle in [DATA_IO_SOURCES](/DATA_IO_SOURCES.md)).

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
| **`describe()`** | One **`to_dict()`** on the current plan; string columns compute **`n_unique`** with a full scan of non-null values; **`date`** / **`datetime`** columns report min/max over non-null values. |
| **`info()`**, **`repr()`** | Schema / root-buffer **`shape`** only; no row data materialization. |
| **Async** **`acollect`** / **`ato_dict`** / … | Same work as sync; prefers Rust/Tokio awaitable when available, else thread pool ([FASTAPI](/FASTAPI.md)). |
| **`submit`** / **`ExecutionHandle.result`** | Same as **`collect`**; background thread or **`executor.submit`**. |
| **`stream`** / **`astream`** | One full collect, then **`dict[str, list]`** row slices (like **`collect_batches`**). |

Set **`PYDANTABLE_VERBOSE_ERRORS=1`** to append a short **`schema=…`** context line when Rust raises **`ValueError`** during **`execute_plan`** (debugging only).

## Choosing an import style (core vs pandas vs PySpark)

All three use the **same** Rust engine; only **names** and **import paths** differ.

| Style | Import | Method flavor | When it helps |
|-------|--------|---------------|---------------|
| **Default (Polars-shaped)** | **`from pydantable import DataFrame`** | **`with_columns`**, **`filter`**, **`select`** | New code and docs; matches [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) vocabulary. |
| **Pandas-shaped** | **`from pydantable.pandas import DataFrame`** | **`assign`**, **`merge`**, pandas-like **`head`**, duplicate masks / **`get_dummies`** / **`cut`/`qcut`** / **`ewm().mean()`** (see [PANDAS_UI](/PANDAS_UI.md)) | Porting pandas tutorials or muscle memory. |
| **PySpark-shaped** | **`from pydantable.pyspark import DataFrame`** | **`withColumn`**, **`where`**, **`show`** | Spark mental model; still in-process (not a Spark cluster). |

See [PANDAS_UI](/PANDAS_UI.md), [PYSPARK_UI](/PYSPARK_UI.md), and **Naming map (core ↔ pandas ↔ PySpark)** there.

## Copy as / interchange

| Goal | API | Extra |
|------|-----|--------|
| Columnar Python **`dict[str, list]`** | **`to_dict()`** / **`collect(as_lists=True)`** | none |
| Validated rows | **`collect()`** (default) | none |
| Polars **`DataFrame`** | **`to_polars()`** | **`pip install 'pydantable[polars]'`** |
| PyArrow **`Table`** | **`to_arrow()`** | **`pip install 'pydantable[arrow]'`** |
| File round-trip | **`materialize_*`** (**`from pydantable import …`**) + **`MyModel(cols)`** / **`export_*`**; or **`read_*`** + transforms + **`write_parquet`** | **`[arrow]`** (buffers); **`[polars]`** (Rust export + lazy **`write_*`** path); core wheel includes Rust readers |

Each path that builds Polars or Arrow **first** runs the same Rust materialization as **`to_dict()`** unless documented otherwise.

### DataFrame interchange protocol (`__dataframe__`) and Streamlit

Some tools (including Streamlit’s `st.dataframe`) can render interactive tables from objects that implement the **Python DataFrame Interchange Protocol** (`__dataframe__`).

As of **0.21.0**, `pydantable` implements `__dataframe__` on `DataFrame` (and `DataFrameModel` via delegation). This path **materializes** to a PyArrow `Table` first (same cost class as `to_arrow()`), then delegates to PyArrow’s interchange export.

See [STREAMLIT](/STREAMLIT.md) for install notes, fallbacks (including `st.data_editor(df.to_arrow())`), and limitations.

The Python module `python/pydantable/rust_engine.py` is the thin wrapper that invokes
`execute_plan`, `execute_join`, and related functions on `_core` (no alternate engines).

**0.18.0 — Grouped execution errors:** When Polars **`collect()`** fails during **`group_by().agg()`**, the raised **`ValueError`** may include the prefix **`Polars execution error (group_by().agg()):`** so the failure is identifiable as grouped aggregation rather than a generic plan step. This does not change aggregation results or schema rules ([INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md)).

Optional **UI modules** (`pydantable.pandas`, `pydantable.pyspark`) only change **method
names and imports** (e.g. `assign` vs `withColumn`). They do not select a different
execution engine.

**Typed expressions** (`Expr`, `Column`, PySpark `F.col(...)`) are validated in Rust
(`ExprNode`), then lowered to Polars inside the extension. The expression and window surface
has grown across releases (globals, framed windows, maps, temporal helpers, multi-key
**`rangeBetween`**, etc.). The authoritative feature list and semantics are [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md),
[WINDOW_SQL_SEMANTICS](/WINDOW_SQL_SEMANTICS.md), [SUPPORTED_TYPES](/SUPPORTED_TYPES.md), and [CHANGELOG](/CHANGELOG.md).

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

- **`info()`** returns a **multi-line string** listing logical column names, **dtype** annotations, and a **row count** aligned with **`shape[0]`** (root-buffer semantics—see [INTERFACE_CONTRACT](/INTERFACE_CONTRACT.md) **Introspection**). It does **not** force a full **`collect()`** beyond what **`shape`** already implies for buffer-backed frames.
- **`describe()`** (**0.20.0+**): one **`to_dict()`** materialization, then Python-side stats for **int**, **float**, **bool**, **str**, **`date`**, and **`datetime`** columns (nullable forms included). Numeric: mean/min/max/std where applicable. Bool: true/false/null counts. String: row count, **`n_unique`** (full scan of non-null strings), min/max **length**, null count. **`date` / `datetime`**: non-null count, min, max, null count. Other dtypes are omitted.

## Jupyter / HTML (`_repr_html_`) and display options

In **Jupyter**, **IPython**, **VS Code** notebooks, and similar frontends, **`DataFrame`** and **`DataFrameModel`** implement **`_repr_html_()`** and **`_repr_mimebundle_()`** so the last line of a cell can render as an **HTML table** (pandas-style), without installing **`polars`**.

**Defaults:** up to **20** rows, **40** columns, **500** characters per cell (see **`pydantable.dataframe._impl`**).

**Tuning (**0.20.0+**):** set environment variables **`PYDANTABLE_REPR_HTML_MAX_ROWS`**, **`PYDANTABLE_REPR_HTML_MAX_COLS`**, **`PYDANTABLE_REPR_HTML_MAX_CELL_LEN`**, or call **`pydantable.set_display_options(...)`** / **`get_repr_html_limits()`** / **`reset_display_options()`** from {mod}`pydantable.display`.

- **Preview only:** bounded rows/columns/cell length.
- **Materialization:** the preview runs the same engine path as **`head(N)`** + **`to_dict()`** for the bounded slice.
- **Safety:** cell text is **HTML-escaped** so arbitrary string data does not inject markup.
- **Grouped handles:** **`GroupedDataFrame`** / **`DynamicGroupedDataFrame`** (and grouped model wrappers) prepend a short label, then show the inner frame preview.

For the full dataset, use **`to_dict()`**, **`collect()`**, **`to_polars()`**, or **`to_arrow()`** as usual.
