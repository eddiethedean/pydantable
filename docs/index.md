# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust-powered execution core (Polars-backed inside the native extension).

```{note}
This **documentation site** is the detailed manual. The repository **README** on your Git host is the short entrypoint; both should stay aligned for install commands and version. **Current release:** **0.23.0** (see {doc}`changelog`).
```

## At a glance

- **Schemas first:** Pydantic annotations drive column types, nullability, and early expression errors (Rust AST).
- **SQLModel-like `DataFrameModel`:** whole-table type, generated row models, column or row inputs.
- **Optional Python Polars:** install `pydantable[polars]` for `to_polars()` and Rust-backed **`export_*`** / lazy **`write_*`** sinks; core usage does not require `import polars`.
- **Optional PyArrow interchange:** install `pydantable[arrow]` for **`materialize_parquet` / `materialize_ipc`**, **`to_arrow` / `ato_arrow`**, and **`Table` / `RecordBatch`** constructor ingest. **Lazy local files:** **`read_parquet`**, **`read_csv`**, **`read_ndjson`**, **`read_ipc`**, **`read_json`** + **`DataFrame.write_*`** ({doc}`EXECUTION`, {doc}`IO_OVERVIEW`).

**Materialization (0.5+):** `collect()` returns a **list of Pydantic row models** for the current schema. Use **`to_dict()`** (or **`collect(as_lists=True)`**) for columnar **`dict[str, list]`** responses. **`to_polars()`** is available when the optional Python **`polars`** package is installed; **`to_arrow()`** when **`pyarrow`** is installed (**0.16.0**). **0.15.0** adds **`acollect`**, **`ato_dict`**, and **`ato_polars`** for **async** thread-offloaded materialization; **0.16.0** adds **`ato_arrow`**. Details: {doc}`EXECUTION` and {doc}`DATAFRAMEMODEL`.

**Scalar dtypes** include `int`, `float`, `bool`, `str`, `datetime`, `date`, `time`, `timedelta`, `bytes`, homogeneous **`dict[str, T]`** maps, each nullable via `Optional` / `| None`. **Structs**, **lists**, **UUID**, **Decimal**, and **Enum** columns are documented in {doc}`SUPPORTED_TYPES`. Unsupported `DataFrameModel` field annotations fail at **class definition** time.

**Expressions (0.7+ through 0.20.0):** typed **`Expr`** builds a Rust AST — globals (**`global_row_count`**, **`global_sum`**, …), ranked and framed windows (including multi-key **`rangeBetween`**; see {doc}`WINDOW_SQL_SEMANTICS`), maps and temporal helpers, PySpark mirrors in {doc}`PYSPARK_PARITY` (**0.17.0** added more `sql.functions` wrappers; **0.18.0** / **0.19.0** did not expand the façade matrix; **0.20.0** adds **`show`** / **`summary`** on the PySpark UI **`DataFrame`**). **`Expr`** / **`WhenChain`** expose **`__repr__`** for debugging. Semantics: {doc}`INTERFACE_CONTRACT` and {doc}`changelog`. **Next:** {doc}`ROADMAP` — **0.23.x** ships the lazy/eager I/O split and format guides; **Planned v1.0.0** is the **1.0** stability cut (optional polish in **Later**).

**Trusted ingest:** **`trusted_mode`** (`off` / `shape_only` / `strict`) on constructors — {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`. **I/O:** sync **`collect` / `to_dict` / `to_polars` / `to_arrow`** plus **async** **`acollect` / `ato_dict` / `ato_polars` / `ato_arrow`**; **`materialize_*`** / **`read_*`** / **`fetch_sql`** / **`export_*`** / **`write_sql`**. For **lazy** `read_*`, ingest validation options (`trusted_mode`, `ignore_errors`, `on_validation_errors`) are applied at **materialization time** (e.g. `to_dict()` / `collect()`). Lazy HTTP Parquet cleanup via **`read_parquet_url_ctx`** ({doc}`IO_HTTP`); **JSON** array-of-objects files — {doc}`IO_JSON`. **Missing extension:** lazy scans and sinks may raise **`MissingRustExtensionError`** if **`pydantable._core`** is not built ({doc}`changelog`). Service patterns: {doc}`EXECUTION`, {doc}`FASTAPI`. **Arrow `map<utf8, …>`** columns can ingest as **`dict[str, T]`** ({doc}`SUPPORTED_TYPES`).

**String representation:** **`repr(df)`** on **`DataFrame`** and **`DataFrameModel`** prints the parameterized schema (class name, column names, and dtype annotations). Very wide schemas list the first **32** columns and **`… and N more`**. Row counts are **not** included—filters and joins can change the result size without materializing; use **`collect()`**, **`to_dict()`**, or **`len(...)`** on materialized results when you need shape ({doc}`EXECUTION`, {doc}`DATAFRAMEMODEL`). **Discovery (0.20.0+):** **`columns`**, **`shape`**, **`dtypes`**, **`info()`**, **`describe()`** on the core API—see {doc}`INTERFACE_CONTRACT` **Introspection** for **`shape`** vs executed row count. **Jupyter / IPython:** **`_repr_html_()`** draws a bounded **HTML table** (pandas-style) without **`polars`** ({doc}`EXECUTION` **Jupyter / HTML**).

## Where to go next

| Audience | Start here |
|----------|------------|
| **Library users** | {doc}`DATAFRAMEMODEL` — contract, inputs, transforms, materialization |
| **FastAPI apps** | {doc}`FASTAPI` — routers, request bodies, responses |
| **Semantics** (nulls, joins, ordering) | {doc}`INTERFACE_CONTRACT` · {doc}`VERSIONING` (0.x semver) |
| **Data sources & I/O** | {doc}`IO_DECISION_TREE` — pick an API · {doc}`IO_OVERVIEW` — one page per format/transport · {doc}`DATA_IO_SOURCES` — planning & async stacks |
| **Contributors** | {doc}`DEVELOPER` — build, test, Sphinx, release |
| **Polars parity and gaps** | {doc}`PARITY_SCORECARD` and {doc}`POLARS_TRANSFORMATIONS_ROADMAP` |
| **Roadmap (0.23 I/O shipped, 1.0 next)** | {doc}`ROADMAP` — **Shipped** sections through **0.23.x**, **Planned v1.0.0** |
| **Five-minute tour** | {doc}`QUICKSTART` |

```{toctree}
:titlesonly:
:hidden:
:caption: Getting started

QUICKSTART
DATAFRAMEMODEL
SUPPORTED_TYPES
FASTAPI
EXECUTION
DATA_IO_SOURCES
STREAMLIT
```

```{toctree}
:titlesonly:
:hidden:
:caption: Data I/O (by format)

IO_OVERVIEW
IO_DECISION_TREE
IO_PARQUET
IO_CSV
IO_NDJSON
IO_JSON
IO_IPC
IO_HTTP
IO_SQL
IO_EXTRAS
```

```{toctree}
:titlesonly:
:hidden:
:caption: Semantics and contracts

INTERFACE_CONTRACT
VERSIONING
WINDOW_SQL_SEMANTICS
WHY_NOT_POLARS
```

```{toctree}
:titlesonly:
:hidden:
:caption: Alternate import surfaces

PANDAS_UI
PYSPARK_UI
PYSPARK_INTERFACE
PYSPARK_PARITY
```

```{toctree}
:titlesonly:
:hidden:
:caption: Polars alignment

PARITY_SCORECARD
POLARS_WORKFLOWS
POLARS_TRANSFORMATIONS_ROADMAP
```

```{toctree}
:titlesonly:
:hidden:
:caption: Project

ROADMAP
README
pydantable_plan
DEVELOPER
PERFORMANCE
changelog
```

```{toctree}
:titlesonly:
:hidden:
:caption: Reference

api/index
```
