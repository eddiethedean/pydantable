# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust-powered execution core (Polars-backed inside the native extension).

```{note}
This **documentation site** is the detailed manual. The repository **README** on your Git host is the short entrypoint; both should stay aligned for install commands and version. **Current release:** **0.22.0** (see {doc}`changelog`).
```

## At a glance

- **Schemas first:** Pydantic annotations drive column types, nullability, and early expression errors (Rust AST).
- **SQLModel-like `DataFrameModel`:** whole-table type, generated row models, column or row inputs.
- **Optional Python Polars:** install `pydantable[polars]` for `to_polars()`; core usage does not require `import polars`.
- **Optional PyArrow interchange:** install `pydantable[arrow]` for **`read_parquet` / `read_ipc`**, **`to_arrow` / `ato_arrow`**, and **`Table` / `RecordBatch`** constructor ingest.

**Materialization (0.5+):** `collect()` returns a **list of Pydantic row models** for the current schema. Use **`to_dict()`** (or **`collect(as_lists=True)`**) for columnar **`dict[str, list]`** responses. **`to_polars()`** is available when the optional Python **`polars`** package is installed; **`to_arrow()`** when **`pyarrow`** is installed (**0.16.0**). **0.15.0** adds **`acollect`**, **`ato_dict`**, and **`ato_polars`** for **async** thread-offloaded materialization; **0.16.0** adds **`ato_arrow`**. Details: {doc}`EXECUTION` and {doc}`DATAFRAMEMODEL`.

**Scalar dtypes** include `int`, `float`, `bool`, `str`, `datetime`, `date`, `time`, `timedelta`, `bytes`, homogeneous **`dict[str, T]`** maps, each nullable via `Optional` / `| None`. **Structs**, **lists**, **UUID**, **Decimal**, and **Enum** columns are documented in {doc}`SUPPORTED_TYPES`. Unsupported `DataFrameModel` field annotations fail at **class definition** time.

**Expressions (0.7+ through 0.20.0):** typed **`Expr`** builds a Rust AST — globals (**`global_row_count`**, **`global_sum`**, …), ranked and framed windows (including multi-key **`rangeBetween`**; see {doc}`WINDOW_SQL_SEMANTICS`), maps and temporal helpers, PySpark mirrors in {doc}`PYSPARK_PARITY` (**0.17.0** added more `sql.functions` wrappers; **0.18.0** / **0.19.0** did not expand the façade matrix; **0.20.0** adds **`show`** / **`summary`** on the PySpark UI **`DataFrame`**). **`Expr`** / **`WhenChain`** expose **`__repr__`** for debugging. Semantics: {doc}`INTERFACE_CONTRACT` and {doc}`changelog`. **Next:** {doc}`ROADMAP` **Planned v1.0.0** for the **1.0** stability cut (optional polish in **Later**).

**Trusted ingest:** **`trusted_mode`** (`off` / `shape_only` / `strict`) on constructors — {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`. **I/O:** sync **`collect` / `to_dict` / `to_polars` / `to_arrow`** plus **async** **`acollect` / `ato_dict` / `ato_polars` / `ato_arrow`**; **`read_parquet` / `read_ipc`** for file/bytes ingest ({doc}`EXECUTION`, {doc}`FASTAPI`). **Arrow `map<utf8, …>`** columns can ingest as **`dict[str, T]`** ({doc}`SUPPORTED_TYPES`).

**String representation:** **`repr(df)`** on **`DataFrame`** and **`DataFrameModel`** prints the parameterized schema (class name, column names, and dtype annotations). Very wide schemas list the first **32** columns and **`… and N more`**. Row counts are **not** included—filters and joins can change the result size without materializing; use **`collect()`**, **`to_dict()`**, or **`len(...)`** on materialized results when you need shape ({doc}`EXECUTION`, {doc}`DATAFRAMEMODEL`). **Discovery (0.20.0+):** **`columns`**, **`shape`**, **`dtypes`**, **`info()`**, **`describe()`** on the core API—see {doc}`INTERFACE_CONTRACT` **Introspection** for **`shape`** vs executed row count. **Jupyter / IPython:** **`_repr_html_()`** draws a bounded **HTML table** (pandas-style) without **`polars`** ({doc}`EXECUTION` **Jupyter / HTML**).

## Where to go next

| Audience | Start here |
|----------|------------|
| **Library users** | {doc}`DATAFRAMEMODEL` — contract, inputs, transforms, materialization |
| **FastAPI apps** | {doc}`FASTAPI` — routers, request bodies, responses |
| **Semantics** (nulls, joins, ordering) | {doc}`INTERFACE_CONTRACT` · {doc}`VERSIONING` (0.x semver) |
| **Data sources & I/O (planning)** | {doc}`DATA_IO_SOURCES` — common reads/writes, SQL async stacks |
| **Contributors** | {doc}`DEVELOPER` — build, test, Sphinx, release |
| **Polars parity and gaps** | {doc}`PARITY_SCORECARD` and {doc}`POLARS_TRANSFORMATIONS_ROADMAP` |
| **Roadmap (0.20 shipped, 1.0 next)** | {doc}`ROADMAP` — **Shipped in 0.20.0**, **Planned v1.0.0** |
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
