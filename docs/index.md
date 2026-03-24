# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust-powered execution core (Polars-backed inside the native extension).

```{note}
This **documentation site** is the detailed manual. The repository **README** on your Git host is the short entrypoint; both should stay aligned for install commands and version.
```

## At a glance

- **Schemas first:** Pydantic annotations drive column types, nullability, and early expression errors (Rust AST).
- **SQLModel-like `DataFrameModel`:** whole-table type, generated row models, column or row inputs.
- **Optional Python Polars:** install `pydantable[polars]` for `to_polars()`; core usage does not require `import polars`.
- **Optional PyArrow interchange:** install `pydantable[arrow]` for **`read_parquet` / `read_ipc`**, **`to_arrow` / `ato_arrow`**, and **`Table` / `RecordBatch`** constructor ingest.

**Materialization (0.5+):** `collect()` returns a **list of Pydantic row models** for the current schema. Use **`to_dict()`** (or **`collect(as_lists=True)`**) for columnar **`dict[str, list]`** responses. **`to_polars()`** is available when the optional Python **`polars`** package is installed; **`to_arrow()`** when **`pyarrow`** is installed (**0.16.0**). **0.15.0** adds **`acollect`**, **`ato_dict`**, and **`ato_polars`** for **async** thread-offloaded materialization; **0.16.0** adds **`ato_arrow`**. Details: {doc}`EXECUTION` and {doc}`DATAFRAMEMODEL`.

**Scalar dtypes** include `int`, `float`, `bool`, `str`, `datetime`, `date`, `time`, `timedelta`, `bytes`, homogeneous **`dict[str, T]`** maps, each nullable via `Optional` / `| None`. **Structs**, **lists**, **UUID**, **Decimal**, and **Enum** columns are documented in {doc}`SUPPORTED_TYPES`. Unsupported `DataFrameModel` field annotations fail at **class definition** time.

**Expressions (0.7+ through current):** typed **`Expr`** builds a Rust AST — globals (**`global_row_count`**, **`global_sum`**, …), ranked and framed windows (including multi-key **`rangeBetween`**; see {doc}`WINDOW_SQL_SEMANTICS`), maps and temporal helpers, PySpark mirrors in {doc}`PYSPARK_PARITY` (**0.17.0** adds more `sql.functions` wrappers — string replace/strip, **`strptime`**, **`binary_len`**, list helpers). Semantics: {doc}`INTERFACE_CONTRACT` and {doc}`changelog`.

**Trusted ingest:** **`trusted_mode`** (`off` / `shape_only` / `strict`) on constructors — {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`. **I/O:** sync **`collect` / `to_dict` / `to_polars` / `to_arrow`** plus **async** **`acollect` / `ato_dict` / `ato_polars` / `ato_arrow`**; **`read_parquet` / `read_ipc`** for file/bytes ingest ({doc}`EXECUTION`, {doc}`FASTAPI`). **Arrow `map<utf8, …>`** columns can ingest as **`dict[str, T]`** ({doc}`SUPPORTED_TYPES`).

## Where to go next

| Audience | Start here |
|----------|------------|
| **Library users** | {doc}`DATAFRAMEMODEL` — contract, inputs, transforms, materialization |
| **FastAPI apps** | {doc}`FASTAPI` — routers, request bodies, responses |
| **Semantics** (nulls, joins, ordering) | {doc}`INTERFACE_CONTRACT` |
| **Contributors** | {doc}`DEVELOPER` — build, test, Sphinx, release |
| **Polars parity and gaps** | {doc}`PARITY_SCORECARD` and {doc}`POLARS_TRANSFORMATIONS_ROADMAP` |

```{toctree}
:titlesonly:
:hidden:
:caption: Getting started

DATAFRAMEMODEL
SUPPORTED_TYPES
FASTAPI
EXECUTION
```

```{toctree}
:titlesonly:
:hidden:
:caption: Semantics and contracts

INTERFACE_CONTRACT
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
