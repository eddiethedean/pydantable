# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust-powered execution core (Polars-backed inside the native extension).

```{note}
This **documentation site** is the detailed manual. The repository **README** on your Git host is the short entrypoint; both should stay aligned for install commands and version.
```

## At a glance

- **Schemas first:** Pydantic annotations drive column types, nullability, and early expression errors (Rust AST).
- **SQLModel-like `DataFrameModel`:** whole-table type, generated row models, column or row inputs.
- **Optional Python Polars:** install `pydantable[polars]` for `to_polars()`; core usage does not require `import polars`.

**Materialization (0.5+):** `collect()` returns a **list of Pydantic row models** for the current schema. Use **`to_dict()`** (or **`collect(as_lists=True)`**) for columnar **`dict[str, list]`** responses. **`to_polars()`** is available when the optional Python **`polars`** package is installed. Details: {doc}`EXECUTION` and {doc}`DATAFRAMEMODEL`.

**Scalar dtypes** include `int`, `float`, `bool`, `str`, `datetime`, `date`, `time`, `timedelta`, `bytes`, homogeneous **`dict[str, T]`** maps, each nullable via `Optional` / `| None`. **Structs**, **lists**, **UUID**, **Decimal**, and **Enum** columns are documented in {doc}`SUPPORTED_TYPES`. Unsupported `DataFrameModel` field annotations fail at **class definition** time.

**Expressions (0.7+ through current):** typed **`Expr`** builds a Rust AST — globals (**`global_row_count`**, **`global_sum`**, …), ranked and framed windows (including multi-key **`rangeBetween`**; see {doc}`WINDOW_SQL_SEMANTICS`), maps and temporal helpers, PySpark mirrors in {doc}`PYSPARK_PARITY`. Semantics: {doc}`INTERFACE_CONTRACT` and {doc}`changelog`.

**Trusted ingest:** **`trusted_mode`** (`off` / `shape_only` / `strict`) and legacy **`validate_data`** on constructors — {doc}`DATAFRAMEMODEL`, {doc}`SUPPORTED_TYPES`. **I/O:** materialization and interchange are **synchronous** today; async coverage is on the roadmap ({doc}`EXECUTION`, {doc}`ROADMAP`).

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
