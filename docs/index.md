# PydanTable

**Strongly-typed DataFrame layer for FastAPI and Pydantic services**, with a Rust-powered execution core (Polars-backed inside the native extension).

```{note}
This **documentation site** is the detailed manual. The repository **README** on your Git host is the short entrypoint; both should stay aligned for install commands and version. **Current release:** **1.1.0** (see {doc}`changelog`) — stable **1.x** API under the policy in {doc}`VERSIONING`. Roadmap history and the completed **v1.0.0** gate checklist live in {doc}`ROADMAP`.
```

## Choose your path

- **Services (FastAPI)**: start with {doc}`FASTAPI`, then {doc}`DATAFRAMEMODEL` and {doc}`EXECUTION`.
- **Data workflows**: start with {doc}`DATAFRAMEMODEL`, then {doc}`IO_DECISION_TREE` and {doc}`IO_OVERVIEW`.
- **Library/interop**: start with {doc}`INTERFACE_CONTRACT` and {doc}`VERSIONING`, then {doc}`PLAN_AND_PLUGINS`.
- `DataFrameModel` transform chains can return typed after-schema models directly (no `to_dict()` materialization step). For pyright/Pylance, use `as_model(...)` (see {doc}`DATAFRAMEMODEL`).

If you’re not sure where something is documented, use {doc}`DOCS_MAP`.

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
DOCS_MAP
TROUBLESHOOTING
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
:caption: Cookbook

cookbook/index
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
:caption: Introspection and extensions

PLAN_AND_PLUGINS
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
ARCHITECTURE
DEVELOPER
DOCS_STYLE_GUIDE
PERFORMANCE
changelog
```

```{toctree}
:titlesonly:
:hidden:
:caption: Reference

api/index
```
