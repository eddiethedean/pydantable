# Moltres SQL: `SqlDataFrame` and `SqlDataFrameModel`

This guide covers the **optional** integration between PydanTable and
[**moltres-core**](https://pypi.org/project/moltres-core/) on PyPI: typed
`DataFrame` / `DataFrameModel` instances whose **execution engine** is
`moltres_core.MoltresPydantableEngine`, implementing the same
**`ExecutionEngine`** protocol as **`pydantable.engine.protocols`** (from
[**pydantable-protocol**](https://pypi.org/project/pydantable-protocol/) on PyPI;
see {doc}`CUSTOM_ENGINE_PACKAGE`).

```{note}
**Install:** ``pip install "pydantable[moltres]"`` (adds **moltres-core**). The
core **pydantable** package does not import **moltres-core** at import time;
``SqlDataFrame`` / ``SqlDataFrameModel`` are loaded lazily from the root package
(``from pydantable import SqlDataFrame``) or imported explicitly from
``pydantable.sql_moltres``.
```

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see {doc}`DATAFRAMEMODEL`, {doc}`EXECUTION`). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | `pydantable.io` — {doc}`IO_SQL` (**`fetch_sqlmodel`**, **`fetch_sql_raw`**, …). |
| **Lazy execution** of transforms via a **SQL** backend (Moltres compiles plans to SQL) | **`SqlDataFrame`** / **`SqlDataFrameModel`** with **`sql_config=`** or **`moltres_engine=`**. |

The SQL I/O helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`SqlDataFrame`** wires Moltres as that engine so
`select`, `filter`, `collect`, etc. go through **moltres-core** (subject to what
the engine implements today).

## Imports

```python
# Lazy (no moltres import until accessed)
from pydantable import SqlDataFrame, SqlDataFrameModel

# Explicit
from pydantable.sql_moltres import (
    SqlDataFrame,
    SqlDataFrameModel,
    moltres_engine_from_sql_config,
)
```

If **moltres-core** is missing, constructing these classes raises **ImportError**
with an install hint (`pydantable[moltres]`).

## `EngineConfig` and constructing an engine

Moltres uses **`moltres_core.EngineConfig`** for SQLAlchemy **2.x** connection
settings. You must supply exactly one of **`dsn`**, **`engine`**, or **`session`**
(see the **moltres-core** package docs on PyPI and upstream **Moltres** docs).

```python
from moltres_core import EngineConfig

# Example: in-memory SQLite (DSN)
sql_config = EngineConfig(dsn="sqlite:///:memory:")
```

**Reuse one engine** across many frames:

```python
from pydantable.sql_moltres import moltres_engine_from_sql_config

engine = moltres_engine_from_sql_config(sql_config)
# Pass moltres_engine=engine on each SqlDataFrame / SqlDataFrameModel
```

## `SqlDataFrame`

`SqlDataFrame` subclasses **`DataFrame`**. Use
`SqlDataFrame[YourSchema](data, ...)` the same way as `DataFrame[YourSchema]`,
but you **must** provide a SQL execution backend via one of:

1. **`engine=`** — any object satisfying `ExecutionEngine` (advanced / tests).
2. **`moltres_engine=`** — a pre-built `MoltresPydantableEngine`.
3. **`sql_config=`** — an `EngineConfig`; PydanTable builds
   `ConnectionManager` + `MoltresPydantableEngine` internally.

Precedence is **exactly** that order: explicit **`engine=`** wins.

```python
from pydantic import BaseModel

from moltres_core import EngineConfig
from pydantable import SqlDataFrame


class Row(BaseModel):
    id: int
    name: str


cfg = EngineConfig(dsn="sqlite:///:memory:")
df = SqlDataFrame[Row](
    {"id": [1, 2], "name": ["a", "b"]},
    sql_config=cfg,
)
```

Constructor flags (`trusted_mode`, `fill_missing_optional`, validation hooks,
etc.) match `DataFrame` — see {doc}`DATAFRAMEMODEL` and {doc}`STRICTNESS`.

## `SqlDataFrameModel`

`SqlDataFrameModel` subclasses **`DataFrameModel`**
and sets the inner frame class to `SqlDataFrame`. Define columns on a subclass
as usual; pass **`sql_config=`**, **`moltres_engine=`**, or **`engine=`** on
construction.

```python
from moltres_core import EngineConfig
from pydantable import SqlDataFrameModel


class Users(SqlDataFrameModel):
    id: int
    name: str


cfg = EngineConfig(dsn="sqlite:///:memory:")
users = Users({"id": [1], "name": ["Ada"]}, sql_config=cfg)
```

## Expressions (`Expr`) and the native runtime

`Expr`-based `filter` / `with_columns` / … rely on
`pydantable.engine.get_expression_runtime()`, which is tied to the **native**
Rust core when the default engine is `NativePolarsEngine`. With **only** the
Moltres engine bound to your frame, expression-heavy APIs may raise
`UnsupportedEngineOperationError` unless you integrate a compatible expression
runtime — see **Expressions** in {doc}`CUSTOM_ENGINE_PACKAGE` and {doc}`ADR-engines`.

Prefer operations your **moltres-core** version documents as supported, or use
the native engine for expression-heavy paths.

## File I/O vs execution engine

Lazy **`read_*`** paths can still use **pydantable-native** for local files; that
is separate from **`DataFrame._engine`**. A custom SQL engine does **not**
automatically route `read_parquet` through the database. See **File I/O vs
execution engine** in {doc}`CUSTOM_ENGINE_PACKAGE`.

## Versioning

**moltres-core** declares a dependency on **pydantable-protocol** compatible
with your **pydantable** release line. Keep **pydantable**, **pydantable-protocol**,
and **moltres-core** on mutually supported combinations (see {doc}`VERSIONING`).

## See also

- {doc}`CUSTOM_ENGINE_PACKAGE` — third-party `ExecutionEngine` packages (includes **moltres-core** as a reference).
- {doc}`IO_SQL` — eager SQL **I/O** (`fetch_sqlmodel`, `write_sqlmodel`, …).
- {doc}`ADR-engines` — engine abstraction and extension points.
- {doc}`EXECUTION` — materialization and engines.
- {doc}`DATAFRAMEMODEL` — `DataFrameModel` patterns shared by `SqlDataFrameModel`.
