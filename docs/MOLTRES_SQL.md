# Moltres SQL: `SqlDataFrame` and `SqlDataFrameModel`

This guide covers the **optional** integration between PydanTable and
[**moltres-core**](https://pypi.org/project/moltres-core/) on PyPI: typed
`DataFrame` / `DataFrameModel` instances whose **execution engine** is
`moltres_core.MoltresPydantableEngine`, implementing the same
**`ExecutionEngine`** protocol as **`pydantable.engine.protocols`** (from
[**pydantable-protocol**](https://pypi.org/project/pydantable-protocol/) on PyPI;
see {doc}`CUSTOM_ENGINE_PACKAGE`).

```{note}
**Install:** ``pip install "pydantable[moltres]"`` (adds **moltres-core** and **rapsqlite**). The
core **pydantable** package does not import **moltres-core** at import time;
``SqlDataFrame`` / ``SqlDataFrameModel`` are loaded lazily from the root package
(``from pydantable import SqlDataFrame``) or imported explicitly from
``pydantable.sql_moltres``.
```

## SQLite: sync (Moltres) vs async (rapsqlite)

**Moltres** builds a **synchronous** SQLAlchemy engine from ``EngineConfig``. For SQLite, use a normal URL such as ``sqlite:///:memory:`` or ``sqlite:///path/to.db``.

**rapsqlite** registers the ``sqlite+rapsqlite`` dialect for **async** SQLAlchemy (``sqlalchemy.ext.asyncio.create_async_engine``). Moltres’ current ``MoltresPydantableEngine`` uses the sync connection stack, so pass **sync** DSNs to ``sql_config=``. Use ``sqlite+rapsqlite://...`` in your own async SQLAlchemy code (or future Moltres async wiring); ``ato_dict`` / ``acollect`` on ``SqlDataFrame`` still delegate to Moltres’ async entrypoints, which may run sync work on a thread pool — see {doc}`EXECUTION`.

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see {doc}`DATAFRAMEMODEL`, {doc}`EXECUTION`). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — {doc}`IO_SQL` (**`fetch_sqlmodel`**, **`fetch_sql_raw`**, **`write_sql_raw`**, …). |
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

### Lazy read from a SQL table

Use **`SqlDataFrame[Schema].from_sql_table(table, sql_config=…)`** (or **`moltres_engine=`**) so the frame holds a Moltres **`SqlRootData`** root: **no `SELECT` runs** until you call **`to_dict()`**, **`collect()`**, **`head()`**, etc. For **`SqlDataFrameModel`**, use **`MyModel.read_sql_table(table, …)`**.

For **SQLite in-memory** (`sqlite:///:memory:`), each new SQLAlchemy / Moltres connection pool is an **empty** database unless you share one **`moltres_engine`** for both DDL and the frame. Prefer a **file** URL (`sqlite:///…/app.db`) while wiring this up, or build tables using the same **`ConnectionManager`** / engine you pass as **`moltres_engine=`**.

```python
from pydantic import BaseModel
from sqlalchemy import Column, Integer, MetaData, String, Table, insert
from moltres_core import ConnectionManager, EngineConfig

from pydantable.sql_moltres import SqlDataFrame, moltres_engine_from_sql_config


class Row(BaseModel):
    id: int
    name: str


# File-backed SQLite so DDL and the lazy frame see the same DB
cfg = EngineConfig(dsn="sqlite:////tmp/app.db")
eng = moltres_engine_from_sql_config(cfg)
cm = ConnectionManager(cfg)
md = MetaData()
items = Table("items", md, Column("id", Integer, primary_key=True), Column("name", String(40)))
md.create_all(cm.engine)
with cm.engine.connect() as conn:
    conn.execute(insert(items).values(id=1, name="a"))
    conn.commit()

df = SqlDataFrame[Row].from_sql_table(items, moltres_engine=eng)  # lazy
cols = df.to_dict()  # runs SELECT here
```

Eager in-memory columns (no SQL root) — same constructor as **`DataFrame`**:

```python
from moltres_core import EngineConfig
from pydantable import SqlDataFrame

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
