# Lazy SQL DataFrame: `SqlDataFrame` and `SqlDataFrameModel`

This guide covers the **optional** SQLAlchemy-backed lazy execution path: typed
`DataFrame` / `DataFrameModel` instances whose **execution engine** comes from the
same optional stack installed with ``pip install "pydantable[sql]"`` (SQLAlchemy **2.x** connection plumbing plus pydantable’s bridge to **`ExecutionEngine`** — see [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md)).

The reason to use a **SQL** execution engine is to keep **transformations on the
database side** for as long as possible: the lazy-SQL bridge compiles the typed plan to SQL
so you are not forced to **pull entire result sets into Python memory** just to
`select`, `sort`, `join`, and similar steps. That matters most when the **end of
the pipeline writes back to the same database** — there is little benefit in
round-tripping the full dataset through the app when the work can stay in the
server.

!!! note
    **Install:** ``pip install "pydantable[sql]"`` (SQLAlchemy bridge; add a **DB-API** driver for your DSN — e.g. **psycopg** / **asyncpg** for Postgres, **aiosqlite** or optional **rapsqlite** for async SQLite). The
    core **pydantable** package does not import the optional SQL bridge at import time;
    ``SqlDataFrame`` / ``SqlDataFrameModel`` are loaded lazily from the root package
    (``from pydantable import SqlDataFrame``) or imported explicitly from
    ``pydantable.sql_dataframe``.


## SQLite: sync (lazy-SQL bridge) vs async (rapsqlite)

The lazy-SQL stack builds a **synchronous** SQLAlchemy engine from ``EngineConfig``. For SQLite, use a normal URL such as ``sqlite:///:memory:`` or ``sqlite:///path/to.db``.

**rapsqlite** registers the ``sqlite+rapsqlite`` dialect for **async** SQLAlchemy (``create_async_engine``). That stack is for **your** async SQLAlchemy application code, not for the current **Pydantable** wiring described below.

### Async drivers and ``SqlDataFrame`` (today)

The optional SQL bridge ships both **sync** and **async** connection helpers (``ConnectionManager`` vs ``AsyncConnectionManager``), and the latter can build an ``AsyncEngine`` from an async-style DSN (including normalizing ``sqlite://`` → ``sqlite+aiosqlite`` where applicable).

However, the engine pydantable constructs for ``sql_config=`` / ``sql_engine_from_config()`` is implemented on top of the **sync** stack only: it uses ``QueryExecutor`` with a **synchronous** ``ConnectionManager``. Its ``async_execute_plan`` coroutine **offloads** the synchronous ``execute_plan`` to a worker thread (``asyncio.to_thread``), so the event loop stays responsive, but **database I/O is still sync SQLAlchemy** under the hood.

So **you cannot** point ``sql_config=`` at an async-only URL and expect PydanTable to construct ``create_async_engine`` today: ``EngineConfig`` passed through ``ConnectionManager`` must resolve to a **sync** ``Engine`` (see the bridge package’s ``sql/connection.py``).

### If you need async SQLAlchemy today

- Use **sync** DSNs with ``SqlDataFrame`` / ``sql_engine=`` as documented here.
- Use **async** engines and sessions (e.g. ``sqlite+rapsqlite://…``, ``postgresql+asyncpg://…``) in **FastAPI / SQLAlchemy** code paths that **do not** go through the lazy-SQL ``ExecutionEngine``, or use the eager SQL I/O helpers in [IO_SQL](/IO_SQL.md) with your own session/engine.

### Roadmap (upstream)

**Native** async driver support for this path would require the SQL bridge to expose an async-first engine so plan execution uses ``AsyncQueryExecutor`` + ``AsyncConnectionManager`` and ``async_execute_plan`` awaits real async I/O. When that exists, PydanTable can add matching constructors (e.g. async ``sql_config`` resolution) without duplicating SQL logic here.

Until then, ``ato_dict`` / ``acollect`` on ``SqlDataFrame`` still use the engine’s ``async_execute_plan`` entrypoint (thread offload) — see [EXECUTION](/EXECUTION.md).

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see [DATAFRAMEMODEL](/DATAFRAMEMODEL.md), [EXECUTION](/EXECUTION.md)). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — [IO_SQL](/IO_SQL.md) (**SQLModel-first:** **`fetch_sqlmodel`**, **`write_sqlmodel`**, …; **string SQL:** **`fetch_sql_raw`**, **`write_sql_raw`**, …). |
| **Lazy execution** with transforms staying **in SQL** where the engine supports it (plans compiled to SQL; avoid full-table pulls when you only need a terminal write or small materialization) | **`SqlDataFrame`** / **`SqlDataFrameModel`** with **`sql_config=`** or **`sql_engine=`**. |

The SQL I/O helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`SqlDataFrame`** wires the lazy-SQL engine so
`select`, `filter`, `collect`, etc. go through the SQL bridge (subject to what
the engine implements today).

## Imports

```python
# Lazy (optional SQL bridge not imported until accessed)
from pydantable import SqlDataFrame, SqlDataFrameModel

# Explicit
from pydantable.sql_dataframe import (
    SqlDataFrame,
    SqlDataFrameModel,
    sql_engine_from_config,
)
```

If the optional SQL bridge is missing, constructing these classes raises **ImportError**
with an install hint (`pydantable[sql]`).

**pandas- or PySpark-shaped SQL frames** (same lazy-SQL engine; lazy-loaded so
`import pydantable.pandas` does not pull the bridge until you access the
names):

```python
from pydantable.pandas import SqlDataFrame, SqlDataFrameModel  # pandas-style API
from pydantable.pyspark import SqlDataFrame, SqlDataFrameModel  # PySpark-style API

# Or explicitly:
from pydantable.pandas_sql_dataframe import SqlDataFrame, SqlDataFrameModel
from pydantable.pyspark.sql_dataframe import SqlDataFrame, SqlDataFrameModel
```

## `EngineConfig` and constructing an engine

Examples use **`EngineConfig`** from the SQLAlchemy bridge module (``import moltres_core`` after installing ``[sql]``) for SQLAlchemy **2.x** connection
settings. You must supply exactly one of **`dsn`**, **`engine`**, or **`session`**
(see the bridge package docs shipped with the extra).

```python
from moltres_core import EngineConfig

# Example: in-memory SQLite (DSN)
sql_config = EngineConfig(dsn="sqlite:///:memory:")
```

**Reuse one engine** across many frames:

```python
from pydantable.sql_dataframe import sql_engine_from_config

engine = sql_engine_from_config(sql_config)
# Pass sql_engine=engine on each SqlDataFrame / SqlDataFrameModel
```

## `SqlDataFrame`

`SqlDataFrame` subclasses **`DataFrame`**. Use
`SqlDataFrame[YourSchema](data, ...)` the same way as `DataFrame[YourSchema]`,
but you **must** provide a SQL execution backend via one of:

1. **`engine=`** — any object satisfying `ExecutionEngine` (advanced / tests).
2. **`sql_engine=`** — a pre-built lazy-SQL `ExecutionEngine` instance.
3. **`sql_config=`** — an `EngineConfig`; PydanTable builds
   `ConnectionManager` + the bridge engine internally.

Precedence is **exactly** that order: explicit **`engine=`** wins.

### Lazy read from a SQL table

Use **`SqlDataFrame[Schema].from_sql_table(table, sql_config=…)`** (or **`sql_engine=`**) so the frame holds a **`SqlRootData`** root: **no `SELECT` runs** until you call **`to_dict()`**, **`collect()`**, **`head()`**, etc. The **`table`** argument is a SQLAlchemy **`Table`** / **`FromClause`** — with **SQLModel**, pass **`YourModel.__table__`**. Column **names** must match the Pydantic **schema** fields on **`SqlDataFrame`**. For **`SqlDataFrameModel`**, use **`MyModel.read_sql_table(table, …)`**.

For **SQLite in-memory** (`sqlite:///:memory:`), each new SQLAlchemy connection pool is an **empty** database unless you share one **`sql_engine`** for both DDL and the frame. Prefer a **file** URL (`sqlite:///…/app.db`) while wiring this up, or build tables using the same **`ConnectionManager`** / engine you pass as **`sql_engine=`**.

```python
from pathlib import Path
import tempfile

from pydantic import BaseModel
from moltres_core import ConnectionManager, EngineConfig
from sqlmodel import Field, SQLModel, Session

from pydantable.sql_dataframe import SqlDataFrame, sql_engine_from_config


class Row(BaseModel):
    """Pydantable row schema; field names align with the SQL table."""

    id: int
    name: str


class Item(SQLModel, table=True):
    """Physical table; use ``Item.__table__`` for ``from_sql_table``."""

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(max_length=40)


with tempfile.TemporaryDirectory() as td:
    db_file = Path(td) / "app.db"
    cfg = EngineConfig(dsn=f"sqlite:///{db_file}")
    eng = sql_engine_from_config(cfg)
    cm = ConnectionManager(cfg)

    SQLModel.metadata.create_all(cm.engine)
    with Session(cm.engine) as session:
        session.add(Item(id=1, name="a"))
        session.commit()

    df = SqlDataFrame[Row].from_sql_table(Item.__table__, sql_engine=eng)  # lazy
    cols = df.to_dict()  # runs SELECT here
```

You can define a single **SQLModel** class and use it both as the **table** definition and as the **dataframe** schema type (**`SqlDataFrame[ThatModel]`**) when the shapes match; the split above shows how **`Row`** (view schema) and **`Item`** (DDL) line up by column name.

Eager in-memory columns (no SQL root) — same constructor as **`DataFrame`**:

```python
from moltres_core import EngineConfig
from pydantable import SqlDataFrame

cfg = EngineConfig(dsn="sqlite:///:memory:")
df = SqlDataFrame[Row]({"id": [1, 2], "name": ["a", "b"]},
    sql_config=cfg,)
```

Constructor flags (`trusted_mode`, `fill_missing_optional`, validation hooks,
etc.) match `DataFrame` — see [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) and [STRICTNESS](/STRICTNESS.md).

### DataFrame transformations (lazy-SQL engine)

`SqlDataFrame` / `SqlDataFrameModel` use the same **method names** as [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) / [EXECUTION](/EXECUTION.md), but the SQL bridge only implements a subset of the full Polars/Rust pipeline.

**Generally work** (the bridge can execute or fold these into the SQL plan where supported):

- **Projection:** `select`, `drop`
- **Row windows:** `head`, `slice`
- **Order:** `sort` (and related ordering helpers your SQL bridge version exposes)
- **Terminals:** `collect`, `to_dict`, `to_dicts` (materialize rows or column dicts)
- **Async terminals:** `ato_dict`, `acollect`, `ato_dicts`, … (SQL work may run on a thread pool; see [EXECUTION](/EXECUTION.md))

**Example (sync):**

```python
from pydantic import BaseModel
from moltres_core import EngineConfig

from pydantable.sql_dataframe import SqlDataFrame


class Row(BaseModel):
    id: int
    name: str


cfg = EngineConfig(dsn="sqlite:///:memory:")
df = SqlDataFrame[Row]({"id": [3, 1, 2], "name": ["c", "a", "b"]},
    sql_config=cfg,)
assert df.select("name").to_dict() == {"name": ["c", "a", "b"]}
assert df.head(2).to_dict() == {"id": [3, 1], "name": ["c", "a"]}
assert df.sort("id").to_dict() == {"id": [1, 2, 3], "name": ["a", "b", "c"]}
```

**Not supported today:** `filter` / `with_columns` / other paths that require the **native** `Expr` runtime raise **`UnsupportedEngineOperationError`** (see **Expressions** below). For **Expr**-heavy work, use the default **Polars/Rust** engine or materialize with [IO_SQL](/IO_SQL.md) helpers first.

## `SqlDataFrameModel`

`SqlDataFrameModel` subclasses **`DataFrameModel`**
and sets the inner frame class to `SqlDataFrame`. Define columns on a subclass
as usual; pass **`sql_config=`**, **`sql_engine=`**, or **`engine=`** on
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

## FastAPI: shared engine and async terminals

Install **`pydantable[fastapi,sql]`** (or your project’s equivalent). Build **one** shared lazy-SQL engine per process (or pool) and reuse **`sql_engine=`** on frames — do **not** create a fresh `EngineConfig(dsn="sqlite:///:memory:")` per request, or each handler would see an **empty** database.

The pattern below stores the engine on **`app.state`** at startup and returns columnar JSON with **`await …ato_dict()`** (async terminal). Sync routes can call **`to_dict()`** directly. See [FASTAPI](/FASTAPI.md) and [EXECUTION](/EXECUTION.md) for wider FastAPI + pydantable guidance.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from moltres_core import EngineConfig

from pydantable.sql_dataframe import SqlDataFrameModel, sql_engine_from_config


class AppUser(SqlDataFrameModel):
    id: int
    name: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = EngineConfig(dsn="sqlite:///:memory:")  # use a file DSN or pooled URL in production
    app.state.sql_engine = sql_engine_from_config(cfg)
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/users/names")
async def user_names():
    df = AppUser(
        {"id": [1, 2], "name": ["Ada", "Bob"]},
        sql_engine=app.state.sql_engine,
    )
    names = df.select("name")
    return await names.ato_dict()
```

## Expressions (`Expr`) and the native runtime

`Expr`-based `filter` / `with_columns` / … rely on
`pydantable.engine.get_expression_runtime()`, which is tied to the **native**
Rust core when the default engine is `NativePolarsEngine`. With **only** the
lazy-SQL engine bound to your frame, expression-heavy APIs may raise
`UnsupportedEngineOperationError` unless you integrate a compatible expression
runtime — see **Expressions** in [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md) and [ADR-engines](/ADR-engines.md).

Prefer operations your SQL bridge version documents as supported, or use
the native engine for expression-heavy paths.

## File I/O vs execution engine

Lazy **`read_*`** paths can still use **pydantable-native** for local files; that
is separate from **`DataFrame._engine`**. A custom SQL engine does **not**
automatically route `read_parquet` through the database. See **File I/O vs
execution engine** in [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md).

## Versioning

The SQL bridge declares a dependency on **pydantable-protocol** compatible
with your **pydantable** release line. Keep **pydantable**, **pydantable-protocol**,
and the bridge package on mutually supported combinations (see [VERSIONING](/VERSIONING.md)).

## See also

- [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md) — third-party `ExecutionEngine` packages (reference implementations ship alongside the SQL bridge).
- [IO_SQL](/IO_SQL.md) — eager SQL **I/O** (**SQLModel-first:** `fetch_sqlmodel`, `write_sqlmodel`, …; **string SQL:** `*_raw` helpers).
- [SQLMODEL_SQL_ROADMAP](/SQLMODEL_SQL_ROADMAP.md) — SQLModel-first API history and design notes.
- [FASTAPI](/FASTAPI.md) — columnar bodies, NDJSON, and service patterns (works alongside **`SqlDataFrameModel`**).
- [ADR-engines](/ADR-engines.md) — engine abstraction and extension points.
- [EXECUTION](/EXECUTION.md) — materialization and engines.
- [DATAFRAMEMODEL](/DATAFRAMEMODEL.md) — `DataFrameModel` patterns shared by `SqlDataFrameModel`.
