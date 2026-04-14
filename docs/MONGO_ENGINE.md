# Mongo engine: `EnteiDataFrame` and `EnteiDataFrameModel`

**Topics here:** (1) lazy **`EnteiDataFrame`** / **`EnteiDataFrameModel`** with pydantable’s **`EnteiPydantableEngine`**; (2) eager **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** (and async mirrors) for **PyMongo** column dicts — no **entei-core** required for (2).

This guide covers the **optional** integration between PydanTable,
[**entei-core**](https://pypi.org/project/entei-core/) (**`MongoRoot`** and
columnar scans), and **`pydantable.mongo_entei_engine.EnteiPydantableEngine`**, which
implements the same **`ExecutionEngine`** protocol as **`pydantable.engine.protocols`**
(from [**pydantable-protocol**](https://pypi.org/project/pydantable-protocol/) on PyPI;
see {doc}`CUSTOM_ENGINE_PACKAGE`).

**`MongoRoot`** (from **entei-core**) is a plan root that binds materialization to a
MongoDB collection (via PyMongo). Planning still uses the **native** Rust planner;
at execution time **`EnteiPydantableEngine`** turns **`MongoRoot`** into columnar
**`dict[str, list]`** via **entei-core**, then runs the native executor.

The parallel SQL-backed story is {doc}`MOLTRES_SQL` (**`SqlDataFrame`** /
**`SqlDataFrameModel`** with **moltres-core**).

**Compatibility (1.17.0):** **`pydantable[mongo]`** pins **`entei-core`** to
**`>=0.2.0,<0.3`** (see **`pyproject.toml`**). Install a matching **PyPI**
**`entei-core`** release before using these facades.

```{note}
**Install:** ``pip install "pydantable[mongo]"`` (pulls **entei-core**, **pymongo**) or
``pip install "entei-core>=0.2.0,<0.3"`` and ``pip install pymongo``. The core **pydantable** package does not import
**entei-core** at import time; ``EnteiDataFrame`` / ``EnteiDataFrameModel`` and the
lazy aliases below resolve only when accessed.
```

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see {doc}`DATAFRAMEMODEL`, {doc}`EXECUTION`). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — {doc}`IO_SQL` (**`fetch_sqlmodel`**, **`write_sqlmodel`**, …). |
| **Eager** Mongo I/O: **`dict[str, list]`** in / out of a collection (no **`DataFrame`**) | **`fetch_mongo`**, **`iter_mongo`**, **`write_mongo`** and **`afetch_mongo`**, **`aiter_mongo`**, **`awrite_mongo`** — below. |
| **Lazy execution** with transforms compiled to **SQL** (Moltres) | **`SqlDataFrame`** / **`SqlDataFrameModel`** — {doc}`MOLTRES_SQL`. |
| **Lazy execution** over a **MongoDB collection** with the same typed **`DataFrame`** API | **`EnteiDataFrame`** / **`EnteiDataFrameModel`** — this page. |

Eager SQL helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`EnteiDataFrame`** uses **`EnteiPydantableEngine`** as that engine so
`select`, `filter`, `collect`, etc. go through the native planner and executor (with
**`MongoRoot`** materialized via **entei-core** when needed).

## Imports

```python
# Lazy (no entei-core import until accessed)
from pydantable import (
    EnteiDataFrame,
    EnteiDataFrameModel,
    EnteiPydantableEngine,
    MongoRoot,
)

# Explicit (``EnteiPydantableEngine`` is defined in ``pydantable.mongo_entei_engine``)
from pydantable.mongo_entei import (
    EnteiDataFrame,
    EnteiDataFrameModel,
    EnteiPydantableEngine,
)
from entei_core import MongoRoot
```

If **entei-core** is missing, constructing these classes or resolving the lazy
aliases raises **ImportError** with an install hint (`pydantable[mongo]` or
`entei-core`).

## Eager column-dict I/O (PyMongo)

Same pattern as **SQL** eager helpers ({doc}`IO_SQL`): import **from `pydantable`**
(not `pydantable.io` in application code). These use **PyMongo** only (they do **not**
require **entei-core**), but **`pydantable[mongo]`** installs **pymongo** for you.

| Sync | Async |
| ---- | ----- |
| **`fetch_mongo(collection, match=..., projection=..., sort=..., limit=..., fields=...)`** → **`dict[str, list]`** | **`await afetch_mongo(...)`** |
| **`iter_mongo(..., batch_size=...)`** → yields rectangular batches | **`async for batch in aiter_mongo(...)`** |
| **`write_mongo(collection, data, ordered=..., chunk_size=...)`** → inserted row count | **`await awrite_mongo(...)`** |

**`fetch_mongo`** materializes the full cursor in memory; for large scans prefer **`iter_mongo`**.
**`write_mongo`** uses **`insert_many`** from a rectangular column dict (same row count in every column).

## `EnteiDataFrame`

`EnteiDataFrame` subclasses **`DataFrame`**. Define a **`Schema`** subclass, then
build a lazy frame from a PyMongo **`Collection`**:

```python
from pydantable import EnteiDataFrame, Schema


class Row(Schema):
    x: int
    y: str | None = None


# coll = mongo_client.db.my_collection
df = EnteiDataFrame[Row].from_collection(coll)
```

Optional **`fields=`** limits which document keys are read (defaults to all keys
in the schema’s field map). Optional **`engine=`** reuses a single
**`EnteiPydantableEngine`** across many frames.

Materialization (`collect`, `to_dict`, `acollect`, …) follows {doc}`EXECUTION` and
uses the engine’s **`execute_plan`** / **`async_execute_plan`** entrypoints.

## `EnteiDataFrameModel`

**`EnteiDataFrameModel`** subclasses **`DataFrameModel`** and defaults to
**`EnteiPydantableEngine`**. Use **`from_collection`** on a concrete model class
to get a model instance backed by a collection:

```python
from pydantable import EnteiDataFrameModel


class RowModel(EnteiDataFrameModel):
    x: int
    y: str | None = None


m = RowModel.from_collection(coll)
rows = m.rows()
```

## Engine and `MongoRoot` in application code

For low-level tests or custom wiring, import **`EnteiPydantableEngine`** from
**`pydantable`** (lazy) or **`pydantable.mongo_entei_engine`**, and **`MongoRoot`**
from **`pydantable`** (lazy) or **`entei_core`**. **`MongoRoot(collection, fields=...)`**
is the root object passed into plan execution when data should be read from MongoDB
rather than from an in-memory column dict.

## See also

- {doc}`IO_OVERVIEW` — where **`fetch_mongo`** / **`iter_mongo`** fit in the broader I/O surface.
- {doc}`CUSTOM_ENGINE_PACKAGE` — third-party **`ExecutionEngine`** packages.
- {doc}`ADR-engines` — engine abstraction overview.
- {doc}`DEVELOPER` — **`make test-mongo`** runs **`tests/mongo/`** (e.g. **mongomock**);
  **entei-core**’s own tests ship with that distribution.
