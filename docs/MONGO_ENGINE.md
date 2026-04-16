# Mongo: Beanie, lazy engine, and column-dict I/O

**Primary model for MongoDB with PydanTable:** define collections with [Beanie](https://github.com/BeanieODM/beanie) **`Document`** subclasses, then wire **lazy** **`MongoDataFrame`** / **`MongoDataFrameModel`** (**`from_beanie`**, **`from_beanie_async`**) and **eager** **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** (and async **`afetch_mongo`** / **`aiter_mongo`** / **`awrite_mongo`**) through **`sync_pymongo_collection`** where you use a sync DB. Install **`pip install "pydantable[mongo]"`** (pulls **PyMongo**, **Beanie**, and the optional Mongo plan stack used by lazy frames). ODM-first patterns: [BEANIE](/BEANIE.md).

**Also supported:** Pydantic **`Schema`** / **`MongoDataFrameModel`** with **`from_collection(coll)`** when you already hold a **sync** PyMongo **`Collection`** and are not using Beanie. That path is fine for tests or thin scripts; for applications, prefer **Beanie** as the single source of truth for collection names, indexes, and document shape.

**Topics here:** (1) lazy **`MongoDataFrame`** / **`MongoDataFrameModel`** with pydantable’s **`MongoPydantableEngine`**; (2) eager column-dict I/O — the plan stack is **not** required for (2) alone.

This guide covers the **optional** integration between PydanTable, the Mongo plan
library ( **`MongoRoot`** and columnar scans — installed with **`[mongo]`**), and
**`pydantable.mongo_dataframe_engine.MongoPydantableEngine`**, which implements the
same **`ExecutionEngine`** protocol as **`pydantable.engine.protocols`** (from
[**pydantable-protocol**](https://pypi.org/project/pydantable-protocol/) on PyPI;
see [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md)).

**`MongoRoot`** is a plan root that binds materialization to a MongoDB collection
(via PyMongo). Planning still uses the **native** Rust planner; at execution time
**`MongoPydantableEngine`** turns **`MongoRoot`** into columnar **`dict[str, list]`**
via the plan library, then runs the native executor.

The parallel SQL-backed story is [SQL_ENGINE](/SQL_ENGINE.md) (**`SqlDataFrame`** /
**`SqlDataFrameModel`** with the lazy-SQL stack).

**Compatibility (1.17.0):** **`pydantable[mongo]`** pins the Mongo plan package to
**`>=0.2.0,<0.3`** (see **`pyproject.toml`**). Install matching releases before using lazy **`MongoDataFrame`** facades.

!!! note
    **Install:** ``pip install "pydantable[mongo]"`` pulls **pymongo**, **Beanie**, and the Mongo plan stack. The core **pydantable** package does not import the plan stack at import time; ``MongoDataFrame`` / ``MongoDataFrameModel`` resolve only when accessed.


## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see [DATAFRAMEMODEL](/DATAFRAMEMODEL.md), [EXECUTION](/EXECUTION.md)). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — [IO_SQL](/IO_SQL.md) (**`fetch_sqlmodel`**, **`write_sqlmodel`**, …). |
| **Eager** Mongo I/O: **`dict[str, list]`** in / out of a collection (no **`DataFrame`**) | **`fetch_mongo`**, **`iter_mongo`**, **`write_mongo`** and **`afetch_mongo`**, **`aiter_mongo`**, **`awrite_mongo`** — ideally with **`sync_pymongo_collection(MyDocument, sync_db)`** for sync **`Collection`** ({ref}`mongo-eager-beanie`); **`AsyncCollection`** uses native async (see **PyMongo surface area** below). |
| **Lazy execution** with transforms compiled to **SQL** (lazy-SQL bridge) | **`SqlDataFrame`** / **`SqlDataFrameModel`** — [SQL_ENGINE](/SQL_ENGINE.md). |
| **Lazy execution** over a **MongoDB collection** with the same typed **`DataFrame`** API | **`MongoDataFrame`** / **`MongoDataFrameModel`** — **`from_beanie`** or **`from_beanie_async`** with a Beanie **`Document`** (or **`from_collection`**); see {ref}`mongo-primary-beanie` and [BEANIE](/BEANIE.md). |

Eager SQL helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`MongoDataFrame`** uses **`MongoPydantableEngine`** as that engine so
`select`, `filter`, `collect`, etc. go through the native planner and executor (with
**`MongoRoot`** materialized via the plan library when needed).

(mongo-primary-beanie)=
## Primary path: Beanie `Document` models

[Beanie](https://github.com/BeanieODM/beanie) is the **recommended** ODM for MongoDB here: one **`Document`** class per collection, Pydantic-shaped fields, and **`get_collection_name()`** after **`init_beanie`**.

Beanie uses PyMongo’s **async** API (`AsyncMongoClient`, `AsyncDatabase`, …). Pydantable’s **`MongoRoot`** / **`fetch_mongo`** paths need a **sync** `pymongo.database.Database` and **`pymongo.collection.Collection`** (`find()`, `insert_many()`). Use a **synchronous** `MongoClient(uri).dbname` whose **database name** matches the **`AsyncDatabase`** you pass to **`await init_beanie(database=...)`**.

- **`MongoDataFrame[Row].from_beanie(MyDocument, database=sync_db)`** — lazy typed transforms over that collection.
- **`fetch_mongo(sync_pymongo_collection(MyDocument, sync_db))`** — eager **`dict[str, list]`** without building a **`DataFrame`** plan.
- **`write_mongo(sync_pymongo_collection(MyDocument, sync_db), data)`** — inserts from a rectangular column dict.

At runtime, :func:`~pydantable.mongo_beanie.sync_pymongo_collection` only needs **pymongo** (or **mongomock** in tests); it does **not** import Beanie—it only calls **`get_collection_name()`** on your class.

```python
from pymongo import MongoClient

from beanie import Document, init_beanie
from pydantic import Field

from pydantable import MongoDataFrame, Schema, fetch_mongo, sync_pymongo_collection, write_mongo


class Item(Document):
    x: int = Field(...)
    label: str | None = None


class Row(Schema):
    """Pydantic schema for the ``DataFrame`` row type (align fields with ``Item``)."""

    x: int
    label: str | None = None


async def setup(async_client, sync_uri: str) -> None:
    await init_beanie(database=async_client.myapp, document_models=[Item])
    # sync client for pydantable — same DB name as ``async_client.myapp``
    sync_db = MongoClient(sync_uri).myapp
    df = MongoDataFrame[Row].from_beanie(Item, database=sync_db)
    cols = fetch_mongo(sync_pymongo_collection(Item, sync_db))
    _ = write_mongo(sync_pymongo_collection(Item, sync_db), {"x": [1], "label": ["a"]})
```

### `MongoDataFrameModel` with Beanie

Use **`MyModel.from_beanie(Item, database=sync_db)`** on a concrete **`MongoDataFrameModel`** subclass whose schema matches the documents you read.

(mongo-eager-beanie)=
### Eager column-dict I/O with Beanie

Prefer **`sync_pymongo_collection(DocumentClass, sync_db)`** as the **`collection`** argument to **`fetch_mongo`**, **`iter_mongo`**, and **`write_mongo`** so collection names stay aligned with Beanie.

| Sync | Async |
| ---- | ----- |
| **`fetch_mongo(sync_pymongo_collection(Doc, db), ...)`** → **`dict[str, list]`** | **`await afetch_mongo(...)`** |
| **`iter_mongo(sync_pymongo_collection(Doc, db), ...)`** | **`async for batch in aiter_mongo(...)`** |
| **`write_mongo(sync_pymongo_collection(Doc, db), data, ...)`** | **`await awrite_mongo(...)`** |

!!! note
    **ODM hooks:** ``write_mongo`` / ``awrite_mongo`` are **driver-level** inserts (PyMongo) from a rectangular column dict. They do **not** run Beanie's ``validate_on_save`` or event-based actions. For ODM-aware inserts that execute Beanie hooks, use **`await awrite_beanie(MyDocument, data)`** (see below).


**`fetch_mongo`** materializes the full cursor in memory; for large scans prefer **`iter_mongo`**.

## Eager column-dict I/O (PyMongo `Collection`)

Same pattern as **SQL** eager helpers ([IO_SQL](/IO_SQL.md)): import **from `pydantable`**
(not `pydantable.io` in application code). These use **PyMongo** only (they do **not**
require the Mongo plan stack), but **`pydantable[mongo]`** installs **pymongo** and **Beanie** for you.

If you are **not** using Beanie, pass any **sync** **`Collection`** you already have:

| Sync | Async |
| ---- | ----- |
| **`fetch_mongo(collection, match=..., projection=..., sort=..., skip=..., limit=..., fields=..., session=..., max_time_ms=...)`** → **`dict[str, list]`** | **`await afetch_mongo(...)`** |
| **`iter_mongo(..., batch_size=...)`** → yields rectangular batches | **`async for batch in aiter_mongo(...)`** |
| **`write_mongo(collection, data, ordered=..., chunk_size=..., session=...)`** → inserted row count | **`await awrite_mongo(...)`** |

### PyMongo surface area (what pydantable wraps)

Pydantable’s optional Mongo helpers are built for **rectangular column dicts** and the same **typed DataFrame** story as SQL I/O — not a full mirror of the [PyMongo](https://pymongo.readthedocs.io/en/stable/) API.

**Wrapped for sync `pymongo.collection.Collection`:**

- Reads: `find` → optional `sort`, `skip`, `limit`, cursor `batch_size`, `max_time_ms`, and optional **`ClientSession`** via `session=`.
- Writes: chunked `insert_many` with `ordered=` and optional `session=`.

**Async helpers (`afetch_mongo`, `aiter_mongo`, `awrite_mongo`):**

- If `collection` is a **`pymongo.asynchronous.collection.AsyncCollection`**, pydantable uses the **native async** PyMongo API (`async for` on the cursor, `await insert_many`). Use **`is_async_mongo_collection(collection)`** to branch in application code.
- If `collection` is a **sync** `Collection`, these functions still offload blocking I/O with **`asyncio.to_thread`** (or an optional **`Executor`**), same as before.

**Low-level helpers** (also importable from **`pydantable`**): **`afetch_mongo_async`**, **`aiter_mongo_async`**, **`awrite_mongo_async`** — identical semantics but **only** for async collections.

**Out of scope** (use PyMongo or Beanie directly): aggregation pipelines, change streams, GridFS, CSFLE, `bulk_write` / upserts, collations and other `find` options not listed above, and lazy scan tuning inside the plan library’s `MongoRoot`.

## Async-first Beanie ODM I/O (no sync Collection required)

When your application is already using Beanie's async ODM, you can stay fully in that world for eager I/O:

- **`await afetch_beanie(MyDocument, ...)`** → **`dict[str, list]`**
- **`async for batch in aiter_beanie(MyDocument, ...)`** → rectangular batches
- **`await awrite_beanie(MyDocument, data, ...)`** → inserts via Beanie so **`validate_on_save`** and **event-based actions** can run

These APIs also accept a **Beanie query object** (for example, the result of ``MyDocument.find(...)``) so you can use Beanie's operator DSL, projections, and ``fetch_links`` behavior.

### ODM-aware inserts (`awrite_beanie`)

Beanie supports on-save validation (`Settings.validate_on_save = True`) and event-based actions (``@before_event`` / ``@after_event``). See Beanie docs:

- [On save validation](https://beanie-odm.dev/tutorial/on-save-validation/)
- [Event-based actions](https://beanie-odm.dev/tutorial/event-based-actions/)

Pydantable's **`awrite_beanie`** inserts rows by constructing Beanie documents and calling ``await doc.insert(...)`` so those behaviors can run.

### Relations / links (`fetch_links=True`)

Beanie can prefetch linked documents with ``fetch_links=True`` (and optional nesting depth controls). See [Relations](https://beanie-odm.dev/tutorial/relations/).

When you call **`afetch_beanie(..., fetch_links=True)`**, nested documents are flattened into **dot-path columns** by default (for example ``door.height``).

## Async-first lazy execution (`MongoDataFrame.from_beanie_async`)

If you want the **lazy** `MongoDataFrame` / `MongoDataFrameModel` API over a Beanie `Document` without wiring a sync PyMongo client, use:

- **`MongoDataFrame[Row].from_beanie_async(MyDocument, ...)`** — first argument can also be a **pre-built Beanie query** (e.g. `MyDocument.find(...).sort(...)`) with the same semantics as **`afetch_beanie`**.
- **`MyModel.from_beanie_async(MyDocument, ...)`** (where `MyModel` subclasses `MongoDataFrameModel`)

This root is **async-only**: materialize with **`await acollect()`** / **`await ato_dict()`**. Sync terminals (`collect`, `to_dict`, `write_parquet`, ...) will raise.

## Alternative: Pydantic `Schema` only (no Beanie)

You can skip Beanie and pass a **sync** PyMongo **`Collection`** directly. This is supported for **tests**, **prototypes**, or when another layer owns the driver—but **Beanie remains the recommended primary model** for application code.

### `MongoDataFrame`

```python
from pydantable import MongoDataFrame, Schema


class Row(Schema):
    x: int
    y: str | None = None


# coll = mongo_client.db.my_collection  # sync Collection
df = MongoDataFrame[Row].from_collection(coll)
```

Optional **`fields=`** limits which document keys are read (defaults to all keys
in the schema’s field map). Optional **`engine=`** reuses a single
**`MongoPydantableEngine`** across many frames.

Materialization (`collect`, `to_dict`, `acollect`, …) follows [EXECUTION](/EXECUTION.md) and
uses the engine’s **`execute_plan`** / **`async_execute_plan`** entrypoints.

### `MongoDataFrameModel`

```python
from pydantable import MongoDataFrameModel


class RowModel(MongoDataFrameModel):
    x: int
    y: str | None = None


m = RowModel.from_collection(coll)
rows = m.rows()
```

## Imports

```python
# Lazy (Mongo plan stack not imported until accessed)
from pydantable import (
    MongoDataFrame,
    MongoDataFrameModel,
    MongoPydantableEngine,
    MongoRoot,
    sync_pymongo_collection,
)

# Explicit (``MongoPydantableEngine`` is defined in ``pydantable.mongo_dataframe_engine``)
from pydantable.mongo_dataframe import (
    MongoDataFrame,
    MongoDataFrameModel,
    MongoPydantableEngine,
)
# ``MongoRoot`` is defined by the optional Mongo plan package (``pip install "pydantable[mongo]"``).
from entei_core import MongoRoot
```

If the Mongo plan stack is missing, constructing these classes or resolving the lazy
aliases raises **ImportError** with an install hint (`pydantable[mongo]`).

## Engine and `MongoRoot` in application code

For low-level tests or custom wiring, import **`MongoPydantableEngine`** from
**`pydantable`** (lazy) or **`pydantable.mongo_dataframe_engine`**, and **`MongoRoot`**
from **`pydantable`** (lazy) or from the Mongo plan package module (``entei_core`` after ``[mongo]``). **`MongoRoot(collection, fields=...)`**
is the root object passed into plan execution when data should be read from MongoDB
rather than from an in-memory column dict.

## See also

- [IO_OVERVIEW](/IO_OVERVIEW.md) — where **`fetch_mongo`** / **`iter_mongo`** fit in the broader I/O surface.
- [CUSTOM_ENGINE_PACKAGE](/CUSTOM_ENGINE_PACKAGE.md) — third-party **`ExecutionEngine`** packages.
- [ADR-engines](/ADR-engines.md) — engine abstraction overview.
- [DEVELOPER](/DEVELOPER.md) — **`make test-mongo`** runs **`tests/mongo/`** (e.g. **mongomock**);
  the Mongo plan package’s own tests ship with that distribution.
