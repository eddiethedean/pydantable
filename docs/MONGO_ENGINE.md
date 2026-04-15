# Mongo: Beanie, lazy engine, and column-dict I/O

**Primary model for MongoDB with PydanTable:** define collections with [Beanie](https://github.com/BeanieODM/beanie) **`Document`** subclasses, then wire **lazy** **`EnteiDataFrame`** / **`EnteiDataFrameModel`** and **eager** **`fetch_mongo`** / **`iter_mongo`** / **`write_mongo`** through **`from_beanie`** and **`sync_pymongo_collection`**. Install **`pip install "pydantable[mongo]"`** (**entei-core**, **pymongo**, and **Beanie**).

**Also supported:** Pydantic **`Schema`** / **`EnteiDataFrameModel`** with **`from_collection(coll)`** when you already hold a **sync** PyMongo **`Collection`** and are not using Beanie. That path is fine for tests or thin scripts; for applications, prefer **Beanie** as the single source of truth for collection names, indexes, and document shape.

**Topics here:** (1) lazy **`EnteiDataFrame`** / **`EnteiDataFrameModel`** with pydantable’s **`EnteiPydantableEngine`**; (2) eager column-dict I/O — no **entei-core** required for (2) alone.

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
**`entei-core`** release before using lazy **Entei** facades.

```{note}
**Install:** ``pip install "pydantable[mongo]"`` pulls **entei-core**, **pymongo**, and **Beanie**. The core **pydantable** package does not import **entei-core** at import time; ``EnteiDataFrame`` / ``EnteiDataFrameModel`` and the lazy aliases below resolve only when accessed.
```

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see {doc}`DATAFRAMEMODEL`, {doc}`EXECUTION`). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — {doc}`IO_SQL` (**`fetch_sqlmodel`**, **`write_sqlmodel`**, …). |
| **Eager** Mongo I/O: **`dict[str, list]`** in / out of a collection (no **`DataFrame`**) | **`fetch_mongo`**, **`iter_mongo`**, **`write_mongo`** — ideally with **`sync_pymongo_collection(MyDocument, sync_db)`** ({ref}`mongo-eager-beanie`). |
| **Lazy execution** with transforms compiled to **SQL** (Moltres) | **`SqlDataFrame`** / **`SqlDataFrameModel`** — {doc}`MOLTRES_SQL`. |
| **Lazy execution** over a **MongoDB collection** with the same typed **`DataFrame`** API | **`EnteiDataFrame`** / **`EnteiDataFrameModel`** — **prefer `from_beanie`** with a Beanie **`Document`**; see {ref}`mongo-primary-beanie`. |

Eager SQL helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`EnteiDataFrame`** uses **`EnteiPydantableEngine`** as that engine so
`select`, `filter`, `collect`, etc. go through the native planner and executor (with
**`MongoRoot`** materialized via **entei-core** when needed).

(mongo-primary-beanie)=
## Primary path: Beanie `Document` models

[Beanie](https://github.com/BeanieODM/beanie) is the **recommended** ODM for MongoDB here: one **`Document`** class per collection, Pydantic-shaped fields, and **`get_collection_name()`** after **`init_beanie`**.

Beanie uses PyMongo’s **async** API (`AsyncMongoClient`, `AsyncDatabase`, …). Pydantable’s **`MongoRoot`** / **`fetch_mongo`** paths need a **sync** `pymongo.database.Database` and **`pymongo.collection.Collection`** (`find()`, `insert_many()`). Use a **synchronous** `MongoClient(uri).dbname` whose **database name** matches the **`AsyncDatabase`** you pass to **`await init_beanie(database=...)`**.

- **`EnteiDataFrame[Row].from_beanie(MyDocument, database=sync_db)`** — lazy typed transforms over that collection.
- **`fetch_mongo(sync_pymongo_collection(MyDocument, sync_db))`** — eager **`dict[str, list]`** without building a **`DataFrame`** plan.
- **`write_mongo(sync_pymongo_collection(MyDocument, sync_db), data)`** — inserts from a rectangular column dict.

At runtime, :func:`~pydantable.mongo_beanie.sync_pymongo_collection` only needs **pymongo** (or **mongomock** in tests); it does **not** import Beanie—it only calls **`get_collection_name()`** on your class.

```python
from pymongo import MongoClient

from beanie import Document, init_beanie
from pydantic import Field

from pydantable import EnteiDataFrame, Schema, fetch_mongo, sync_pymongo_collection, write_mongo


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
    df = EnteiDataFrame[Row].from_beanie(Item, database=sync_db)
    cols = fetch_mongo(sync_pymongo_collection(Item, sync_db))
    _ = write_mongo(sync_pymongo_collection(Item, sync_db), {"x": [1], "label": ["a"]})
```

### `EnteiDataFrameModel` with Beanie

Use **`MyModel.from_beanie(Item, database=sync_db)`** on a concrete **`EnteiDataFrameModel`** subclass whose schema matches the documents you read.

(mongo-eager-beanie)=
### Eager column-dict I/O with Beanie

Prefer **`sync_pymongo_collection(DocumentClass, sync_db)`** as the **`collection`** argument to **`fetch_mongo`**, **`iter_mongo`**, and **`write_mongo`** so collection names stay aligned with Beanie.

| Sync | Async |
| ---- | ----- |
| **`fetch_mongo(sync_pymongo_collection(Doc, db), ...)`** → **`dict[str, list]`** | **`await afetch_mongo(...)`** |
| **`iter_mongo(sync_pymongo_collection(Doc, db), ...)`** | **`async for batch in aiter_mongo(...)`** |
| **`write_mongo(sync_pymongo_collection(Doc, db), data, ...)`** | **`await awrite_mongo(...)`** |

```{note}
**ODM hooks:** ``write_mongo`` / ``awrite_mongo`` are **driver-level** inserts (PyMongo) from a rectangular column dict. They do **not** run Beanie's ``validate_on_save`` or event-based actions. For ODM-aware inserts that execute Beanie hooks, use **`await awrite_beanie(MyDocument, data)`** (see below).
```

**`fetch_mongo`** materializes the full cursor in memory; for large scans prefer **`iter_mongo`**.

## Eager column-dict I/O (PyMongo `Collection`)

Same pattern as **SQL** eager helpers ({doc}`IO_SQL`): import **from `pydantable`**
(not `pydantable.io` in application code). These use **PyMongo** only (they do **not**
require **entei-core**), but **`pydantable[mongo]`** installs **pymongo** and **Beanie** for you.

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

**Out of scope** (use PyMongo or Beanie directly): aggregation pipelines, change streams, GridFS, CSFLE, `bulk_write` / upserts, collations and other `find` options not listed above, and lazy **Entei** scan tuning inside **entei-core**’s `MongoRoot`.

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

## Async-first lazy execution (`EnteiDataFrame.from_beanie_async`)

If you want the **lazy** `EnteiDataFrame` / `EnteiDataFrameModel` API over a Beanie `Document` without wiring a sync PyMongo client, use:

- **`EnteiDataFrame[Row].from_beanie_async(MyDocument, ...)`** — first argument can also be a **pre-built Beanie query** (e.g. `MyDocument.find(...).sort(...)`) with the same semantics as **`afetch_beanie`**.
- **`MyModel.from_beanie_async(MyDocument, ...)`** (where `MyModel` subclasses `EnteiDataFrameModel`)

This root is **async-only**: materialize with **`await acollect()`** / **`await ato_dict()`**. Sync terminals (`collect`, `to_dict`, `write_parquet`, ...) will raise.

## Alternative: Pydantic `Schema` only (no Beanie)

You can skip Beanie and pass a **sync** PyMongo **`Collection`** directly. This is supported for **tests**, **prototypes**, or when another layer owns the driver—but **Beanie remains the recommended primary model** for application code.

### `EnteiDataFrame`

```python
from pydantable import EnteiDataFrame, Schema


class Row(Schema):
    x: int
    y: str | None = None


# coll = mongo_client.db.my_collection  # sync Collection
df = EnteiDataFrame[Row].from_collection(coll)
```

Optional **`fields=`** limits which document keys are read (defaults to all keys
in the schema’s field map). Optional **`engine=`** reuses a single
**`EnteiPydantableEngine`** across many frames.

Materialization (`collect`, `to_dict`, `acollect`, …) follows {doc}`EXECUTION` and
uses the engine’s **`execute_plan`** / **`async_execute_plan`** entrypoints.

### `EnteiDataFrameModel`

```python
from pydantable import EnteiDataFrameModel


class RowModel(EnteiDataFrameModel):
    x: int
    y: str | None = None


m = RowModel.from_collection(coll)
rows = m.rows()
```

## Imports

```python
# Lazy (no entei-core import until accessed)
from pydantable import (
    EnteiDataFrame,
    EnteiDataFrameModel,
    EnteiPydantableEngine,
    MongoRoot,
    sync_pymongo_collection,
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
