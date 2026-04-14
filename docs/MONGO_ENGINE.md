# Mongo engine: `EnteiDataFrame` and `EnteiDataFrameModel`

This guide covers the **optional** integration between PydanTable and
[**entei-core**](https://pypi.org/project/entei-core/) on PyPI: typed
`DataFrame` / `DataFrameModel` instances whose **execution engine** is
`entei_core.EnteiPydantableEngine`, implementing the same **`ExecutionEngine`**
protocol as **`pydantable.engine.protocols`** (from
[**pydantable-protocol**](https://pypi.org/project/pydantable-protocol/) on PyPI;
see {doc}`CUSTOM_ENGINE_PACKAGE`).

**entei-core** also supplies **`MongoRoot`**, a plan root that binds materialization
to a MongoDB collection (via PyMongo). Planning still uses the **native** Rust
planner; execution pushes work into **entei-core**’s Mongo-aware path where
applicable.

The parallel SQL-backed story is {doc}`MOLTRES_SQL` (**`SqlDataFrame`** /
**`SqlDataFrameModel`** with **moltres-core**).

```{note}
**Install:** ``pip install "pydantable[mongo]"`` (pulls **entei-core**) or
``pip install entei-core``. You also need a MongoDB client such as **pymongo**
at runtime. The core **pydantable** package does not import **entei-core** at
import time; ``EnteiDataFrame`` / ``EnteiDataFrameModel`` and the lazy aliases
below resolve only when accessed.
```

## When to use this

| Goal | Use |
| ---- | --- |
| Default Polars/Rust execution for in-memory or file-backed workflows | `DataFrame` / `DataFrameModel` (see {doc}`DATAFRAMEMODEL`, {doc}`EXECUTION`). |
| **Eager** SQL I/O: load columns from a DB into a frame, or write tables | **`from pydantable import …`** — {doc}`IO_SQL` (**`fetch_sqlmodel`**, **`write_sqlmodel`**, …). |
| **Lazy execution** with transforms compiled to **SQL** (Moltres) | **`SqlDataFrame`** / **`SqlDataFrameModel`** — {doc}`MOLTRES_SQL`. |
| **Lazy execution** over a **MongoDB collection** with the same typed **`DataFrame`** API | **`EnteiDataFrame`** / **`EnteiDataFrameModel`** — this page. |

Eager SQL helpers materialize **column dicts** in Python; they do not replace
`DataFrame._engine`. **`EnteiDataFrame`** wires **entei-core** as that engine so
`select`, `filter`, `collect`, etc. go through **`EnteiPydantableEngine`** (subject
to what the engine implements for Mongo-backed plans).

## Imports

```python
# Lazy (no entei-core import until accessed)
from pydantable import (
    EnteiDataFrame,
    EnteiDataFrameModel,
    EnteiPydantableEngine,
    MongoRoot,
)

# Explicit
from pydantable.mongo_entei import (
    EnteiDataFrame,
    EnteiDataFrameModel,
)
```

If **entei-core** is missing, constructing these classes or resolving the lazy
aliases raises **ImportError** with an install hint (`pydantable[mongo]` or
`entei-core`).

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

For low-level tests or custom wiring, you can import **`EnteiPydantableEngine`**
and **`MongoRoot`** from **`pydantable`** (lazy) or from **`entei_core`** directly.
**`MongoRoot(collection, fields=...)`** is the root object passed into plan
execution when data should be read from MongoDB rather than from an in-memory
column dict.

## See also

- {doc}`CUSTOM_ENGINE_PACKAGE` — third-party **`ExecutionEngine`** packages.
- {doc}`ADR-engines` — engine abstraction overview.
- {doc}`DEVELOPER` — **`make test-mongo`** runs **`tests/mongo/`** (e.g. **mongomock**);
  **entei-core**’s own tests ship with that distribution.
