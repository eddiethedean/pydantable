# Beanie ODM integration (MongoDB)

This page documents the **Beanie-first** MongoDB integration in PydanTable.

Beanie is an async ODM for MongoDB (Pydantic-based). See the upstream docs:
[Beanie documentation](https://beanie-odm.dev/).

## What pydantable supports (feature map)

PydanTable splits MongoDB support into **three** layers. Which one you should use
depends on whether you want **driver-level** I/O, **ODM-aware** behavior (hooks),
or **lazy DataFrame transforms**.

| Goal | Recommended API |
|------|------------------|
| **Eager** column-dict I/O with a sync PyMongo `Collection` | `fetch_mongo` / `iter_mongo` / `write_mongo` |
| **Eager** column-dict I/O using Beanie async ODM features (no sync client) | `afetch_beanie` / `aiter_beanie` / `awrite_beanie` |
| **Lazy** `DataFrame` API over a Mongo collection (typed transforms, then materialize) | `EnteiDataFrame` / `EnteiDataFrameModel` |

This page focuses on the Beanie-first pieces. For the engine details and the
entei-core `MongoRoot` story, see {doc}`MONGO_ENGINE`.

## Install

```bash
pip install "pydantable[mongo]"
```

This installs:

- **Beanie** (ODM)
- **pymongo** (driver)
- **entei-core** (lazy Mongo roots + columnar materialization used by the Entei engine)

## Beanie settings that matter

Beanie config lives on the `Document.Settings` inner class (upstream: [Defining a document](https://beanie-odm.dev/tutorial/defining-a-document/)).

Relevant settings for pydantable usage:

- **Collection name**: `Settings.name`
- **Indexes**: `Settings.indexes` (including `Indexed(...)` fields)
- **Encoders**: `Settings.bson_encoders` (how Python values are represented in BSON)
- **Keep nulls**: `Settings.keep_nulls`
- **Validation-on-save**: `Settings.validate_on_save`
- **Link nesting depth limits**: `Settings.max_nesting_depth*`

PydanTable treats Beanie as the **source of truth** for:

- what collection to read/write (via `Document.get_collection_name()`)
- whether you want ODM-level hooks/validation (when you choose ODM-aware APIs)

## Eager async Beanie I/O (full ODM leverage)

These functions return **`dict[str, list]`** like other eager I/O helpers.

### `afetch_beanie` (load all results)

Use this when you want a single in-memory column dict:

```python
from pydantable import afetch_beanie

# docs = await MyDocument.find(...).to_list() but columnar
cols = await afetch_beanie(MyDocument)
```

You can also pass a **Beanie query object** directly, so you can use Beanie’s full
query DSL (operators, chained `.find()`, `.sort()`, `.project()`, etc.) upstream:

```python
query = MyDocument.find(MyDocument.some_field == 1, fetch_links=True)
cols = await afetch_beanie(query)
```

#### Projections

Beanie supports projections via `.project(ProjectionModel)` (upstream: [Finding documents → Projections](https://beanie-odm.dev/tutorial/finding-documents/#projections)).

PydanTable supports two projection styles:

- **`projection_model=MyProjectionModel`**: forwards to Beanie `.project(...)`
- **`fields=[...]`**: convenience helper that builds a temporary projection model

```python
cols = await afetch_beanie(MyDocument, fields=["id", "name"])
```

#### Relations / links

Beanie can prefetch links with `fetch_links=True` (upstream: [Relations](https://beanie-odm.dev/tutorial/relations/)).

```python
cols = await afetch_beanie(
    MyDocument,
    fetch_links=True,
    nesting_depth=2,
)
```

By default, pydantable **flattens** nested objects to dot-path columns (e.g.
`door.height`). You can turn flattening off with `flatten=False` (you’ll then get
nested values inside the column dict).

#### `_id` vs `id`

Beanie maps Mongo `_id` to `Document.id`. PydanTable normalizes this with:

- **`id_column="id"`** (default): outputs an `id` column
- **`id_column="_id"`**: outputs an `_id` column

### `aiter_beanie` (stream batches)

Use this for bounded-memory ingestion:

```python
from pydantable import aiter_beanie

async for batch in aiter_beanie(MyDocument, batch_size=10_000):
    # batch is dict[str, list]
    ...
```

### `awrite_beanie` (ODM-aware inserts)

Use this when you **want Beanie features to run** during inserts:

- `Settings.validate_on_save = True` (upstream: [On save validation](https://beanie-odm.dev/tutorial/on-save-validation/))
- event-based actions (`@before_event`, `@after_event`) (upstream: [Event-based actions](https://beanie-odm.dev/tutorial/event-based-actions/))

```python
from pydantable import BeanieWriteOptions, awrite_beanie

opts = BeanieWriteOptions(skip_actions=None, link_rule=None)
inserted = await awrite_beanie(MyDocument, {"x": [1, 2], "y": ["a", "b"]}, options=opts)
```

```{important}
`write_mongo` / `awrite_mongo` are driver-level `insert_many` helpers on a sync
PyMongo collection. They **do not** run Beanie validation-on-save or event hooks.
Use `awrite_beanie` when you need ODM semantics.
```

## Lazy execution over Mongo (Entei) without a sync client

If you want the **typed lazy DataFrame API** over a Beanie `Document` without
creating a sync PyMongo client, use the async-root constructors:

- `EnteiDataFrame[Schema].from_beanie_async(...)`
- `EnteiDataFrameModel.from_beanie_async(...)`

```python
from pydantable import EnteiDataFrame, Schema

class Row(Schema):
    id: str
    x: int

df = EnteiDataFrame[Row].from_beanie_async(MyDocument, criteria=MyDocument.x > 0)

# IMPORTANT: async-only materialization
rows = await df.acollect()
cols = await df.ato_dict()
```

```{warning}
`from_beanie_async(...)` is **async-only**. Calling sync terminals like `collect()`,
`to_dict()`, or sync lazy sinks will raise. Use `await acollect()` / `await ato_dict()`
instead.
```

## Migrations and schema evolution

Beanie has first-class migration tooling (upstream: [Migrations](https://beanie-odm.dev/tutorial/migrations/)).

PydanTable does not run migrations for you, but the recommended workflow is:

- keep Beanie `Document` classes as the schema source of truth
- run Beanie migrations when document shape changes
- keep your pydantable `Schema` / `DataFrameModel` types aligned with the *post-migration* shape

For engine-backed lazy execution details, see {doc}`MONGO_ENGINE`.

