# entei-core

Optional **MongoDB** execution helpers for [pydantable](https://github.com/eddiethedean/pydantable): **`EnteiPydantableEngine`** subclasses the native Polars engine, materializes **`MongoRoot`** (pymongo collections) to columnar dicts, then runs the existing Rust executor.

## Install

```bash
pip install entei-core
```

From this monorepo (after an editable `pydantable` install):

```bash
pip install -e ./entei-core
```

## Expressions and default engine

`Expr` / `filter` / `with_columns` use `pydantable.engine.get_expression_runtime()`, which requires the **process default** engine to be the native extension. Keep **`get_default_engine()`** as `NativePolarsEngine`, and pass **`engine=EnteiPydantableEngine()`** only on frames that use a **`MongoRoot`** or plain columnar data.

## Quick example

```python
import mongomock  # or pymongo.MongoClient(...)
from pydantable import Schema

from entei_core import EnteiDataFrame, MongoRoot


class Row(Schema):
    x: int


client = mongomock.MongoClient()
coll = client.db.items
coll.insert_many([{"x": 1}, {"x": 2}])

df = EnteiDataFrame[Row].from_collection(coll)
assert df.collect(as_lists=True) == {"x": [1, 2]}
```

## Versioning

Track **`pydantable`** / **`pydantable-protocol`** minor lines you test against (see pydantable’s VERSIONING docs).
