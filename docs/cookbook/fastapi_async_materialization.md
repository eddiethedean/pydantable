# FastAPI: async materialization (`collect`, `to_dict`)

`pydantable` materialization is CPU-bound native work. In async routes, use the async
APIs (**`await df.collect()`**, **`await df.to_dict()`**) which mirror sync **`collect`** /
**`to_dict`** but do not block the event loop. The **`a*`** names (**`acollect`**, **`ato_dict`**,
…) remain available and behave the same.

For **lazy file reads** chained before materialization, see {doc}`/cookbook/async_lazy_pipeline`.

## Recipe

```python
import asyncio

from pydantable import DataFrameModel


class User(DataFrameModel):
    id: int
    age: int | None


async def run() -> None:
    df = User({"id": [1, 2], "age": [20, None]})

    rows = await df.collect()
    assert [r.model_dump() for r in rows] == [
        {"id": 1, "age": 20},
        {"id": 2, "age": None},
    ]

    cols = await df.to_dict()
    assert cols == {"id": [1, 2], "age": [20, None]}


asyncio.run(run())
```

**Also valid:** **`await df.acollect()`** / **`await df.ato_dict()`** — identical semantics.

## Pitfalls

- **Cancellation**: cancelling the awaiting task does **not** cancel in-flight native work.
- **Thread pools**: you can pass a custom `executor=`; see {doc}`/FASTAPI` for patterns.
