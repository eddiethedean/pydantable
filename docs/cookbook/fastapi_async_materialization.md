# FastAPI: async materialization (`acollect`, `ato_dict`)

`pydantable` materialization is CPU-bound native work. In async routes, use the async
APIs which run execution in a worker thread.

## Recipe

```python
import asyncio

from pydantable import DataFrameModel


class User(DataFrameModel):
    id: int
    age: int | None


async def run() -> None:
    df = User({"id": [1, 2], "age": [20, None]})

    rows = await df.acollect()
    assert [r.model_dump() for r in rows] == [
        {"id": 1, "age": 20},
        {"id": 2, "age": None},
    ]

    cols = await df.ato_dict()
    assert cols == {"id": [1, 2], "age": [20, None]}


asyncio.run(run())
```

## Pitfalls

- **Cancellation**: cancelling the awaiting task does **not** cancel in-flight native work.
- **Thread pools**: you can pass a custom `executor=`; see {doc}`/FASTAPI` for patterns.

