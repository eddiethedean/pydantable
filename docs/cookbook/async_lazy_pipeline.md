# Async lazy pipeline (`Async.read_*` → `await collect()`)

End-to-end pattern for **`async def`** code: **lazy scan** → **transforms** → **one await** on a terminal async method. Prefer **`MyModel.Async.read_parquet`** (same as **`aread_parquet`**) and **`await …collect()`** / **`to_dict()`** (aliases of **`acollect`** / **`ato_dict`**) so names stay async-first. See {doc}`/DATAFRAMEMODEL` **Three layers** and {doc}`/EXECUTION` for costs and threading.

## Minimal example

```python
import asyncio

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


async def load_rows(path: str):
    adf = await UserDF.Async.read_parquet(path, trusted_mode="shape_only")
    df = adf.select_as(UserDF, adf.col.id, adf.col.age)
    return await df.acollect()


async def load_columnar(path: str):
    return await UserDF.Async.read_parquet(path, trusted_mode="shape_only").to_dict()


asyncio.run(load_rows("/path/to/file.parquet"))
```

## Concurrent reads

Use **`asyncio.gather`** on multiple **`AwaitableDataFrameModel`** instances (each **`aread_*`** / **`Async.read_*`** returns a pending chain):

```python
async def load_two(path_a: str, path_b: str):
    a, b = await asyncio.gather(
        UserDF.Async.read_parquet(path_a, trusted_mode="shape_only"),
        UserDF.Async.read_parquet(path_b, trusted_mode="shape_only"),
    )
    # a, b are concrete UserDF; further transforms use sync collect or acollect.
    return a, b
```

## Executor

Pass **`executor=`** to **`Async.read_*`** / **`aread_*`** (and to **`collect()`** / **`acollect()`** on a concrete model) to pin blocking file setup and engine work to a shared **`ThreadPoolExecutor`** — see {doc}`/FASTAPI` and {doc}`/MATERIALIZATION`.

## Lazy metadata caveat

**`await adf.columns`**, **`shape`**, **`empty`**, **`dtypes`** on a **pending** chain do not materialize row data; for file-backed scans **`shape`** may show **zero rows** until **`collect()`**. See the warning in {doc}`/DATAFRAMEMODEL` **Three layers**.

## Related

- {doc}`/FASTAPI` — routes, **`StreamingResponse`**, uploads.
- {doc}`/cookbook/fastapi_async_materialization` — **`collect`** / **`acollect`** on a concrete model.
- {doc}`/MATERIALIZATION` — four terminal modes.
