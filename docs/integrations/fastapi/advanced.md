# FastAPI (advanced)

This page contains **less common** FastAPI integration topics: deeper async/I/O patterns,
experimental URL transports, and “how the pieces fit” when you’re building larger systems.

If you’re looking for the common path, start with [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path/), then the
short index + reference tables in [FASTAPI](/integrations/fastapi/fastapi/).

## Four materialization modes (FastAPI)

The same lazy plan can be materialized in **four** ways; see [MATERIALIZATION](/user-guide/materialization/) for the full table and **`PlanMaterialization`**.

Below, routes read **Parquet** from a **server-local path** (shared volume, artifact from an upstream job, or a temp file you wrote after **`await upload.read()`**). In production, **validate and sandbox** paths (allowlist directories, reject `..`, etc.). **`trusted_mode="shape_only"`** matches typical “file already matches our schema” pipelines; use default **`trusted_mode`** when you need full cell validation.

Row-list JSON bodies are covered in [FASTAPI](/integrations/fastapi/fastapi/) and [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies/); **async file routes** should **`await MyModel.aread_*`** (lazy scan, blocking open/read off the event loop) rather than **`await amaterialize_*`**, which builds a full **`dict[str, list]`** first. SQL: **`await afetch_sqlmodel`** / **`await afetch_sql_raw`** as needed ([IO_SQL](/io/sql/)).

### 1. Blocking — sync `def` + lazy `read_parquet` + `collect()` / `to_dict()`

**Sync** **`read_parquet`** keeps work on a Polars **`LazyFrame`** until **`collect()`** / **`to_dict()`** (see [EXECUTION](/user-guide/execution/)).

```python
from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel

app = FastAPI()


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@app.get("/users-blocking", response_model=list[UserRow])
def report_from_parquet_blocking(path: str = Query(..., description="Readable Parquet path on server")):
    df = UserDF.read_parquet(path, trusted_mode="shape_only").select("id", "age")
    return df.collect()


@app.get("/users-columnar-blocking")
def columnar_from_parquet_blocking(path: str = Query(...)):
    df = UserDF.read_parquet(path, trusted_mode="shape_only")
    return df.to_dict()
```

### 2. Async — `async def` + `await collect()` / `await to_dict()` (or `acollect` / `ato_dict`)

**`aread_*`** (or **`UserDF.Async.read_parquet`**, …) returns **`AwaitableDataFrameModel`**: chain lazy transforms (**`select`**, **`filter`**, …) and use **one** leading **`await`** on **`collect()`** / **`to_dict()`** — unprefixed aliases of **`acollect()`** / **`ato_dict()`**.

```python
from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel

app = FastAPI()


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@app.get("/users-async", response_model=list[UserRow])
async def report_from_parquet(path: str = Query(...)):
    return await UserDF.Async.read_parquet(path, trusted_mode="shape_only").select(
        "id", "age"
    ).collect()


@app.get("/users-columnar-async")
async def columnar_async(path: str = Query(...)):
    return await UserDF.Async.read_parquet(path, trusted_mode="shape_only").to_dict()
```

### 3. Deferred — `submit()` + `await handle.result()`

```python
import asyncio

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel

app = FastAPI()


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@app.get("/users-deferred", response_model=list[UserRow])
async def report_deferred(path: str = Query(...)):
    df = await UserDF.aread_parquet(path, trusted_mode="shape_only")
    handle = df.select("id", "age").submit()
    return await handle.result()


@app.get("/users-two-deferred")
async def two_cohorts_deferred(path_a: str = Query(...), path_b: str = Query(...)):
    df_a, df_b = await asyncio.gather(
        UserDF.aread_parquet(path_a, trusted_mode="shape_only"),
        UserDF.aread_parquet(path_b, trusted_mode="shape_only"),
    )
    h_a = df_a.select("id", "age").submit()
    h_b = df_b.select("id", "age").submit()
    out_a, out_b = await asyncio.gather(h_a.result(), h_b.result())
    return {"cohort_a": out_a, "cohort_b": out_b}
```

### 4. Chunked — `stream()` / `astream()` + streaming body

```python
import json

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse

from pydantable import DataFrameModel

app = FastAPI()


class UserDF(DataFrameModel):
    id: int
    age: int | None


def ndjson_sync(df):
    for batch in df.stream(batch_size=1_000):
        yield (json.dumps(batch, default=str) + "\n").encode()


@app.get("/users-stream-sync")
def users_stream_sync(path: str = Query(...)):
    df = UserDF.read_parquet(path, trusted_mode="shape_only").select("id", "age")
    return StreamingResponse(ndjson_sync(df), media_type="application/x-ndjson")


async def ndjson_async(df):
    async for batch in df.astream(batch_size=1_000):
        yield (json.dumps(batch, default=str) + "\n").encode()


@app.get("/users-stream-async")
async def users_stream_async(path: str = Query(...)):
    df = await UserDF.aread_parquet(path, trusted_mode="shape_only")
    df = df.select("id", "age")
    return StreamingResponse(ndjson_async(df), media_type="application/x-ndjson")
```

These are **chunked replay** responses, not out-of-core Polars streaming; very large tables may need pagination or writing to object storage instead ([EXECUTION](/user-guide/execution/)).

## `DataFrameModel` I/O in `async def` routes

Prefer **`await MyModel.aread_*`**, **`await afetch_sqlmodel`** / **`await afetch_sql_raw`** when you need SQL (**``from pydantable import …``**), **`await MyModel.aexport_*`**, and **`await MyModel.awrite_sql`** / **`await MyModel.awrite_sqlmodel`**.

Install what you need:

```bash
pip install "pydantable[io]"
pip install "pydantable[sql]"     # plus a DBAPI driver
pip install "pydantable[cloud]"   # fsspec backends (experimental)
pip install "pydantable[rap]"     # rapcsv + rapfiles (optional)
```

## Experimental HTTP(S) and object-store URLs

HTTP(S) helpers download with stdlib `urllib`, then parse (**experimental**): set **`PYDANTABLE_IO_EXPERIMENTAL=1`** or pass **`experimental=True`** to URL helpers. Do not fetch untrusted URLs without size limits and timeouts at your gateway.

Object-store URIs (`s3://`, `gs://`, `file://`, …) use `fsspec` (**`pydantable[cloud]`**) and are also experimental.

## Optional: true-async CSV with `aread_csv_rap`

`amaterialize_csv` uses `asyncio.to_thread` around sync/Rust paths. `aread_csv_rap` (install `pydantable[rap]`) uses rapcsv + rapfiles for async file reads without that thread offload.

