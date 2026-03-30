# FastAPI Integration Guide

This guide is the **navigation + reference** for using pydantable inside FastAPI:
validated request bodies, typed transforms, async materialization, and streaming.

If you want the shortest runnable service first, start with {doc}`GOLDEN_PATH_FASTAPI`.

**Start here:** {doc}`GOLDEN_PATH_FASTAPI` (one runnable async app: lifespan, `Depends`, `acollect`, streaming).

**Related recipes:** {doc}`/cookbook/fastapi_columnar_bodies` (column-shaped JSON bodies), {doc}`/cookbook/fastapi_async_materialization`, {doc}`/cookbook/fastapi_observability` (request IDs + **`observe`**), {doc}`/cookbook/fastapi_background_tasks` (**`BackgroundTasks`** + **`submit`**), {doc}`/cookbook/async_lazy_pipeline` (lazy `aread_*` → transforms → materialize). Example **service layout** (routers + lifespan): `docs/examples/fastapi/service_layout/` in the repo. **Roadmap / “when to use what”:** {doc}`/FASTAPI_ENHANCEMENTS`.

## How to read this page (quick map)

- If you’re building a service **today**, read:
  - {ref}`fastapi-install`
  - {ref}`fastapi-fast-path` (the “golden path” + cookbooks)
  - {ref}`fastapi-errors` (422 vs 400 vs 503)
  - {ref}`fastapi-testing` (lifespan-aware `TestClient`)
- If you’re deciding **row vs column** payloads, jump to:
  - {ref}`columnar-openapi-fastapi` (OpenAPI columnar models + `Depends`)
  - {ref}`column-shaped-json-request-bodies` (columnar bodies without helpers)
- If you’re tuning **async / executors / streaming**, jump to:
  - {ref}`four-materialization-modes-fastapi`
  - {doc}`EXECUTION` and {doc}`MATERIALIZATION` (deep dive)

(fastapi-install)=
## Install (what to `pip install`)

```bash
pip install pydantable
```

Optional helpers used throughout the FastAPI docs:

```bash
pip install "pydantable[fastapi]"
```

For I/O-heavy service routes (Arrow buffers, Parquet/IPC helpers, streaming writers), you’ll often also want:

```bash
pip install "pydantable[io]"
```

(fastapi-fast-path)=
## Fast path for services (recommended order)

1. Run {doc}`GOLDEN_PATH_FASTAPI` end-to-end.
2. Pick your payload shape:
   - **Row list**: `list[YourDF.RowModel]` in requests + `response_model=list[YourDTO]` in responses.
   - **Columnar JSON**: `dict[str, list]` shapes; see {doc}`/cookbook/fastapi_columnar_bodies`.
3. Decide response size:
   - Small/medium: `collect()` / `to_dict()`
   - Large: `astream()` + `ndjson_streaming_response` ({doc}`FASTAPI_ENHANCEMENTS`)
4. Lock down your error mapping: {ref}`fastapi-errors`.

## Optional `pydantable.fastapi` helpers

Install the extra:

```bash
pip install "pydantable[fastapi]"
```

Then import `pydantable.fastapi` (not required for basic FastAPI usage):

- **`executor_lifespan(app, max_workers=..., thread_name_prefix=...)`** — async context manager that attaches a `ThreadPoolExecutor` to **`app.state.executor`** for **`acollect(executor=...)`** and **`pydantable.io`** helpers.
- **`get_executor(request)`** — for **`Depends(get_executor)`**, returning **`request.app.state.executor`** (or **`None`** if unset).
- **`register_exception_handlers(app)`** — registers HTTP handlers for **`MissingRustExtensionError`** (**503**), **`ColumnLengthMismatchError`** (**400**), and in-handler **`pydantic.ValidationError`** (**422**); see {ref}`fastapi-errors`.
- **`ndjson_streaming_response(astream_iter)`** / **`ndjson_chunk_bytes(astream_iter)`** — build **`application/x-ndjson`** **`StreamingResponse`** from **`await df.astream(...)`** without duplicating JSON line encoding; see {doc}`/FASTAPI_ENHANCEMENTS`.
- **`columnar_body_model`**, **`columnar_body_model_from_dataframe_model`** — build a Pydantic model whose fields are **`list[T]`** per column (OpenAPI-friendly **`dict[str, list]`**). Optional **`example=`** / **`json_schema_extra=`** for Swagger examples.
- **`columnar_dependency(model_cls, ...)`**, **`rows_dependency(model_cls, ...)`** — **`Depends(...)`** factories that validate the request body and return a **`DataFrameModel`** instance (columnar JSON or **`list[RowModel]`**), forwarding **`trusted_mode`** and related **`DataFrameModel`** kwargs.

Inbound request validation is still FastAPI’s default **`RequestValidationError`** (**422**) when the *request body* fails to parse.

(columnar-openapi-fastapi)=
## Columnar OpenAPI and `Depends`

Use **`columnar_body_model_from_dataframe_model(MyDF)`** as **`Body`** / **`response_model`** when clients send or receive column-shaped JSON (same shape as **`to_dict()`**). For routes, prefer **`columnar_dependency`** so you inject a **`MyDF`** directly:

```python
from typing import Annotated

from fastapi import Depends, FastAPI

from pydantable.fastapi import columnar_dependency

app = FastAPI()

@app.post("/ingest")
def ingest(df: Annotated[User, Depends(columnar_dependency(User, trusted_mode="strict"))]) -> dict:
    return df.to_dict()
```

For **row-array** JSON bodies, use **`rows_dependency(User)`**; OpenAPI documents **`list[User.RowModel]`**.

**Nested row fields** (e.g. **`inner: NestedModel`**) become **`list[NestedModel]`** in columnar JSON (one nested object per row index). That shape is valid but can be heavy on the wire; prefer flat columns when you can.

**Validation layers:** Pydantic validates each column as **`list[T]`** (wrong element types → **422** before your handler). **Row/column length consistency** is enforced when constructing **`DataFrameModel`** inside the dependency; mismatched lengths raise **`ColumnLengthMismatchError`** (subclass of **`ValueError`**). With **`register_exception_handlers`**, that maps to **400**; without it, you typically see **500**—see {ref}`fastapi-errors`.

**NDJSON** streaming responses do not get a per-chunk OpenAPI schema (same as any streaming body); columnar **`response_model`** applies to single JSON **`to_dict()`** responses only.

**Testing:** **`pydantable.testing.fastapi`** provides **`fastapi_app_with_executor()`** and **`fastapi_test_client(app)`** (context manager) so **`executor_lifespan`** runs under **`TestClient`** and **`get_executor`** works. Use **`TestClient(..., raise_server_exceptions=False)`** when asserting **500** responses from dependencies. See **`tests/test_pydantable_fastapi_columnar.py`**.

(fastapi-testing)=
### Testing note (lifespan and `TestClient`)

FastAPI’s `TestClient` is synchronous; if your app uses a lifespan function (including `executor_lifespan`), prefer `pydantable.testing.fastapi.fastapi_test_client(app)` so the lifespan runs and `Depends(get_executor)` works. See {doc}`FASTAPI_ENHANCEMENTS` (Phase 7).

(fastapi-errors)=
## HTTP errors and exception handlers

| Situation | Typical exception | HTTP status | Notes |
|-----------|-------------------|-------------|--------|
| Invalid JSON body shape / types at the boundary | `fastapi.exceptions.RequestValidationError` | **422** | FastAPI’s default handler (before your route runs). |
| Manual validation inside a route (e.g. `model_validate`) | `pydantic.ValidationError` | **422** | Use **`register_exception_handlers`** from **`pydantable.fastapi`**, or map yourself. |
| Native extension missing | `MissingRustExtensionError` | **503** | **`register_exception_handlers`** returns a JSON **`detail`** string. |
| Mismatched column lengths after Pydantic accepted the body | `ColumnLengthMismatchError` | **400** | From **`register_exception_handlers`**; JSON **`{"detail": "<message>"}`**. |
| Engine / plan / transform errors | Often `ValueError` | **400** / **422** / **500** | **Do not** map all `ValueError`s globally; **`register_exception_handlers`** only adds **`ColumnLengthMismatchError`**. |

## Why this matters

For FastAPI services, `pydantable` gives you:

- Pydantic validation at API boundaries (`RowModel` per `DataFrameModel`)
- typed dataframe transformations in handlers or services
- **`collect()`** — `list` of Pydantic models for the **current** projection (ideal for `response_model=list[YourRow]`)
- **`to_dict()`** — `dict[str, list]` when the response is **column-shaped** JSON
- **`acollect()`**, **`ato_dict()`**, **`ato_polars()`**, **`ato_arrow()`** — same semantics off the event loop (Rust/Tokio awaitable when available; see below)
- **`submit()`** + **`await handle.result()`** — background **`collect()`**
- **`stream()`** / **`astream()`** — sync / async iteration of **`dict[str, list]`** column chunks after one engine collect (for streaming HTTP bodies; see {doc}`EXECUTION`). Use **`stream()`** in **`def`** routes with **`StreamingResponse`**; use **`async for`** over **`astream()`** in **`async def`** routes.
- **`DataFrameModel`** / **`DataFrame[Schema]`** **classmethods** — lazy **`read_*`**, **`aread_*`**, **`export_*`**, **`write_sql`**, **`awrite_sql`**, and lazy **`write_*`** ({doc}`IO_OVERVIEW`). **Eager** column loads (**`materialize_*`**, **`fetch_sql`**, **`iter_sql`**, …) live on **`pydantable.io`**; pass **`dict[str, list]`** into **`MyModel(...)`** / **`DataFrame[Schema](...)`** for typed frames. **Parquet / CSV / NDJSON / IPC / JSON:** Rust-first on local paths where the wheel supports it; **PyArrow** for buffers and column subsets. **Out-of-core:** **`read_*`** + transforms + **`DataFrame.write_*`**. **HTTP Parquet:** **`read_parquet_url_ctx`** / **`aread_parquet_url_ctx`** ({doc}`IO_HTTP`). Extras: **`[sql]`**, **`[cloud]`**, **`[rap]`**. **`pip install 'pydantable[io]'`** pulls **PyArrow + Polars** for columnar paths.
- **`await amaterialize_*`** / **`await afetch_sql`** from **`pydantable.io`** — same threading model as other async I/O helpers (**`asyncio.to_thread`** / **`executor=`**).
- **`to_arrow()`** — materialize a PyArrow **`Table`** after the same engine path as **`to_dict()`** (not zero-copy; see [`EXECUTION.md`](EXECUTION.md))

**Synchronous materialization** (`collect()`, `to_dict()`, `collect(as_lists=True)`, optional `to_polars()`) runs **blocking** Rust + Polars work on the **current thread**.

**Asynchronous materialization (0.15.0+):** `await df.acollect()`, `await df.ato_dict()`, `await df.ato_polars()`, and ( **0.16.0+** ) `await df.ato_arrow()` prefer a Rust **`async_execute_plan`** coroutine when the wheel supports it; otherwise the same work runs in **`asyncio.to_thread`** or in a **`concurrent.futures.Executor`** (`executor=...`). Use this from **`async def`** route handlers so the ASGI event loop stays responsive. Cancelling the waiter does **not** cancel in-flight engine work; see [`EXECUTION.md`](EXECUTION.md) for limits (GIL, copies, `to_polars`, `to_arrow`).

**`DataFrameModel`** also exposes **`arows()`** and **`ato_dicts()`** as async counterparts to **`rows()`** / **`to_dicts()`**.

## Install

From PyPI (prebuilt wheels include the native extension on supported platforms):

```bash
pip install pydantable
```

For **`pydantable.fastapi`** helpers (lifespan executor, `Depends`, shared exception handlers):

```bash
pip install "pydantable[fastapi]"
```

From a git checkout, build the extension (for example with [Maturin](https://www.maturin.rs/)):

```bash
pip install .
```

(four-materialization-modes-fastapi)=
## Four materialization modes (FastAPI)

The same lazy plan can be materialized in **four** ways; see {doc}`MATERIALIZATION` for the full table and **`PlanMaterialization`**.

Below, routes read **Parquet** from a **server-local path** (shared volume, artifact from an upstream job, or a temp file you wrote after **`await upload.read()`**). In production, **validate and sandbox** paths (allowlist directories, reject `..`, etc.). **`trusted_mode="shape_only"`** matches typical “file already matches our schema” pipelines; use default **`trusted_mode`** when you need full cell validation.

Row-list JSON bodies are covered in [Column-shaped JSON request bodies](#column-shaped-json-request-bodies) below; **async file routes** should **`await MyModel.aread_*`** (lazy scan, blocking open/read off the event loop) rather than **`await amaterialize_*`**, which builds a full **`dict[str, list]`** first. SQL: **`await afetch_sql`** / **`aiter_sql`** from **`pydantable.io`** ({doc}`IO_OVERVIEW`).

### 1. Blocking — sync `def` + `collect()` / `to_dict()`

**Sync** **`pydantable.io.materialize_parquet`** blocks the worker thread while Rust/PyArrow read the file; **`UserDF(cols)`** then **`collect()`** / **`to_dict()`** run the lazy plan (often trivial if the “plan” is just the scan root).

```python
from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel
from pydantable.io import materialize_parquet

app = FastAPI()


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@app.get("/users-blocking", response_model=list[UserRow])
def report_from_parquet_blocking(path: str = Query(..., description="Readable Parquet path on server")):
    df = UserDF(materialize_parquet(path), trusted_mode="shape_only").select("id", "age")
    return df.collect()


@app.get("/users-columnar-blocking")
def columnar_from_parquet_blocking(path: str = Query(...)):
    df = UserDF(materialize_parquet(path), trusted_mode="shape_only")
    return df.to_dict()
```

### 2. Async — `async def` + `await collect()` / `await to_dict()` (or `acollect` / `ato_dict`)

**`aread_*`** (or **`UserDF.Async.read_parquet`**, …) returns **`AwaitableDataFrameModel`**: chain lazy transforms (**`select`**, **`filter`**, …) and use **one** leading **`await`** on **`collect()`** / **`to_dict()`** — unprefixed aliases of **`acollect()`** / **`ato_dict()`** — (e.g. **`return await UserDF.Async.read_parquet(...).select(...).collect()`**). Sync lazy **`read_parquet`** stays on **`DataFrameModel`**; the **`Async`** namespace holds the async readers so names match without an **`a`** prefix. File open + scan setup run in **`asyncio.to_thread`** (or **`executor=`**); the plan stays lazy until terminal materialization. You can still **`df = await UserDF.aread_parquet(...)`** for a concrete model in two steps. For eager columns first, **`await amaterialize_parquet(path)`** from **`pydantable.io`** + **`UserDF(cols)`** ({doc}`IO_OVERVIEW`).

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

(chained-await-fastapi)=
#### Nested `await` on the read (optional)

If you prefer the older style, **`await (await UserDF.aread_parquet(...)).select(...).acollect()`** still works: the inner **`await`** resolves the **`AwaitableDataFrameModel`** to a concrete model (same as assigning **`df = await UserDF.aread_parquet(...)`**). Parentheses around the inner **`await`** are required because **`await`** binds less tightly than method calls.

### 3. Deferred — `submit()` + `await handle.result()`

**`asyncio.gather`** loads two Parquet paths concurrently (**`await UserDF.aread_parquet`**), then **`submit()`** overlaps **`collect()`** work (still useful when CPU-heavy transforms follow the scan).

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
async def two_cohorts_deferred(
    path_a: str = Query(...),
    path_b: str = Query(...),
):
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

Lazy **`read_parquet`** / **`aread_parquet`** build the same **`DataFrame`** you **`stream`** or **`astream`** after one engine collect. Install **`pydantable[polars]`**. **NDJSON**-style response: one JSON object per batch.

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
    return StreamingResponse(
        ndjson_sync(df),
        media_type="application/x-ndjson",
    )


async def ndjson_async(df):
    async for batch in df.astream(batch_size=1_000):
        yield (json.dumps(batch, default=str) + "\n").encode()


@app.get("/users-stream-async")
async def users_stream_async(path: str = Query(...)):
    df = await UserDF.aread_parquet(path, trusted_mode="shape_only")
    df = df.select("id", "age")
    return StreamingResponse(
        ndjson_async(df),
        media_type="application/x-ndjson",
    )
```

These are **chunked replay** responses, not out-of-core Polars streaming; very large tables may need pagination or writing to object storage instead ({doc}`EXECUTION`).

## Trusted ingest (`trusted_mode`)

For **`DataFrameModel(...)`** and **`DataFrame[Schema](...)`**, ingestion defaults to full per-cell validation (`trusted_mode="off"`). For **trusted** bulk paths (pre-validated upstream data, internal services), use:

| Mode | Meaning |
|------|---------|
| **`trusted_mode="off"`** | Default: full Pydantic validation per cell (same as omitting the argument). |
| **`trusted_mode="shape_only"`** | Skip element validation; still checks column names and row counts. May emit **`DtypeDriftWarning`** when payloads would fail **`strict`** (see {doc}`SUPPORTED_TYPES`). |
| **`trusted_mode="strict"`** | Trusted bulk input plus dtype / nested-shape checks against the schema (including Polars columns). |

Details: [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md), [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md).

```python
from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


# Trusted path: caller guarantees rows already match RowModel (e.g. validated earlier in the pipeline).
df = UserDF(
    [{"id": 1, "age": 20}, {"id": 2, "age": None}],
    trusted_mode="shape_only",
)
```

(column-shaped-json-request-bodies)=
## Column-shaped JSON request bodies

Row lists are natural for OpenAPI (`list[YourRowModel]`). Some clients send **columnar** JSON (`parallel arrays`). Model that with a Pydantic body whose fields are lists, then pass a **`dict[str, list]`** into **`DataFrameModel`**:

```python
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UsersColumnarBody(BaseModel):
    """OpenAPI-friendly columnar payload: keys match dataframe columns."""

    id: list[int]
    age: list[int | None]


# In a route: UsersColumnarBody validated by FastAPI, then:
body = UsersColumnarBody(id=[1, 2], age=[20, None])
df = UserDF({"id": body.id, "age": body.age})
```

Same schema rules apply as for columnar constructors in [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md) (equal-length columns, types per field).

## Parquet and Arrow IPC uploads (multipart)

For **file** bodies, read bytes and use **`pydantable.io.materialize_parquet`** / **`materialize_ipc`** (**bytes** need **PyArrow** — **`pip install 'pydantable[arrow]'`** or **`[io]`**). That path is **blocking** while PyArrow decodes. In **`async def`** handlers, prefer writing to a temp path and **`await MyModel.aread_parquet(path)`** so the blocking work is offloaded like other async readers ({doc}`IO_HTTP`). FastAPI file routes require **`python-multipart`** (`pip install python-multipart`).

Use **`trusted_mode="shape_only"`** or **`strict`** for internal uploads where the file is already schema-shaped; use default validation for untrusted clients.

```python
from fastapi import FastAPI, UploadFile

from pydantable import DataFrameModel
from pydantable.io import materialize_parquet


class UserDF(DataFrameModel):
    id: int
    age: int | None


app = FastAPI()


@app.post("/upload-parquet")
async def upload_parquet(file: UploadFile):
    raw = await file.read()
    df = UserDF(materialize_parquet(raw), trusted_mode="shape_only")
    return df.to_dict()
```

## `DataFrameModel` I/O in `async def` routes

Prefer **`await MyModel.aread_*`**, **`await amaterialize_*`** / **`await afetch_sql`** from **`pydantable.io`** (then **`MyModel(cols)`**), **`await MyModel.aexport_*`**, and **`await MyModel.awrite_sql`**. **`pydantable.io`** **`materialize_*`** / **`fetch_sql`** return **`dict[str, list]`** — wrap with **`MyModel(...)`** when you want a typed frame.

**Install** what you need:

```bash
pip install 'pydantable[io]'      # PyArrow + Polars for columnar paths / fallbacks
pip install 'pydantable[sql]'     # SQLAlchemy only; add your DB driver (psycopg, pymysql, …)
pip install 'pydantable[cloud]'   # fsspec for s3://, gs://, file://, … (experimental)
pip install 'pydantable[rap]'     # optional: aread_csv_rap (rapcsv + rapfiles)
```

| Sync | Async | Typical use in a route |
|------|-------|-------------------------|
| **`MyModel.read_parquet`** | **`await MyModel.Async.read_parquet`** (or **`aread_parquet`**) | Large local Parquet without full Python dict |
| **`materialize_parquet`** | **`await amaterialize_parquet`** | Eager columns from path or bytes (**`pydantable.io`**) → **`MyModel(cols)`** |
| **`materialize_ipc`** | **`await amaterialize_ipc`** | Arrow IPC / Feather file (**`pydantable.io`**) |
| **`materialize_csv`** | **`await amaterialize_csv`** | CSV path (**`pydantable.io`**) |
| **`materialize_ndjson`** | **`await amaterialize_ndjson`** | NDJSON / JSON-lines file (**`pydantable.io`**) |
| **`MyModel.export_parquet`** | **`await MyModel.aexport_parquet`** | Persist to Parquet |
| **`MyModel.export_*`** | **`await MyModel.aexport_*`** | Other formats |
| **`fetch_sql`** | **`await afetch_sql`** | Parameterized **`SELECT`** → **`dict[str, list]`** → **`MyModel(cols)`** |
| **`MyModel.write_sql`** | **`await MyModel.Async.write_sql`** (or **`awrite_sql`**) | Append or replace table from column dict |

**`DataFrame.write_parquet`** (and siblings) are synchronous (Rust); call them from a thread pool inside **`async def`** if the write can be large. All **`amaterialize_*`** / **`aread_*`** / **`afetch_sql`** / **`aexport_*`** / **`awrite_sql`** accept **`executor=`** (same semantics as **`df.acollect(executor=…)`**). Reuse one bounded **`ThreadPoolExecutor`** from **`app.state`** or **`Depends`** for dataframe materialization **and** file/SQL I/O so load is predictable under concurrency.

### Example: read a Parquet path without blocking the loop

```python
from fastapi import FastAPI

from pydantable import DataFrameModel


class RowDF(DataFrameModel):
    id: int
    name: str


app = FastAPI()


@app.get("/from-parquet-path")
async def from_parquet_path(path: str):
    return await RowDF.aread_parquet(path, trusted_mode="shape_only").ato_dict()
```

### Example: upload → temp file → async read

For large uploads, avoid holding **`bytes`** longer than necessary: write to a temp path, **`await MyModel.aread_parquet(path)`** (lazy scan), then delete. Same pattern works for IPC/CSV/NDJSON with **`aread_ipc`**, **`aread_csv`**, etc.

```python
import tempfile
from pathlib import Path

from fastapi import FastAPI, UploadFile

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


app = FastAPI()


@app.post("/upload-parquet-async")
async def upload_parquet_async(file: UploadFile):
    suffix = Path(file.filename or "upload").suffix or ".parquet"
    data = await file.read()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data)
        path = tmp.name
    try:
        return await (await UserDF.aread_parquet(path, trusted_mode="shape_only")).ato_dict()
    finally:
        Path(path).unlink(missing_ok=True)
```

### Async SQL (`afetch_sql` / `awrite_sql` / `aiter_sql`)

**SQLAlchemy 2.x** (**`pip install 'pydantable[sql]'`** plus your **DBAPI** driver): use **`await afetch_sql`** from **`pydantable.io`** and **`RowDF(cols, ...)`**; **`MyModel.write_sql`** / **`await MyModel.awrite_sql`** for sinks. **`aiter_sql`** yields **`dict[str, list]`** batches — wrap each with **`RowDF(batch, ...)`**. Keep **`SELECT`** parameterized; never build SQL from untrusted request fields without binds.

```python
from typing import Annotated, Any

from fastapi import Body

from pydantable import DataFrameModel
from pydantable.io import afetch_sql, aiter_sql


class RowDF(DataFrameModel):
    id: int
    name: str


@app.get("/rows")
async def rows(database_url: str):
    cols = await afetch_sql(
        "SELECT id, name FROM t WHERE active = :a",
        database_url,
        parameters={"a": True},
    )
    df = RowDF(cols, trusted_mode="shape_only")
    return await df.ato_dict()


@app.get("/rows-chained")
async def rows_chained(database_url: str):
    return await RowDF(
        await afetch_sql(
            "SELECT id, name FROM t WHERE active = :a",
            database_url,
            parameters={"a": True},
        ),
        trusted_mode="shape_only",
    ).ato_dict()


@app.get("/rows-streaming")
async def rows_streaming(database_url: str):
    # Stream batches to keep peak memory bounded.
    out: list[RowDF] = []
    async for batch_cols in aiter_sql(
        "SELECT id, name FROM t WHERE active = :a",
        database_url,
        parameters={"a": True},
        batch_size=10_000,
    ):
        out.append(RowDF(batch_cols, trusted_mode="shape_only"))
    return {"batches": len(out)}


@app.post("/bulk-insert")
async def bulk_insert(
    database_url: str,
    cols: Annotated[dict[str, list[Any]], Body(...)],
):
    await RowDF.awrite_sql(cols, "staging_events", database_url, if_exists="append")
    return {"ok": True}
```

### Experimental HTTP(S) and object-store URLs

**HTTP(S)** helpers download with stdlib **`urllib`**, then parse (**experimental**): set **`PYDANTABLE_IO_EXPERIMENTAL=1`** or pass **`experimental=True`** to **`fetch_parquet_url`**, **`fetch_csv_url`**, **`fetch_ndjson_url`**, or **`fetch_bytes`**. Do not fetch untrusted URLs without size limits and timeouts at your gateway.

**Object-style URIs** (**`s3://`**, **`gs://`**, **`file://`**, …) use **`read_from_object_store`** with **`fsspec`** (**`pip install 'pydantable[cloud]'`** + backend drivers as needed). Same experimental flag / parameter as other URL helpers.

### Optional: true-async CSV with **`aread_csv_rap`**

**`amaterialize_csv`** uses **`asyncio.to_thread`** around sync/Rust paths. **`aread_csv_rap`** (**`pip install 'pydantable[rap]'`**) uses **rapcsv** + **rapfiles** for async file reads without that thread offload—useful if you standardize on the **`[rap]`** stack. From sync code only, **`materialize_csv(..., use_rap=True)`** delegates to **`asyncio.run(aread_csv_rap(...))`** when no event loop is running (see **`pydantable.io`** docstrings).

Tier-2/3 readers (**Excel**, **BigQuery**, **Snowflake**, **Kafka**, …) live under **`pydantable.io.extras`**; they are mostly **sync** SDK calls—wrap them in **`asyncio.to_thread`** (or your shared **`executor=`**) from **`async def`** routes the same way. Planning reference: [`DATA_IO_SOURCES.md`](DATA_IO_SOURCES.md).

## Injectable executor with `Depends`

Besides **`lifespan`** + **`app.state`**, you can inject a **`ThreadPoolExecutor`** (or **`None`**) via **`Depends`** so tests and routes share one pattern:

```python
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated

from fastapi import Depends, FastAPI

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pydantable")


def get_df_executor() -> ThreadPoolExecutor:
    return _executor


app = FastAPI()


@app.post("/users-async")
async def create_users_async(
    rows: list[UserDF.RowModel],
    executor: Annotated[ThreadPoolExecutor, Depends(get_df_executor)],
):
    df = UserDF(rows)
    return await df.acollect(executor=executor)
```

Pass **`executor=None`** (omit **`Depends`**) to keep **`asyncio.to_thread`** as in the [Async routes](#async-routes-executors-and-lifespan) example.

Use the **same** injected executor for **`await MyModel.aread_parquet(..., executor=ex)`** / **`await afetch_sql(..., executor=ex)`** when you want file and SQL I/O to share the pool with **`await df.acollect(executor=ex)`**.

**When to use the default thread pool vs a shared `ThreadPoolExecutor`:** The default (**`executor=None`**) uses **`asyncio.to_thread`**, which schedules work on the interpreter’s default executor—fine for light or sporadic I/O. Prefer a **dedicated bounded** **`ThreadPoolExecutor`** (injected via **`Depends`** or **`app.state`**) when you need predictable concurrency limits, shared naming for observability, coordinated shutdown in **`lifespan`**, or to avoid competing with other libraries for the default pool under load. Neither choice makes Rust execution “more async”; both offload blocking work from the event loop.

## Background tasks

Use Starlette **`BackgroundTasks`** for work that must run **after** the response is sent (e.g. logging metrics, cache warming). Background code **cannot** change the HTTP body; exceptions should be logged or handled inside the task— they do **not** become **`500`** responses. For dataframe work that must complete before the client receives data, keep **`await df.ato_dict()`** (or similar) in the handler instead.

## Validation errors and HTTP status codes

- **FastAPI / Pydantic** validate route parameters and body models first. Type mismatches on **`list[YourRowModel]`** typically produce **`422 Unprocessable Entity`** with a structured error body.
- **Application logic** after validation: map expected domain failures to **`HTTPException(status_code=..., detail=...)`** (e.g. **`404`**, **`409`**). Uncaught **`ValueError`** / **`TypeError`** from **`DataFrameModel(...)`** (unknown columns, length mismatch, **`TypeError`** when **`trusted_mode`** is wrong for Polars) become **`500`** unless you catch them and translate.

Recommend validating **untrusted** JSON with default **`trusted_mode`**; reserve **`shape_only`** / **`strict`** for authenticated internal pipelines or files you control.

(async-routes-executors-and-lifespan)=

## Async routes, executors, and lifespan

For **`async def`** handlers, **`await`** the async materialization helpers instead of calling **`collect()`** / **`to_dict()`** directly (unless you intentionally block the loop).

```python
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Bounded pool for dataframe materialization (optional; default is asyncio’s thread pool).
    executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="pydantable")
    app.state.df_executor = executor
    yield
    executor.shutdown(wait=True)


app = FastAPI(lifespan=lifespan)


@app.post("/users-async", response_model=list[UserRow])
async def create_users_async(rows: list[UserDF.RowModel]):
    df = UserDF(rows)
    ex = app.state.df_executor
    return await df.acollect(executor=ex)
```

Without a custom executor, **`await df.acollect()`** is enough: pydantable uses **`asyncio.to_thread`**.

### Chunked column dicts (`stream` / `astream`)

Full **`StreamingResponse`** examples (sync **`def`** + **`stream`**, **`async def`** + **`astream`**) are in [**Four materialization modes**](#four-materialization-modes-fastapi) above.

**`DataFrame.stream()`** and **`DataFrame.astream()`** yield **`dict[str, list]`** batches after **one** engine collect (same contract as **`collect_batches`**; see {doc}`EXECUTION`). They do **not** avoid holding the full materialized result in memory before chunking—use **pagination** or **external storage** when the table is too large for one collect. If you need **one** blob first, **`await df.ato_dict()`** / **`await df.arows()`** and then build your own response shape is still valid.

## Large tables, Polars, Arrow, and trust boundaries

**Default (`trusted_mode="off"`)** is the right choice for **public** or **untrusted** HTTP bodies: every cell is validated against your `RowModel` types before any Rust work runs. Use it when clients can send arbitrary JSON.

**When `trusted_mode` is appropriate in routes**

| Situation | Suggested mode | Notes |
|-----------|----------------|--------|
| Internal service-to-service batch (same org, authN/Z at gateway) | `shape_only` or `strict` | You still enforce column names and row counts; `strict` adds dtype / nested-shape checks for Polars and columnar buffers. |
| Upstream already validated rows (e.g. warehouse export, replay from your own DB) | `shape_only` | Fastest path; assumes wire format matches schema. |
| Polars `DataFrame` or NumPy / **PyArrow** columns built **inside** your stack | `strict` | Checks Polars dtypes (and Python column buffers where implemented) against annotations; see [`SUPPORTED_TYPES.md`](SUPPORTED_TYPES.md) (“Runtime column payloads”). |

**Who may skip full `RowModel` validation**

- Only code paths where **mis-typed data cannot reach the constructor** without a deliberate privilege break (private workers, ETL you own, or payloads already validated by Pydantic at an earlier hop).
- **Do not** attach `trusted_mode="shape_only"` / `strict` directly to a public upload endpoint that accepts raw user JSON unless another layer has already validated every cell.

**Polars and Arrow in handlers**

- Passing a **Polars `DataFrame`** requires trusted mode (`shape_only` or `strict`); see [`DATAFRAMEMODEL.md`](DATAFRAMEMODEL.md).
- **`strict`** rejects Polars columns whose dtypes do not match the schema (including nested list / struct / map shapes). Prefer **`strict`** when the frame comes from Arrow/Parquet/IPC and you want a safety net without per-cell Pydantic.
- For performance characteristics (validation vs ingest vs `collect()`), see [`PERFORMANCE.md`](PERFORMANCE.md) and [`EXECUTION.md`](EXECUTION.md).

## Example 1: Router + multi-table body — revenue by country

Real services often receive **more than one related table** (partner feed, staged
upload, or denormalized batch). Validate each as `list[...RowModel]`, build two
`DataFrameModel` instances, then **join → fill nulls → aggregate**. Return
**`collect()`**; FastAPI applies **`response_model`** to validate and serialize
the response (no need to wrap rows in `model_validate` yourself).

```python
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from pydantable import DataFrameModel

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


class OrderLineDF(DataFrameModel):
    order_id: int
    user_id: int
    amount: float | None


class UserDimDF(DataFrameModel):
    user_id: int
    country: str


class SalesByCountryBody(BaseModel):
    """Two datasets in one JSON payload (common for bulk ingest APIs)."""

    orders: list[OrderLineDF.RowModel]
    users: list[UserDimDF.RowModel]


class CountryRevenueRow(BaseModel):
    country: str
    total: float
    n_orders: int


app = FastAPI()
app.include_router(router)


@router.post("/sales-by-country", response_model=list[CountryRevenueRow])
def sales_by_country(body: SalesByCountryBody):
    orders = OrderLineDF(body.orders)
    users = UserDimDF(body.users)
    rolled = (
        orders.join(users, on="user_id", how="left")
        .fill_null(0.0, subset=["amount"])
        .group_by("country")
        .agg(total=("sum", "amount"), n_orders=("count", "order_id"))
        .sort("country")
    )
    return rolled.collect()
```

Example request (abbreviated):

```json
{
  "orders": [
    {"order_id": 1, "user_id": 10, "amount": 50.0},
    {"order_id": 2, "user_id": 10, "amount": null},
    {"order_id": 3, "user_id": 20, "amount": 20.0}
  ],
  "users": [
    {"user_id": 10, "country": "US"},
    {"user_id": 20, "country": "CA"}
  ]
}
```

Example response (sorted for readability):

```json
[
  {"country": "CA", "total": 20.0, "n_orders": 1},
  {"country": "US", "total": 50.0, "n_orders": 2}
]
```

## Example 2: Query parameters + `collect()` — ranked adults with a cap

Use **`Query`** for bounds that belong on the URL (versioning, caching, client
SDKs). Chain **`filter` → `sort` → `head`** then **`collect()`** for the
response body.

```python
from typing import Annotated

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class AdultRow(BaseModel):
    id: int
    age: int | None


app = FastAPI()


@app.post("/users/adults", response_model=list[AdultRow])
def adults(
    rows: list[UserDF.RowModel],
    min_age: Annotated[int, Query(ge=0, le=120)] = 18,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    df = UserDF(rows)
    ranked = df.filter(df.age >= min_age).sort("age", descending=True).head(limit)
    return ranked.collect()
```

With body `[{"id": 1, "age": 22}, {"id": 2, "age": null}, {"id": 3, "age": 15}, {"id": 4, "age": 30}]` and default query params, the handler returns adults sorted by `age` descending, at most `limit` rows — here `[{"id": 4, "age": 30}, {"id": 1, "age": 22}]` when `limit=2`.

## Example 3: Derived column + filter — top lines by computed total

`with_columns` adds **`line_total`**; subsequent **`filter`**, **`sort`**, and
**`head`** use the **derived** dataframe (`df2`, `df3`, …) so column references
match the migrated schema.

```python
from typing import Annotated

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pydantable import DataFrameModel


class LineItemDF(DataFrameModel):
    sku: str
    qty: int
    unit_price: float


class LineTotalRow(BaseModel):
    sku: str
    qty: int
    line_total: float


app = FastAPI()


@app.post("/procurement/top-lines", response_model=list[LineTotalRow])
def top_lines(
    rows: list[LineItemDF.RowModel],
    min_line_total: Annotated[float, Query(ge=0.0)] = 0.0,
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
):
    df = LineItemDF(rows)
    df2 = df.with_columns(line_total=df.qty * df.unit_price)
    df3 = df2.filter(df2.line_total >= min_line_total).sort(
        "line_total", descending=True
    )
    out = df3.head(limit).select("sku", "qty", "line_total")
    return out.collect()
```

For `[{"sku": "A", "qty": 2, "unit_price": 10.0}, {"sku": "B", "qty": 1, "unit_price": 5.0}]` with `min_line_total=10` and `limit=1`, **`collect()`** yields one row: **`A`** with **`line_total` 20.0**.

## Columnar vs row-shaped responses

- **`to_dict()`** / **`await ato_dict()`** — `dict[str, list]`; one JSON object with parallel arrays.
- **`collect()`** / **`await acollect()`** — `list` of Pydantic models for the **current** schema; return it from the handler and let **`response_model`** define OpenAPI and validate the serialized response.
- **`to_dicts()`** / **`await ato_dicts()`** — `list[dict]` from row models when you want plain dicts without a separate DTO class.
- **`await ato_polars()`** — optional Polars **`DataFrame`** when the **`[polars]`** extra is installed (same semantics as **`to_polars()`**).

## Error timing and API safety

In the current Rust-first design:

- invalid expression type combinations fail while building the expression AST
- invalid `filter()` condition types fail before execution
- invalid `select()` projections (for example, empty projections) fail from Rust
  logical-plan validation before execution

That keeps handlers predictable: many errors surface before **`collect()`** runs.

## Practical pattern for larger apps

- **Routes**: Pydantic request/response models; **`collect()`** for row-list responses.
- **Services**: `DataFrameModel` transforms (reusable across HTTP, CLI, workers).
- **Adapters**: load/save column dicts or row lists from databases, queues, or object storage.

This keeps schema and transformation contracts in one typed layer.

## Testing routes (`TestClient`)

Use FastAPI’s **`TestClient`** (synchronous) to exercise handlers without a live server. Install **`fastapi`** and **`httpx`** (included in the **`[dev]`** extra).

```python
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from pydantable import DataFrameModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


app = FastAPI()


@app.post("/users", response_model=list[UserRow])
def create_users(rows: list[UserDF.RowModel]):
    df = UserDF(rows)
    return df.collect()


client = TestClient(app)
r = client.post("/users", json=[{"id": 1, "age": 20}])
assert r.status_code == 200
assert r.json() == [{"id": 1, "age": 20}]
```

**Column-shaped bodies** are plain **`dict[str, list]`** JSON; use a **`dict`** parameter (or a Pydantic model wrapping that shape) and construct **`DataFrameModel(..., trusted_mode="shape_only")`** when the payload is trusted.

**OpenAPI:** `list[YourDF.RowModel]` and nested Pydantic fields follow **Pydantic v2** JSON Schema generation; there is nothing pydantable-specific beyond the generated `RowModel` types. Inspect **`GET /openapi.json`** in tests when you need stable schema snapshots.
