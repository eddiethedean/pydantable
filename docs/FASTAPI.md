# FastAPI Integration Guide

This guide shows FastAPI-oriented patterns using `DataFrameModel` as the
primary API: validated request bodies, typed transforms, and **`collect()`** to
materialize **row lists** for JSON responses.

## Why this matters

For FastAPI services, `pydantable` gives you:

- Pydantic validation at API boundaries (`RowModel` per `DataFrameModel`)
- typed dataframe transformations in handlers or services
- **`collect()`** — `list` of Pydantic models for the **current** projection (ideal for `response_model=list[YourRow]`)
- **`to_dict()`** — `dict[str, list]` when the response is **column-shaped** JSON
- **`acollect()`**, **`ato_dict()`**, **`ato_polars()`**, **`ato_arrow()`** — same semantics off the event loop (see below)
- **`pydantable.io`** — **`read_*` / `materialize_*` / `export_*`** for **Parquet**, **Arrow IPC**, **CSV**, **NDJSON** (Rust-first on local paths where the wheel supports it; **PyArrow** for buffers, column subsets, streaming IPC). For **out-of-core** pipelines use **`read_*`** + transforms + **`DataFrame.write_parquet`**. Extras: **`[sql]`** (SQLAlchemy **`fetch_sql`**), **`[cloud]`** (**`fsspec`** URLs), **`[rap]`** (true-async CSV via **`aread_csv_rap`**). See **`pip install 'pydantable[io]'`** for **PyArrow + Polars** together.
- **`amaterialize_parquet`**, **`amaterialize_ipc`**, **`amaterialize_csv`**, **`amaterialize_ndjson`** and **`aexport_parquet`**, **`aexport_ipc`**, **`aexport_csv`**, **`aexport_ndjson`** — eager file I/O **off the event loop** (**`asyncio.to_thread`** by default, or your **`executor=`**). Use inside **`async def`** when you need a full **`dict[str, list]`** (e.g. to pass into **`DataFrameModel`**), or to **export** columns to a path.
- **`afetch_sql`** / **`awrite_sql`** — SQLAlchemy-backed table I/O without blocking the loop (same threading model).
- **`to_arrow()`** — materialize a PyArrow **`Table`** after the same engine path as **`to_dict()`** (not zero-copy; see [`EXECUTION.md`](EXECUTION.md))

**Synchronous materialization** (`collect()`, `to_dict()`, `collect(as_lists=True)`, optional `to_polars()`) runs **blocking** Rust + Polars work on the **current thread**.

**Asynchronous materialization (0.15.0+):** `await df.acollect()`, `await df.ato_dict()`, `await df.ato_polars()`, and ( **0.16.0+** ) `await df.ato_arrow()` run that blocking work in **`asyncio.to_thread`** by default, or in a **`concurrent.futures.Executor`** you pass as `executor=...`. Use this from **`async def`** route handlers so the ASGI event loop stays responsive. Cancelling the waiter does **not** cancel in-flight engine work; see [`EXECUTION.md`](EXECUTION.md) for limits (GIL, copies, `to_polars`, `to_arrow`).

**`DataFrameModel`** also exposes **`arows()`** and **`ato_dicts()`** as async counterparts to **`rows()`** / **`to_dicts()`**.

## Install

From PyPI (prebuilt wheels include the native extension on supported platforms):

```bash
pip install pydantable
```

From a git checkout, build the extension (for example with [Maturin](https://www.maturin.rs/)):

```bash
pip install .
```

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

For **file** bodies, read bytes in the handler and use **`materialize_parquet`** / **`materialize_ipc`** from **`pydantable.io`** ( **`as_stream=True`** for streaming IPC); reads from **bytes** require **`pyarrow`** (**`pip install 'pydantable[arrow]'`** or **`[io]`**). That **`materialize_*`** call is still **blocking** while PyArrow decodes—either keep uploads small or offload with **`asyncio.to_thread(materialize_parquet, raw, …)`** / **`run_in_executor`**. For **paths on disk** (shared volume, temp file after **`await file.read()`**), prefer **`await amaterialize_parquet(path)`** so the pattern is consistent with other async routes. FastAPI file routes require **`python-multipart`** (`pip install python-multipart`).

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
    cols = materialize_parquet(raw)
    df = UserDF(cols, trusted_mode="shape_only")
    return df.to_dict()
```

## `pydantable.io` async I/O in `async def` routes

For **FastAPI**, the **`amaterialize_*`**, **`aread_*`**, **`afetch_sql`**, **`aexport_*`**, and **`awrite_sql`** helpers in **`pydantable.io`** mirror the sync API but run work in **`asyncio.to_thread`** (or in a **`ThreadPoolExecutor`** you pass as **`executor=`**). That keeps the ASGI event loop responsive while **Rust**, **PyArrow**, **stdlib CSV**, or **SQLAlchemy** does blocking I/O.

**Install** what you need:

```bash
pip install 'pydantable[io]'      # PyArrow + Polars for columnar paths / fallbacks
pip install 'pydantable[sql]'     # SQLAlchemy only; add your DB driver (psycopg, pymysql, …)
pip install 'pydantable[cloud]'   # fsspec for s3://, gs://, file://, … (experimental)
pip install 'pydantable[rap]'     # optional: aread_csv_rap (rapcsv + rapfiles)
```

| Sync | Async | Typical use in a route |
|------|-------|-------------------------|
| **`read_parquet`** (lazy root) | **`aread_parquet`** | Large local Parquet without full Python dict |
| **`materialize_parquet`** | **`amaterialize_parquet`** | Eager columns from path or bytes |
| **`materialize_ipc`** | **`amaterialize_ipc`** | Arrow IPC / Feather file |
| **`materialize_csv`** | **`amaterialize_csv`** | CSV path |
| **`materialize_ndjson`** | **`amaterialize_ndjson`** | NDJSON / JSON-lines file |
| **`export_parquet`** | **`aexport_parquet`** | Persist column dict to Parquet |
| **`export_ipc`** / **`export_csv`** / **`export_ndjson`** | **`aexport_*`** | Same for other formats |
| **`fetch_sql`** | **`afetch_sql`** | Parameterized **`SELECT`** → **`dict[str, list]`** |
| **`write_sql`** | **`awrite_sql`** | Append or replace table from column dict |

**`DataFrame.write_parquet`** (and siblings) are synchronous (Rust); call them from a thread pool inside **`async def`** if the write can be large. All **`amaterialize_*`** / **`aread_*`** / **`afetch_*`** / **`aexport_*`** / **`awrite_sql`** accept **`executor=`** (same semantics as **`df.acollect(executor=…)`**). Reuse one bounded **`ThreadPoolExecutor`** from **`app.state`** or **`Depends`** for dataframe materialization **and** file/SQL I/O so load is predictable under concurrency.

### Example: read a Parquet path without blocking the loop

```python
from fastapi import FastAPI

from pydantable import DataFrameModel
from pydantable.io import amaterialize_parquet


class RowDF(DataFrameModel):
    id: int
    name: str


app = FastAPI()


@app.get("/from-parquet-path")
async def from_parquet_path(path: str):
    cols = await amaterialize_parquet(path)
    df = RowDF(cols, trusted_mode="shape_only")
    return await df.ato_dict()
```

### Example: upload → temp file → async read

For large uploads, avoid holding **`bytes`** longer than necessary: write to a temp path, **`amaterialize_*`**, then delete.

```python
import tempfile
from pathlib import Path

from fastapi import FastAPI, UploadFile

from pydantable.io import amaterialize_parquet


app = FastAPI()


@app.post("/upload-parquet-async")
async def upload_parquet_async(file: UploadFile):
    suffix = Path(file.filename or "upload").suffix or ".parquet"
    data = await file.read()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data)
        path = tmp.name
    try:
        return await amaterialize_parquet(path)
    finally:
        Path(path).unlink(missing_ok=True)
```

### Async SQL (`afetch_sql` / `awrite_sql`)

**SQLAlchemy 2.x** (**`pip install 'pydantable[sql]'`** plus your **DBAPI** driver): **`fetch_sql`** / **`write_sql`** support any SQLAlchemy URL (**PostgreSQL**, **MySQL**, **SQLite**, **SQL Server**, …). In **`async def`** handlers, use **`afetch_sql`** / **`awrite_sql`**. Keep **`SELECT`** parameterized; never build SQL from untrusted request fields without binds.

```python
from typing import Annotated, Any

from fastapi import Body

from pydantable.io import afetch_sql, awrite_sql


@app.get("/rows")
async def rows(database_url: str):
    cols = await afetch_sql(
        "SELECT id, name FROM t WHERE active = :a",
        database_url,
        parameters={"a": True},
    )
    return cols


@app.post("/bulk-insert")
async def bulk_insert(
    database_url: str,
    cols: Annotated[dict[str, list[Any]], Body(...)],
):
    await awrite_sql(cols, "staging_events", database_url, if_exists="append")
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

Use the **same** injected executor for **`await amaterialize_parquet(..., executor=ex)`** / **`await afetch_sql(..., executor=ex)`** when you want file and SQL I/O to share the pool with **`await df.acollect(executor=ex)`**.

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

### Chunked or streaming JSON

There is **no** built-in row-by-row or column-chunk **`async` iterator** for materialization yet. For **`StreamingResponse`**, a practical pattern is to **`await df.ato_dict()`** (or **`await df.arows()`**) and then stream **serialized chunks** you build yourself (for example **NDJSON** lines or pre-sized batches), keeping in mind that the full result may already be in memory after **`ato_dict`**. For very large tables, prefer **pagination** or **external storage** at the API design level.

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
