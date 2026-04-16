# FastAPI Integration Guide

This guide is the **navigation + reference** for using pydantable inside FastAPI:
validated request bodies, typed transforms, async materialization, and streaming.

If you want the shortest runnable service first, start with [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path.md).

**Start here:** [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path.md) (one runnable async app: lifespan, `Depends`, `acollect`, streaming).

**Related recipes:** [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md) (column-shaped JSON bodies), [fastapi_async_materialization](/cookbook/fastapi_async_materialization.md), [fastapi_observability](/cookbook/fastapi_observability.md) (request IDs + **`observe`**), [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md) (**`BackgroundTasks`** + **`submit`**), [async_lazy_pipeline](/cookbook/async_lazy_pipeline.md) (lazy `aread_*` → transforms → materialize). Example **service layout** (routers + lifespan): `docs/examples/fastapi/service_layout/` in the repo. **Roadmap / “when to use what”:** [FASTAPI_ENHANCEMENTS](/integrations/fastapi/enhancements.md).

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
  - [FASTAPI_ADVANCED](/integrations/fastapi/advanced.md) (four modes + I/O patterns)
  - [EXECUTION](/user-guide/execution.md) and [MATERIALIZATION](/user-guide/materialization.md) (deep dive)

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

1. Run [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path.md) end-to-end.
2. Pick your payload shape:
   - **Row list**: `list[YourDF.RowModel]` in requests + `response_model=list[YourDTO]` in responses.
   - **Columnar JSON**: `dict[str, list]` shapes; see [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md).
3. Decide response size:
   - Small/medium: `collect()` / `to_dict()`
   - Large: `astream()` + `ndjson_streaming_response` ([FASTAPI_ENHANCEMENTS](/integrations/fastapi/enhancements.md))
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
- **`ingest_error_response(failures, status_code=..., title=...)`** — build a structured JSON payload for batch ingest failures (typically produced by `ignore_errors=True` + `on_validation_errors=...`). This is meant for *in-handler* validation failures, not request parsing errors (which remain FastAPI’s **422**).
- **`ndjson_streaming_response(astream_iter)`** / **`ndjson_chunk_bytes(astream_iter)`** — build **`application/x-ndjson`** **`StreamingResponse`** from **`await df.astream(...)`** without duplicating JSON line encoding; see [FASTAPI_ENHANCEMENTS](/integrations/fastapi/enhancements.md).
- **`columnar_body_model`**, **`columnar_body_model_from_dataframe_model`** — build a Pydantic model whose fields are **`list[T]`** per column (OpenAPI-friendly **`dict[str, list]`**). Optional **`example=`** / **`json_schema_extra=`** for Swagger examples.
- **`columnar_dependency(model_cls, ...)`**, **`rows_dependency(model_cls, ...)`** — **`Depends(...)`** factories that validate the request body and return a **`DataFrameModel`** instance (columnar JSON or **`list[RowModel]`**), forwarding **`trusted_mode`** and related **`DataFrameModel`** kwargs. `validation_profile=` is also supported as a preset layer over `trusted_mode` / `fill_missing_optional` / `ignore_errors`. Phase 5 adds `generate_examples=` (OpenAPI enrichment) and `input_key_mode=` (alias-aware columnar ingestion).

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

To accept alias keys in **columnar** request bodies, use `input_key_mode=`:

```python
@app.post("/ingest")
def ingest(df: Annotated[User, Depends(columnar_dependency(User, input_key_mode="both"))]) -> dict:
    return df.to_dict()
```

For **row-array** JSON bodies, use **`rows_dependency(User)`**; OpenAPI documents **`list[User.RowModel]`**.

**Nested row fields** (e.g. **`inner: NestedModel`**) become **`list[NestedModel]`** in columnar JSON (one nested object per row index). That shape is valid but can be heavy on the wire; prefer flat columns when you can.

**Map-like columns** (**`dict[str, T]`**) use the same columnar encoding: each field is **`list[dict]`**-compatible per row (parallel **`dict`** values at each index), matching **`to_dict()`** after ingest. See **Map-like columns** in [SUPPORTED_TYPES](/user-guide/supported-types.md). For the JSON value-kind mapping (nested object vs map vs list), see **JSON (RFC 8259) vs column types** in [SUPPORTED_TYPES](/user-guide/supported-types.md). File-based JSON loading (array vs JSON Lines, eager vs lazy) is described in [IO_JSON](/io/json.md)—HTTP columnar bodies use the same **logical** shapes as **`materialize_json`** / **`to_dict()`**.

**Validation layers:** Pydantic validates each column as **`list[T]`** (wrong element types → **422** before your handler). **Row/column length consistency** is enforced when constructing **`DataFrameModel`** inside the dependency; mismatched lengths raise **`ColumnLengthMismatchError`** (subclass of **`ValueError`**). With **`register_exception_handlers`**, that maps to **400**; without it, you typically see **500**—see {ref}`fastapi-errors`.

**NDJSON** streaming responses do not get a per-chunk OpenAPI schema (same as any streaming body); columnar **`response_model`** applies to single JSON **`to_dict()`** responses only.

**Testing:** **`pydantable.testing.fastapi`** provides **`fastapi_app_with_executor()`** and **`fastapi_test_client(app)`** (context manager) so **`executor_lifespan`** runs under **`TestClient`** and **`get_executor`** works. Use **`TestClient(..., raise_server_exceptions=False)`** when asserting **500** responses from dependencies. See **`tests/test_pydantable_fastapi_columnar.py`**.

(fastapi-testing)=
### Testing note (lifespan and `TestClient`)

FastAPI’s `TestClient` is synchronous; if your app uses a lifespan function (including `executor_lifespan`), prefer `pydantable.testing.fastapi.fastapi_test_client(app)` so the lifespan runs and `Depends(get_executor)` works. See [FASTAPI_ENHANCEMENTS](/integrations/fastapi/enhancements.md) (Phase 7).

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

(fastapi-errors)=
## HTTP errors and exception handlers

| Situation | Typical exception | HTTP status | Notes |
|-----------|-------------------|-------------|--------|
| Invalid JSON body shape / types at the boundary | `fastapi.exceptions.RequestValidationError` | **422** | FastAPI’s default handler (before your route runs). |
| Manual validation inside a route (e.g. `model_validate`) | `pydantic.ValidationError` | **422** | Use **`register_exception_handlers`** from **`pydantable.fastapi`**, or map yourself. |
| Native extension missing | `MissingRustExtensionError` | **503** | **`register_exception_handlers`** returns a JSON **`detail`** string. |
| Mismatched column lengths after Pydantic accepted the body | `ColumnLengthMismatchError` | **400** | From **`register_exception_handlers`**; JSON **`{"detail": "<message>"}`**. |
| Engine / plan / transform errors | Often `ValueError` | **400** / **422** / **500** | **Do not** map all `ValueError`s globally; **`register_exception_handlers`** only adds **`ColumnLengthMismatchError`**. |

## Common building blocks (what most services use)

- **Request bodies**: `list[YourDF.RowModel]` (row list) or columnar JSON via [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md).
- **Responses**:
  - `collect()` / `await acollect()` → `list[Row]` (best with `response_model=list[YourDTO]`)
  - `to_dict()` / `await ato_dict()` → `dict[str, list]` (columnar JSON)
  - `astream()` + `ndjson_streaming_response(...)` → NDJSON streaming for large responses
- **Async routes**: use `await ...acollect(...)` so the loop stays responsive; prefer a bounded executor via `executor_lifespan` + `Depends(get_executor)`.
- **Testing**: use `pydantable.testing.fastapi.fastapi_test_client(app)` so lifespan runs.

If you need deeper I/O/materialization patterns, see [FASTAPI_ADVANCED](/integrations/fastapi/advanced.md) and the cookbook index ([index](/cookbook/index.md)).

## Responses: columnar vs row-shaped

- **`to_dict()`** / **`await ato_dict()`** — `dict[str, list]`; one JSON object with parallel arrays.
- **`collect()`** / **`await acollect()`** — `list` of Pydantic models for the **current** schema; return it from the handler and let **`response_model`** define OpenAPI and validate the serialized response.
- **`to_dicts()`** / **`await ato_dicts()`** — `list[dict]` from row models when you want plain dicts without a separate DTO class.
- **`await ato_polars()`** — optional Polars **`DataFrame`** when the **`[polars]`** extra is installed (same semantics as **`to_polars()`**).

(fastapi-advanced)=
## Advanced topics

If you need deeper async + I/O patterns (four materialization modes, `DataFrameModel` I/O in `async def`,
experimental URL transports, etc.), see [FASTAPI_ADVANCED](/integrations/fastapi/advanced.md).

## Trusted ingest (`trusted_mode`)

For **`DataFrameModel(...)`** and **`DataFrame[Schema](...)`**, ingestion defaults to full per-cell validation (`trusted_mode="off"`). For **trusted** bulk paths (pre-validated upstream data, internal services), use:

| Mode | Meaning |
|------|---------|
| **`trusted_mode="off"`** | Default: full Pydantic validation per cell (same as omitting the argument). |
| **`trusted_mode="shape_only"`** | Skip element validation; still checks column names and row counts. May emit **`DtypeDriftWarning`** when payloads would fail **`strict`** (see [SUPPORTED_TYPES](/user-guide/supported-types.md)). |
| **`trusted_mode="strict"`** | Trusted bulk input plus dtype / nested-shape checks against the schema (including Polars columns). |

Details: [`DATAFRAMEMODEL.md`](/user-guide/dataframemodel.md), [`SUPPORTED_TYPES.md`](/user-guide/supported-types.md).

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

Same schema rules apply as for columnar constructors in [`DATAFRAMEMODEL.md`](/user-guide/dataframemodel.md) (equal-length columns, types per field).

## Parquet and Arrow IPC uploads (multipart)
For upload routes and deeper async I/O patterns, see [FASTAPI_ADVANCED](/integrations/fastapi/advanced.md) and the I/O docs:
[IO_OVERVIEW](/io/overview.md), [IO_HTTP](/io/http.md), and the cookbook pages under [index](/cookbook/index.md).

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

Use the **same** injected executor for **`await MyModel.aread_parquet(..., executor=ex)`** / **`await afetch_sql_raw(..., executor=ex)`** (or **`await afetch_sqlmodel(..., executor=ex)`**) when you want file and SQL I/O to share the pool with **`await df.acollect(executor=ex)`**.

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

Full **`StreamingResponse`** examples (sync **`def`** + **`stream`**, **`async def`** + **`astream`**) are in [FASTAPI_ADVANCED](/integrations/fastapi/advanced.md) (four materialization modes).

**`DataFrame.stream()`** and **`DataFrame.astream()`** yield **`dict[str, list]`** batches after **one** engine collect (same contract as **`collect_batches`**; see [EXECUTION](/user-guide/execution.md)). They do **not** avoid holding the full materialized result in memory before chunking—use **pagination** or **external storage** when the table is too large for one collect. If you need **one** blob first, **`await df.ato_dict()`** / **`await df.arows()`** and then build your own response shape is still valid.

## Large tables, Polars, Arrow, and trust boundaries

**Default (`trusted_mode="off"`)** is the right choice for **public** or **untrusted** HTTP bodies: every cell is validated against your `RowModel` types before any Rust work runs. Use it when clients can send arbitrary JSON.

**When `trusted_mode` is appropriate in routes**

| Situation | Suggested mode | Notes |
|-----------|----------------|--------|
| Internal service-to-service batch (same org, authN/Z at gateway) | `shape_only` or `strict` | You still enforce column names and row counts; `strict` adds dtype / nested-shape checks for Polars and columnar buffers. |
| Upstream already validated rows (e.g. warehouse export, replay from your own DB) | `shape_only` | Fastest path; assumes wire format matches schema. |
| Polars `DataFrame` or NumPy / **PyArrow** columns built **inside** your stack | `strict` | Checks Polars dtypes (and Python column buffers where implemented) against annotations; see [`SUPPORTED_TYPES.md`](/user-guide/supported-types.md) (“Runtime column payloads”). |

**Who may skip full `RowModel` validation**

- Only code paths where **mis-typed data cannot reach the constructor** without a deliberate privilege break (private workers, ETL you own, or payloads already validated by Pydantic at an earlier hop).
- **Do not** attach `trusted_mode="shape_only"` / `strict` directly to a public upload endpoint that accepts raw user JSON unless another layer has already validated every cell.

**Polars and Arrow in handlers**

- Passing a **Polars `DataFrame`** requires trusted mode (`shape_only` or `strict`); see [`DATAFRAMEMODEL.md`](/user-guide/dataframemodel.md).
- **`strict`** rejects Polars columns whose dtypes do not match the schema (including nested list / struct / map shapes). Prefer **`strict`** when the frame comes from Arrow/Parquet/IPC and you want a safety net without per-cell Pydantic.
- For performance characteristics (validation vs ingest vs `collect()`), see [`PERFORMANCE.md`](/project/performance.md) and [`EXECUTION.md`](/user-guide/execution.md).

## End-to-end examples (moved to cookbook)

The longer “Example 1/2/3” service patterns live in the cookbook now:
[fastapi_end_to_end_examples](/cookbook/fastapi_end_to_end_examples.md).

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

**OpenAPI:** `list[YourDF.RowModel]` and nested Pydantic fields follow **Pydantic v2** JSON Schema generation; there is nothing pydantable-specific beyond the generated `RowModel` types. Inspect **`GET /openapi.json`** in tests when you need stable schema snapshots.
