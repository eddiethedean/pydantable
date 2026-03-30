# FastAPI enhancements roadmap

This page tracks **planned and shipped** improvements for using pydantable with **FastAPI**
and **Pydantic**. For the current integration guide, see {doc}`/FASTAPI`; for the shortest
runnable service, see {doc}`/GOLDEN_PATH_FASTAPI`.

## When to use what (quick reference)

| Need | Prefer | Avoid / notes |
|------|--------|----------------|
| Lazy file scan + transforms + async materialize | **`await Model.aread_*`**, then **`select` / `filter`**, then **`await …acollect()`** | **`amaterialize_*`** when you only need a lazy pipeline (eager loads full columns first). |
| Eager columns from file or SQL, then typed frame | **`await amaterialize_*`** / **`await afetch_sql`** from **`pydantable.io`**, then **`MyModel(cols)`** | Mixing eager load with unnecessary extra copies—see {doc}`/IO_OVERVIEW`. |
| Row list JSON **`response_model`** | **`await df.acollect()`** (or **`collect()`** in sync routes) | **`to_dict()`** when clients expect columnar JSON. |
| Columnar JSON response | **`await df.ato_dict()`** | **`acollect()`** if the client expects a list of objects. |
| Large responses / back-pressure | **`astream()`** + **`ndjson_streaming_response`** ({doc}`/FASTAPI` helpers) | Single giant **`to_dict()`** in memory. |
| Untrusted user data | Default **`DataFrameModel`** validation | **`trusted_mode="shape_only"`** unless upstream guarantees cells. |
| Shared thread pool for engine / I/O offload | **`executor_lifespan`** + **`Depends(get_executor)`** | Relying only on the default **`asyncio`** thread pool under heavy load. |

## Production pattern (lifespan + handlers + NDJSON)

Typical **`main.py`** wiring:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from pydantable.fastapi import (
    executor_lifespan,
    get_executor,
    ndjson_streaming_response,
    register_exception_handlers,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with executor_lifespan(app, max_workers=8, thread_name_prefix="myapp"):
        yield

app = FastAPI(lifespan=lifespan)
register_exception_handlers(app)
```

Then in routes: **`Depends(get_executor)`** for **`acollect(executor=...)`**, and
**`return ndjson_streaming_response(df.astream(..., executor=executor))`** for streaming
responses. Install **`pip install 'pydantable[fastapi]'`**.

## NDJSON helpers (`pydantable.fastapi`)

- **`ndjson_streaming_response(async_iter, media_type=...)`** — returns a Starlette
  **`StreamingResponse`**. Default **`media_type`** is **`application/x-ndjson`**; use
  **`application/jsonlines`** if your clients expect that label instead.
- **`ndjson_chunk_bytes(async_iter)`** — async iterator of **UTF-8** lines (**JSON object +
  `\n`** per chunk). Use when you set headers or status yourself on **`StreamingResponse`**.
- Chunks are whatever **`astream()`** yields (**`dict[str, list]`**). Values must be
  **JSON-serializable** (standard **`json.dumps`** rules).

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| **422** on POST before your handler runs | FastAPI **`RequestValidationError`** (body shape/types). Not the same as **`register_exception_handlers`**’s **`pydantic.ValidationError`** handler (in-route). |
| **503** with **`detail`** about **`_core`** | **`MissingRustExtensionError`** — install a wheel or build the native extension ({doc}`/DEVELOPER`). |
| Empty stream body | **`astream()`** produced no chunks (empty frame or zero batches); NDJSON is still valid with an empty body. |
| High latency under concurrent requests | Raise **`executor_lifespan(..., max_workers=...)`** or reduce competing work on the default thread pool ({doc}`/EXECUTION`). |
| Client disconnect does not stop engine work | Documented limitation: cancelling **`await acollect()`** does not cancel in-flight Rust work ({doc}`/EXECUTION`). |
| **500** after **422**-valid columnar JSON | **`DataFrameModel`** construction in **`columnar_dependency`** / **`rows_dependency`** can raise **`ValueError`** (e.g. column length mismatch). Map in the route or with an exception handler; not a **`RequestValidationError`**. |
| OpenAPI shows **`list`** but clients send wrong element type | FastAPI returns **422** from Pydantic; fix payload types. |

## Phased roadmap

### Phase 1 (shipped with this doc’s tooling)

- **`ndjson_streaming_response`** / **`ndjson_chunk_bytes`** in **`pydantable.fastapi`** — NDJSON **`StreamingResponse`** from **`astream()`** without duplicating encode logic.
- Combined **lifespan + exception handlers** pattern documented here (explicit **`executor_lifespan`** + **`register_exception_handlers`** — no hidden globals).
- This **quick reference** table and cross-links.

### Phase 2 — OpenAPI / bodies (shipped)

- **`columnar_body_model`** / **`columnar_body_model_from_dataframe_model`** in **`pydantable.fastapi`** — generated Pydantic models with **`list[T]`** per column, optional **`example=`** / **`json_schema_extra=`** for OpenAPI.
- Use the same model as **`response_model`** for columnar JSON (**`to_dict()`** shape). NDJSON streams still have no per-line OpenAPI schema.

### Phase 3 — Dependencies (shipped)

- **`columnar_dependency`** / **`rows_dependency`** build a **`DataFrameModel`** from validated bodies; see {doc}`/FASTAPI` **Columnar OpenAPI and Depends** and {doc}`/cookbook/fastapi_columnar_bodies`.
- Optional **named executors** (e.g. **`app.state.executors["io"]`**) for separate pools — not packaged; pattern only.

### Phase 4 — Errors

- Narrow **pydantable-specific** exception types (subclasses) mappable to **4xx** in **`register_exception_handlers`**, instead of blanket **`ValueError`** mapping.

### Phase 5 — Ops / observability

- Request-ID + **`observe`** cookbook; optional OpenTelemetry span attributes aligned with **`set_observer`**.

### Phase 6 — Long-running work

- Documented **`BackgroundTasks`** + **`submit`** / **`ExecutionHandle`** patterns for deferred materialization.

### Phase 7 — Testing (shipped)

- **`pydantable.testing.fastapi`:** **`fastapi_app_with_executor`**, **`fastapi_test_client`** (lifespan-aware **`TestClient`**). See {doc}`/FASTAPI` **Columnar OpenAPI and Depends**.

### Phase 8 — Templates

- Cookiecutter / **`uv`** template or a checked-in **example service** layout (docs milestone unless promoted to a package).

## See also

- {doc}`/FASTAPI`
- {doc}`/GOLDEN_PATH_FASTAPI`
- {doc}`/cookbook/fastapi_columnar_bodies`
- {doc}`/cookbook/async_lazy_pipeline`
- {doc}`/EXECUTION`
- {doc}`/DEVELOPER` (native extension build / wheels)
- {doc}`/ROADMAP` (product-wide backlog)
