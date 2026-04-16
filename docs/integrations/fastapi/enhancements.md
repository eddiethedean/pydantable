# FastAPI enhancements roadmap

This page tracks **planned and shipped** improvements for using pydantable with **FastAPI**
and **Pydantic**. For the current integration guide, see [FASTAPI](/integrations/fastapi/fastapi.md); for the shortest
runnable service, see [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path.md).

## When to use what (quick reference)

| Need | Prefer | Avoid / notes |
|------|--------|----------------|
| Lazy file scan + transforms + async materialize | **`await Model.aread_*`**, then **`select` / `filter`**, then **`await …acollect()`** | **`amaterialize_*`** when you only need a lazy pipeline (eager loads full columns first). |
| Eager columns from file or SQL, then typed frame | **`await amaterialize_*`** / **`await afetch_sql`** from **`pydantable.io`**, then **`MyModel(cols)`** | Mixing eager load with unnecessary extra copies—see [IO_OVERVIEW](/io/overview.md). |
| Row list JSON **`response_model`** | **`await df.acollect()`** (or **`collect()`** in sync routes) | **`to_dict()`** when clients expect columnar JSON. |
| Columnar JSON response | **`await df.ato_dict()`** | **`acollect()`** if the client expects a list of objects. |
| Large responses / back-pressure | **`astream()`** + **`ndjson_streaming_response`** ([FASTAPI](/integrations/fastapi/fastapi.md) helpers) | Single giant **`to_dict()`** in memory. |
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
| **503** with **`detail`** about **`_core`** | **`MissingRustExtensionError`** — install a wheel or build the native extension ([DEVELOPER](/project/developer.md)). |
| Empty stream body | **`astream()`** produced no chunks (empty frame or zero batches); NDJSON is still valid with an empty body. |
| High latency under concurrent requests | Raise **`executor_lifespan(..., max_workers=...)`** or reduce competing work on the default thread pool ([EXECUTION](/user-guide/execution.md)). |
| Client disconnect does not stop engine work | Documented limitation: cancelling **`await acollect()`** does not cancel in-flight Rust work ([EXECUTION](/user-guide/execution.md)). |
| **500** after **422**-valid columnar JSON (no handlers) | **`ColumnLengthMismatchError`** when column lengths differ. Call **`register_exception_handlers`** for **400** with a **`detail`** string; or catch in-route. |
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

- **`columnar_dependency`** / **`rows_dependency`** build a **`DataFrameModel`** from validated bodies; see [FASTAPI](/integrations/fastapi/fastapi.md) **Columnar OpenAPI and Depends** and [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md).
- Optional **named executors** (e.g. **`app.state.executors["io"]`**) for separate pools — not packaged; pattern only.

### Phase 4 — Errors (shipped)

- **`PydantableUserError`**, **`ColumnLengthMismatchError`** in **`pydantable.errors`** (subclass **`ValueError`**). **`register_exception_handlers`** maps **`ColumnLengthMismatchError`** → **400**. Further narrow types can be added incrementally ([FASTAPI](/integrations/fastapi/fastapi.md) error table).

### Phase 5 — Ops / observability (shipped as docs)

- [fastapi_observability](/cookbook/fastapi_observability.md) — request-ID middleware, **`observe.emit`** / **`span`**, optional OpenTelemetry bridge note.

### Phase 6 — Long-running work (shipped as docs)

- [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md) — **`BackgroundTasks`** + **`submit`** / **`ExecutionHandle`**, executor alignment, cancellation limits ([EXECUTION](/user-guide/execution.md)).

### Phase 7 — Testing (shipped)

- **`pydantable.testing.fastapi`:** **`fastapi_app_with_executor`**, **`fastapi_test_client`** (lifespan-aware **`TestClient`**). See [FASTAPI](/integrations/fastapi/fastapi.md) **Columnar OpenAPI and Depends**.

### Phase 8 — Templates (shipped as layout + docs)

- Checked-in **example service** layout: **`docs/examples/fastapi/service_layout/`** (`main.py`, **`routers/`**, **`README.md`**). Not a published package—copy into your repo. Cookiecutter / **`uv`** template remains optional future work.

## See also

- [FASTAPI](/integrations/fastapi/fastapi.md)
- [GOLDEN_PATH_FASTAPI](/integrations/fastapi/golden-path.md)
- [fastapi_observability](/cookbook/fastapi_observability.md)
- [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md)
- [fastapi_columnar_bodies](/cookbook/fastapi_columnar_bodies.md)
- [async_lazy_pipeline](/cookbook/async_lazy_pipeline.md)
- [EXECUTION](/user-guide/execution.md)
- [DEVELOPER](/project/developer.md) (native extension build / wheels)
- [ROADMAP](/project/roadmap.md) (product-wide backlog)
