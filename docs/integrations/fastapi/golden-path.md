# Golden path: FastAPI + pydantable

This page is a **realistic** starting point for services: versioned JSON routes, a
health check, non-blocking materialization with a **shared thread pool**, optional
**NDJSON** streaming, and hooks for the same patterns on **lazy file reads** when you
add storage.

## Prerequisites

```bash
pip install "pydantable[fastapi]"
# File uploads (multipart) in routes:
pip install "python-multipart"
```

The **`[fastapi]`** extra installs **FastAPI** only. See [FASTAPI](../../integrations/fastapi/fastapi.md) for the full
integration guide and error-handling table.

## What you ship

| Piece | Role |
|-------|------|
| **`executor_lifespan`** | Attaches a `ThreadPoolExecutor` to **`app.state.executor`** so **`acollect(executor=...)`**, **`pydantable.io`** `amaterialize_*` / `afetch_sql`, and similar offload work off the asyncio loop **without** starving the default thread pool under load. |
| **`get_executor`** + **`Depends`** | Injects that pool into handlers; **`None`** if you skip lifespan (still valid for **`acollect`**). |
| **`register_exception_handlers`** | **`MissingRustExtensionError` → 503**, **`ColumnLengthMismatchError` → 400**, in-route **`pydantic.ValidationError` → 422** (see [HTTP errors and exception handlers](fastapi.md#http-errors-and-exception-handlers)). |
| **Typed routes** | **`list[DataFrameModel.RowModel]`** bodies and **`response_model=list[YourRow]`** keep OpenAPI and clients aligned. |
| **Streaming** | **`astream()`** + **`ndjson_streaming_response`** from **`pydantable.fastapi`** for NDJSON (one JSON object per line). See [FASTAPI_ENHANCEMENTS](../../integrations/fastapi/enhancements.md) (NDJSON semantics, production **lifespan** snippet, troubleshooting). |

## Async I/O beyond this page

This golden path uses **in-memory** frames so you can run it without a Parquet file.
In production you usually chain **lazy** readers:

- **`await MyModel.aread_parquet(path)`** (or **`Async.read_parquet`**) → **`select` / `filter`** → **`await …acollect()`**
- Prefer **`aread_*`** for **non-blocking** open/scan setup; use **`amaterialize_*`** only when you need a full **`dict[str, list]`** in memory first ([FASTAPI](../../integrations/fastapi/fastapi.md), [IO_OVERVIEW](../../io/overview.md)).

That **async read + lazy plan + async materialize** path is where pydantable differs
from hand-rolling **`asyncio.to_thread`** around pandas or Polars alone.

## Runnable example in the repo

This is the full runnable example (the same file as `docs/examples/fastapi/golden_path_app.py` in the repo).
It includes:

- **`GET /health`** — cheap probe for load balancers or Kubernetes.
- **`POST /api/v1/users`** — row-list body, **`select`** then **`acollect(executor=...)`**.
- **`GET /api/v1/users/stream`** — NDJSON chunks from **`astream`**.


--8<-- "examples/fastapi/golden_path_app.py"

```bash
cd docs/examples/fastapi
uvicorn golden_path_app:app --reload
```

### Script output (running the file)

If you run the example file directly (without starting a server), it executes a small self-check:

```bash
PYTHONPATH=python python docs/examples/fastapi/golden_path_app.py
```

```text
--8<-- "examples/fastapi/golden_path_app.py.out.txt"
```

```bash
curl -s localhost:8000/health
curl -s localhost:8000/api/v1/users \
  -H 'Content-Type: application/json' \
  -d '[{"id":1,"age":30},{"id":2,"age":null}]'
curl -s -N localhost:8000/api/v1/users/stream
```

Expected output (example):

```text
{"status":"ok"}
[{"id":1,"age":30},{"id":2,"age":null}]
{"id": [1, 2], "age": [10, null]}
{"id": [3], "age": [40]}
```

## Production checklist

- **Paths:** If you accept filesystem paths from clients, **allowlist** directories and reject **`..`** and symlinks where unsafe; see [FASTAPI](../../integrations/fastapi/fastapi.md) Parquet examples.
- **`trusted_mode`:** Use **`trusted_mode="shape_only"`** only when upstream already guarantees schema; default validation for untrusted sources.
- **Executor size:** Set **`max_workers`** from env (see [fastapi_settings](../../cookbook/fastapi_settings.md)); match CPU and expected concurrent heavy requests.
- **Cancellation:** `await acollect()` does **not** cancel in-flight Rust/Polars work when the client disconnects; see [EXECUTION](../../user-guide/execution.md).

## Related docs

- Multi-router example (routers + lifespan): `docs/examples/fastapi/service_layout/` (README in that folder)
- Roadmap and “when to use what”: [FASTAPI_ENHANCEMENTS](../../integrations/fastapi/enhancements.md)
- Full FastAPI guide: [FASTAPI](../../integrations/fastapi/fastapi.md)
- HTTP status mapping: [HTTP errors and exception handlers](fastapi.md#http-errors-and-exception-handlers)
- Columnar JSON bodies: [fastapi_columnar_bodies](../../cookbook/fastapi_columnar_bodies.md)
- Async materialization: [fastapi_async_materialization](../../cookbook/fastapi_async_materialization.md)
- Lazy async file pipeline: [async_lazy_pipeline](../../cookbook/async_lazy_pipeline.md)
- Settings (`pydantic-settings`): [fastapi_settings](../../cookbook/fastapi_settings.md)
