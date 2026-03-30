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

The **`[fastapi]`** extra installs **FastAPI** only. See {doc}`/FASTAPI` for the full
integration guide and error-handling table.

## What you ship

| Piece | Role |
|-------|------|
| **`executor_lifespan`** | Attaches a `ThreadPoolExecutor` to **`app.state.executor`** so **`acollect(executor=...)`**, **`pydantable.io`** `amaterialize_*` / `afetch_sql`, and similar offload work off the asyncio loop **without** starving the default thread pool under load. |
| **`get_executor`** + **`Depends`** | Injects that pool into handlers; **`None`** if you skip lifespan (still valid for **`acollect`**). |
| **`register_exception_handlers`** | **`MissingRustExtensionError` → 503**, **`ColumnLengthMismatchError` → 400**, in-route **`pydantic.ValidationError` → 422** (see {ref}`fastapi-errors`). |
| **Typed routes** | **`list[DataFrameModel.RowModel]`** bodies and **`response_model=list[YourRow]`** keep OpenAPI and clients aligned. |
| **Streaming** | **`astream()`** + **`ndjson_streaming_response`** from **`pydantable.fastapi`** for NDJSON (one JSON object per line). See {doc}`/FASTAPI_ENHANCEMENTS` (NDJSON semantics, production **lifespan** snippet, troubleshooting). |

## Async I/O beyond this page

This golden path uses **in-memory** frames so you can run it without a Parquet file.
In production you usually chain **lazy** readers:

- **`await MyModel.aread_parquet(path)`** (or **`Async.read_parquet`**) → **`select` / `filter`** → **`await …acollect()`**
- Prefer **`aread_*`** for **non-blocking** open/scan setup; use **`amaterialize_*`** only when you need a full **`dict[str, list]`** in memory first ({doc}`/FASTAPI`, {doc}`/IO_OVERVIEW`).

That **async read + lazy plan + async materialize** path is where pydantable differs
from hand-rolling **`asyncio.to_thread`** around pandas or Polars alone.

## Runnable example in the repo

This is the full runnable example (the same file as `docs/examples/fastapi/golden_path_app.py` in the repo).
It includes:

- **`GET /health`** — cheap probe for load balancers or Kubernetes.
- **`POST /api/v1/users`** — row-list body, **`select`** then **`acollect(executor=...)`**.
- **`GET /api/v1/users/stream`** — NDJSON chunks from **`astream`**.

```{literalinclude} examples/fastapi/golden_path_app.py
:language: python
:linenos:
```

```bash
cd docs/examples/fastapi
uvicorn golden_path_app:app --reload
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

- **Paths:** If you accept filesystem paths from clients, **allowlist** directories and reject **`..`** and symlinks where unsafe; see {doc}`/FASTAPI` Parquet examples.
- **`trusted_mode`:** Use **`trusted_mode="shape_only"`** only when upstream already guarantees schema; default validation for untrusted sources.
- **Executor size:** Set **`max_workers`** from env (see {doc}`/cookbook/fastapi_settings`); match CPU and expected concurrent heavy requests.
- **Cancellation:** `await acollect()` does **not** cancel in-flight Rust/Polars work when the client disconnects; see {doc}`/EXECUTION`.

## Related docs

- Multi-router example (routers + lifespan): `docs/examples/fastapi/service_layout/` (README in that folder)
- Roadmap and “when to use what”: {doc}`/FASTAPI_ENHANCEMENTS`
- Full FastAPI guide: {doc}`/FASTAPI`
- HTTP status mapping: {ref}`fastapi-errors` (in {doc}`/FASTAPI`)
- Columnar JSON bodies: {doc}`/cookbook/fastapi_columnar_bodies`
- Async materialization: {doc}`/cookbook/fastapi_async_materialization`
- Lazy async file pipeline: {doc}`/cookbook/async_lazy_pipeline`
- Settings (`pydantic-settings`): {doc}`/cookbook/fastapi_settings`
