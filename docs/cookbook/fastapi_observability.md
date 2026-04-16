# FastAPI: request IDs and pydantable `observe`

This recipe wires **request correlation** into a FastAPI app and shows how to emit
**`pydantable.observe`** events around materialization without adding OpenTelemetry as a
required dependency.

## Request ID middleware

Attach a stable **`request_id`** to **`request.state`** and echo it on responses so
clients and logs can join traces.

```python
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request


class RequestIdMiddleware(BaseHTTPMiddleware):
    header_name = "X-Request-ID"

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get(self.header_name) or str(uuid.uuid4())
        request.state.request_id = rid
        response = await call_next(request)
        response.headers[self.header_name] = rid
        return response
```

Register **before** routes that read **`request.state.request_id`**:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantable.observe import set_observer, span


def log_event(event: dict) -> None:
    # Send to logging or an OpenTelemetry exporter: op, duration_ms, ok, request_id, …
    rid = event.get("request_id", "")
    ms = event.get("duration_ms")
    ms_s = f"{ms:.1f}" if isinstance(ms, (int, float)) else "?"
    print(f"{rid}\t{event.get('op')}\t{ms_s}ms\tok={event.get('ok')}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    set_observer(log_event)
    try:
        yield
    finally:
        set_observer(None)


app = FastAPI(lifespan=lifespan)
app.add_middleware(RequestIdMiddleware)
```

## `observe` + `span` around materialization

Example **`async`** route: build a small lazy plan, **`await`** **`acollect`**, and time it.
**`request_id`** is merged into every emitted event for this request.

```python
from fastapi import Request

from pydantable import DataFrameModel
from pydantable.observe import span


class Row(DataFrameModel):
    user_id: int
    revenue: float


@app.post("/rollup/preview")
async def preview_rollup(request: Request, body: dict):
    """``body`` might be columnar JSON from a client; validate and materialize."""
    rid = getattr(request.state, "request_id", None)
    cols = body.get("columns")
    if not isinstance(cols, dict):
        return {"error": "expected columns dict"}
    with span("model_construct_and_acollect", request_id=rid):
        df = Row(cols, trusted_mode="shape_only")
        rows = await df.acollect()
    return {"rows": [r.model_dump() for r in rows], "request_id": rid}
```

In production, replace **`print`** in **`log_event`** with **`structlog`** or your tracer.
Map **`event["op"]`**, **`event["duration_ms"]`**, **`event["ok"]`**, and any **`**fields`**
you pass to **`span(...)`** onto span attributes.

## Pitfalls

- **`observe`** is **global**; **`set_observer`** affects the whole process—set it once in
  lifespan (or from your DI container) and always pass **`request_id`** into **`span`**
  fields so concurrent requests stay separable in logs.
- **`PYDANTABLE_TRACE=1`** prints minimal trace lines to stderr when no observer is set—useful
  locally, not in production.
- Heavy work should still use **`executor`** / **`acollect(executor=...)`** as in [GOLDEN_PATH_FASTAPI](/GOLDEN_PATH_FASTAPI.md),
  not block the event loop inside **`span`**.

## See also

- [FASTAPI](/FASTAPI.md) — integration guide and **`register_exception_handlers`**
- [fastapi_background_tasks](/cookbook/fastapi_background_tasks.md) — deferred work with **`submit`**
