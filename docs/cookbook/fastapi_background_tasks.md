# FastAPI: `BackgroundTasks` and `submit` / `ExecutionHandle`

Use Starlette **`BackgroundTasks`** when you want to return an HTTP response **before**
finishing heavy **`collect()`** work, and **`DataFrame.submit()`** when that work should run
from a **thread-pool future** (see [EXECUTION](/user-guide/execution.md)).

## End-to-end pattern

1. **`executor_lifespan`** on the app (shared pool).
2. **`Depends(get_executor)`** in the route.
3. Build a **`DataFrameModel`** (here from validated columnar JSON).
4. **`submit(executor=...)`** returns an **`ExecutionHandle`** immediately.
5. **`BackgroundTasks.add_task`** runs **`await handle.result()`** after the response is sent.

```python
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import BackgroundTasks, Depends, FastAPI, Request
from pydantic import BaseModel, Field

from pydantable import DataFrameModel
from pydantable.fastapi import (
    columnar_dependency,
    executor_lifespan,
    get_executor,
    register_exception_handlers,
)


class UserRow(DataFrameModel):
    user_id: int = Field(validation_alias="userId")
    amount: float


class EnqueueResponse(BaseModel):
    accepted_rows: int
    request_id: str | None


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with executor_lifespan(app, max_workers=8, thread_name_prefix="billing-ingest"):
        yield


app = FastAPI(title="Billing ingest (example)", lifespan=lifespan)
register_exception_handlers(app)


async def persist_rollup(handle, request_id: str | None) -> None:
    """Runs after the HTTP response; holds the worker until collect finishes."""
    try:
        rows = await handle.result()
    except Exception as exc:  # noqa: BLE001 - log-only boundary in an example
        # Replace with metrics / DLQ in production
        print(f"[{request_id}] background collect failed: {exc}")
        return
    # Here: write rows to warehouse, emit metrics, etc.
    print(f"[{request_id}] persisted {len(rows)} rows")


@app.post("/ingest", response_model=EnqueueResponse)
async def enqueue(
    request: Request,
    background_tasks: BackgroundTasks,
    df: Annotated[UserRow, Depends(columnar_dependency(UserRow, trusted_mode="strict"))],
    executor=Depends(get_executor),
):
    if executor is None:
        raise RuntimeError("executor_lifespan must be configured")
    rid = getattr(request.state, "request_id", None)
    n = df.shape[0]
    handle = df.submit(executor=executor)
    background_tasks.add_task(persist_rollup, handle, rid)
    return EnqueueResponse(accepted_rows=n, request_id=rid)
```

Add **`RequestIdMiddleware`** from [fastapi_observability](/cookbook/fastapi_observability.md) if you want **`request_id`**
populated; without it, **`rid`** is **`None`** while the rest still works.

## Semantics and limits

- **`await handle.result()`** blocks the Starlette background worker until the engine finishes;
  size your **`ThreadPoolExecutor`** accordingly (and avoid huge frames in **`BackgroundTasks`**).
- Cancelling **`await acollect()`** / **`result()`** does **not** cancel in-flight Rust work—see [EXECUTION](/user-guide/execution.md).
- For jobs that must survive process restarts, use a real queue (Celery, RQ, SQS, …);
  **`BackgroundTasks`** is **in-process** and **best-effort** only.

## See also

- [MATERIALIZATION](/user-guide/materialization.md) — four terminal modes
- [fastapi_observability](/cookbook/fastapi_observability.md) — request IDs for correlating background logs
- **`docs/examples/fastapi/service_layout/`** — routers + lifespan in the repo
