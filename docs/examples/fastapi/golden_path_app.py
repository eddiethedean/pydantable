"""Example FastAPI service using :mod:`pydantable.fastapi` helpers.

This mirrors a small production layout: versioned routes, a health check, a shared
executor for async materialization, and NDJSON streaming for larger responses.

Run (from this directory)::

    pip install 'pydantable[fastapi]'
    uvicorn golden_path_app:app --reload

Try::

    curl -s localhost:8000/health
    curl -s localhost:8000/api/v1/users -H 'Content-Type: application/json' \\
      -d '[{"id":1,"age":30},{"id":2,"age":null}]'
    curl -s -N localhost:8000/api/v1/users/stream

For **file-backed** lazy reads (``aread_parquet`` → transforms → ``acollect``), see
the GOLDEN_PATH_FASTAPI doc and the async_lazy_pipeline cookbook in the repo docs.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI
from pydantable import DataFrameModel
from pydantable.fastapi import (
    executor_lifespan,
    get_executor,
    ndjson_streaming_response,
    register_exception_handlers,
)
from pydantic import BaseModel


class UserDF(DataFrameModel):
    id: int
    age: int | None


class UserRow(BaseModel):
    id: int
    age: int | None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Dedicated pool avoids starving the default asyncio thread pool under load.
    async with executor_lifespan(
        app,
        max_workers=4,
        thread_name_prefix="pydantable-golden",
    ):
        yield


app = FastAPI(
    title="PydanTable golden path",
    version="1.0.0",
    lifespan=lifespan,
)
register_exception_handlers(app)

api = APIRouter(prefix="/api/v1", tags=["users"])


@api.post("/users", response_model=list[UserRow])
async def upsert_users(
    rows: list[UserDF.RowModel],
    executor=Depends(get_executor),  # noqa: B008
):
    """Accept validated rows, project columns, materialize off the event loop."""
    df = UserDF(rows)
    return await df.select("id", "age").acollect(executor=executor)


@api.get("/users/stream")
async def stream_users(executor=Depends(get_executor)):  # noqa: B008
    """Stream column chunks as NDJSON (one JSON object per line)."""
    df = UserDF(
        {"id": [1, 2, 3], "age": [10, None, 40]},
        trusted_mode="shape_only",
    )
    return ndjson_streaming_response(df.astream(batch_size=2, executor=executor))


app.include_router(api)


@app.get("/health")
def health() -> dict[str, str]:
    """Load balancer / Kubernetes probe: no pydantable work."""
    return {"status": "ok"}
