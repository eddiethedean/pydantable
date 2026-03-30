"""Example FastAPI app layout: lifespan, routers, pydantable.fastapi helpers.

Run from this directory::

    pip install "pydantable[fastapi]"
    uvicorn main:app --reload

See README.md in this folder.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantable.fastapi import executor_lifespan, register_exception_handlers
from routers import health, ingest


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with executor_lifespan(
        app,
        max_workers=4,
        thread_name_prefix="pydantable-example",
    ):
        yield


app = FastAPI(title="pydantable-service-layout-example", lifespan=lifespan)
register_exception_handlers(app)
app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(ingest.router, prefix="/ingest", tags=["ingest"])
