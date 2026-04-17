from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.rag.ingest import ingest_repo_docs
from app.rag.llm import ChatMessage, llm_is_loaded, llm_is_loading, warm_llm
from app.rag.pipeline import rag_chat
from app.rag.store import check_vector_backend, get_counts
from app.settings import get_settings, resolve_db_path
from app.version import app_version

app = FastAPI(title="pydantable-rag")


class BootstrapResponse(BaseModel):
    ok: bool
    started: list[str]


class HealthzResponse(BaseModel):
    ok: bool
    version: str
    db_path: str
    embed_model: str
    llm_model: str
    llm_loaded: bool


class VectorBackendStatus(BaseModel):
    ok: bool
    backend: str | None = None
    error: str | None = None


class CountsStatus(BaseModel):
    docs: int
    vecs: int
    backend: str | None = None
    uninitialized: bool | None = None
    error: str | None = None


class ReadyzResponse(BaseModel):
    ok: bool
    counts: CountsStatus
    llm_loaded: bool
    db_path: str


class DiagResponse(BaseModel):
    version: str
    db_path: str
    vector_backend: VectorBackendStatus
    counts: CountsStatus
    llm_loaded: bool


class ChatRequest(BaseModel):
    message: str = Field(min_length=1)
    history: list[ChatMessage] | None = None


class ChatResponse(BaseModel):
    answer: str
    sources: list[dict]


class IngestRequest(BaseModel):
    paths: list[str] | None = None


@app.post("/bootstrap")
def bootstrap(background_tasks: BackgroundTasks) -> BootstrapResponse:
    """
    Kick off both ingestion and LLM warm-up without blocking the request.
    Useful for hosted environments where cold-start work can trigger 502s.
    """
    s = get_settings()
    repo_root = (Path(__file__).resolve().parents[2]).resolve()

    background_tasks.add_task(
        ingest_repo_docs, settings=s, repo_root=repo_root, paths=None
    )
    background_tasks.add_task(warm_llm, s.llm_model)
    return BootstrapResponse(ok=True, started=["ingest", "warm_llm"])


@app.get("/healthz")
def healthz() -> HealthzResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    return HealthzResponse(
        ok=True,
        version=app_version(),
        db_path=str(dbp),
        embed_model=s.embed_model,
        llm_model=s.llm_model,
        llm_loaded=llm_is_loaded(s.llm_model),
    )


@app.get("/readyz")
def readyz() -> ReadyzResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    docs_ready = counts["docs"] > 0 and counts["vecs"] > 0
    llm_ready = llm_is_loaded(s.llm_model)
    return ReadyzResponse(
        ok=bool(docs_ready and llm_ready),
        counts=CountsStatus.model_validate(counts),
        llm_loaded=llm_ready,
        db_path=str(dbp),
    )


@app.get("/diag")
def diag() -> DiagResponse:
    s = get_settings()
    dbp = resolve_db_path(s.db_path)
    return DiagResponse(
        version=app_version(),
        db_path=str(dbp),
        vector_backend=VectorBackendStatus.model_validate(
            check_vector_backend(db_path=dbp)
        ),
        counts=CountsStatus.model_validate(get_counts(db_path=dbp)),
        llm_loaded=llm_is_loaded(s.llm_model),
    )


@app.get("/health")
def health_compat() -> HealthzResponse:
    return healthz()


class IngestResponse(BaseModel):
    ok: bool
    started: bool


@app.post("/ingest", response_model=IngestResponse)
def ingest(
    background_tasks: BackgroundTasks,
    req: IngestRequest = Body(default_factory=IngestRequest),
) -> IngestResponse:
    s = get_settings()
    repo_root = (Path(__file__).resolve().parents[2]).resolve()
    background_tasks.add_task(
        ingest_repo_docs, settings=s, repo_root=repo_root, paths=req.paths
    )
    return IngestResponse(ok=True, started=True)


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, background_tasks: BackgroundTasks) -> ChatResponse:
    s = get_settings()
    db_path = resolve_db_path(s.db_path)

    if not llm_is_loaded(s.llm_model):
        if not llm_is_loading(s.llm_model):
            background_tasks.add_task(warm_llm, s.llm_model)
        raise HTTPException(
            status_code=503,
            detail="LLM is warming up. Retry in ~30-120s, or enable RAG_PRELOAD_MODELS_ON_STARTUP.",
        )

    result = rag_chat(
        question=req.message,
        db_path=str(db_path),
        embed_model=s.embed_model,
        embed_dims=s.embed_dims,
        top_k=s.top_k,
        llm_model=s.llm_model,
        chat_history=req.history,
    )

    return ChatResponse(
        answer=result.answer,
        sources=[
            {"source": c.source, "chunk_id": c.chunk_id, "distance": c.distance}
            for c in result.retrieved
        ],
    )


@app.on_event("startup")
async def _startup_auto_ingest() -> None:
    s = get_settings()
    if not s.auto_ingest_on_startup:
        return

    dbp = resolve_db_path(s.db_path)
    counts = get_counts(db_path=dbp)
    if s.auto_ingest_if_db_empty and counts["docs"] > 0 and counts["vecs"] > 0:
        return

    repo_root = (Path(__file__).resolve().parents[2]).resolve()
    asyncio.create_task(
        asyncio.to_thread(ingest_repo_docs, settings=s, repo_root=repo_root, paths=None)
    )


@app.on_event("startup")
async def _startup_preload_models() -> None:
    s = get_settings()
    if not s.preload_models_on_startup:
        return
    asyncio.create_task(asyncio.to_thread(warm_llm, s.llm_model))
