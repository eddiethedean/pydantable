from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.rag.embeddings import (
    embedder_is_loaded,
    embedder_is_loading,
    embedding_compute_active,
)
from app.rag.ingest import ingest_repo_docs
from app.rag.llm import ChatMessage, llm_is_loaded, llm_is_loading, warm_llm
from app.rag.pipeline import rag_chat
from app.rag.store import check_vector_backend, get_counts
from app.settings import (
    Settings,
    get_settings,
    resolve_db_path,
    resolve_ingest_repo_root,
)
from app.version import app_version

app = FastAPI(title="pydantable-rag")

_log = logging.getLogger(__name__)


def _ingest_then_warm_llm(
    *, settings: Settings, repo_root: Path, paths: list[str] | None
) -> None:
    """
    Run ingestion before loading the chat LLM so two large HF models are not
    resident at once (avoids OOM on small cloud instances when both were
    scheduled as separate background tasks).
    """
    ingest_repo_docs(settings=settings, repo_root=repo_root, paths=paths)
    warm_llm(settings.llm_model)


def _bootstrap_ingest_then_warm(
    *, settings: Settings, repo_root: Path, paths: list[str] | None
) -> None:
    try:
        _ingest_then_warm_llm(settings=settings, repo_root=repo_root, paths=paths)
    except Exception:
        _log.exception("pydantable-rag: POST /bootstrap background task failed")


class BootstrapResponse(BaseModel):
    ok: bool
    started: list[str]


class HealthzResponse(BaseModel):
    ok: bool
    version: str
    db_path: str
    embed_model: str
    embed_dims: int
    llm_model: str
    llm_loaded: bool
    llm_loading: bool = Field(
        description="True while the LLM is downloading or initializing in-process.",
    )
    embed_loaded: bool = Field(
        description="True once the embedding model for this app is in memory.",
    )
    embed_loading: bool = Field(
        description="True while the embedding model is downloading or initializing.",
    )
    embed_computing: bool = Field(
        description="True while a forward pass is computing vectors (ingest or chat).",
    )


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
    llm_loading: bool
    embed_loaded: bool
    embed_loading: bool
    embed_computing: bool
    db_path: str


class DiagResponse(BaseModel):
    version: str
    db_path: str
    vector_backend: VectorBackendStatus
    counts: CountsStatus
    llm_loaded: bool
    llm_loading: bool
    embed_loaded: bool
    embed_loading: bool
    embed_computing: bool


class ChatRequest(BaseModel):
    message: str = Field(min_length=1)
    history: list[ChatMessage] | None = None


class ChatResponse(BaseModel):
    answer: str
    sources: list[dict]


class IngestRequest(BaseModel):
    paths: list[str] | None = None


_INGEST_BODY = Body(default_factory=IngestRequest)


@app.post("/bootstrap")
def bootstrap(background_tasks: BackgroundTasks) -> BootstrapResponse:
    """
    Kick off both ingestion and LLM warm-up without blocking the request.
    Useful for hosted environments where cold-start work can trigger 502s.
    """
    s = get_settings()
    repo_root = resolve_ingest_repo_root()

    background_tasks.add_task(
        _bootstrap_ingest_then_warm,
        settings=s,
        repo_root=repo_root,
        paths=None,
    )
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
        embed_dims=s.embed_dims,
        llm_model=s.llm_model,
        llm_loaded=llm_is_loaded(s.llm_model),
        llm_loading=llm_is_loading(s.llm_model),
        embed_loaded=embedder_is_loaded(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
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
        llm_loading=llm_is_loading(s.llm_model),
        embed_loaded=embedder_is_loaded(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
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
        llm_loading=llm_is_loading(s.llm_model),
        embed_loaded=embedder_is_loaded(s.embed_model, s.embed_dims),
        embed_loading=embedder_is_loading(s.embed_model, s.embed_dims),
        embed_computing=embedding_compute_active(),
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
    req: IngestRequest = _INGEST_BODY,
) -> IngestResponse:
    s = get_settings()
    repo_root = resolve_ingest_repo_root()
    background_tasks.add_task(
        ingest_repo_docs, settings=s, repo_root=repo_root, paths=req.paths
    )
    return IngestResponse(ok=True, started=True)


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest, background_tasks: BackgroundTasks) -> ChatResponse:
    s = get_settings()
    db_path = resolve_db_path(s.db_path)

    if not llm_is_loaded(s.llm_model):
        loading = llm_is_loading(s.llm_model)
        if not loading:
            background_tasks.add_task(warm_llm, s.llm_model)
        detail = (
            "LLM is loading (download/initialize in progress). Retry shortly."
            if loading
            else (
                "LLM not loaded yet; warm-up was queued. Retry in ~30-120s, or enable "
                "RAG_PRELOAD_MODELS_ON_STARTUP."
            )
        )
        raise HTTPException(status_code=503, detail=detail)

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
async def _startup_background_warmup() -> None:
    """
    Optional ingest and/or LLM preload.
    Ingest runs before LLM when both are enabled.
    """
    s = get_settings()
    want_ingest = s.auto_ingest_on_startup
    want_llm = s.preload_models_on_startup
    if not want_ingest and not want_llm:
        return

    if want_ingest:
        dbp = resolve_db_path(s.db_path)
        counts = get_counts(db_path=dbp)
        if s.auto_ingest_if_db_empty and counts["docs"] > 0 and counts["vecs"] > 0:
            want_ingest = False

    if not want_ingest and not want_llm:
        return

    async def _run() -> None:
        rr = resolve_ingest_repo_root()
        try:
            if want_ingest and want_llm:
                await asyncio.to_thread(
                    _ingest_then_warm_llm,
                    settings=s,
                    repo_root=rr,
                    paths=None,
                )
            elif want_ingest:
                await asyncio.to_thread(
                    ingest_repo_docs, settings=s, repo_root=rr, paths=None
                )
            elif want_llm:
                await asyncio.to_thread(warm_llm, s.llm_model)
        except Exception:
            _log.exception("pydantable-rag: startup background warmup failed")

    asyncio.create_task(_run())
