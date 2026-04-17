from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel


class Settings(BaseModel):
    db_path: str = "data/pydantable_vectors.db"
    # Small, fast default embeddings for CPU deployments.
    embed_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    embed_dims: int = 384
    # Keep the default LLM small to avoid 502s/timeouts on cold start.
    llm_model: str = "HuggingFaceTB/SmolLM2-135M-Instruct"
    preload_models_on_startup: bool = False

    chunk_chars: int = 4000
    chunk_overlap_chars: int = 400
    top_k: int = 6

    auto_ingest_on_startup: bool = False
    auto_ingest_if_db_empty: bool = True


def _getenv_bool(name: str, default: bool) -> bool:
    import os

    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def get_settings() -> Settings:
    load_dotenv()
    import os

    base = Settings()
    return Settings(
        db_path=os.getenv("RAG_DB_PATH", base.db_path),
        embed_model=os.getenv("RAG_EMBED_MODEL", base.embed_model),
        embed_dims=int(os.getenv("RAG_EMBED_DIMS", str(base.embed_dims))),
        llm_model=os.getenv("RAG_LLM_MODEL", base.llm_model),
        preload_models_on_startup=_getenv_bool(
            "RAG_PRELOAD_MODELS_ON_STARTUP", base.preload_models_on_startup
        ),
        chunk_chars=int(os.getenv("RAG_CHUNK_CHARS", str(base.chunk_chars))),
        chunk_overlap_chars=int(
            os.getenv("RAG_CHUNK_OVERLAP_CHARS", str(base.chunk_overlap_chars))
        ),
        top_k=int(os.getenv("RAG_TOP_K", str(base.top_k))),
        auto_ingest_on_startup=_getenv_bool(
            "RAG_AUTO_INGEST_ON_STARTUP", base.auto_ingest_on_startup
        ),
        auto_ingest_if_db_empty=_getenv_bool(
            "RAG_AUTO_INGEST_IF_DB_EMPTY", base.auto_ingest_if_db_empty
        ),
    )


def resolve_db_path(db_path: str) -> Path:
    p = Path(db_path)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent.parent / p).resolve()
