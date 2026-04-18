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
    # Off by default: loading embed + LLM on every replica can OOM small cloud
    # instances and crash-loop (502). Enable via RAG_PRELOAD_MODELS_ON_STARTUP or
    # warm with POST /bootstrap when you have enough RAM.
    preload_models_on_startup: bool = False
    # If True and the SQLite index has rows, start a background LLM warm at
    # process start on **this** replica. Default False: on small hosts, enabling
    # this for every replica often OOM-kills the process (crash-loop in logs).
    # For load-balanced /readyz, either use enough RAM per replica and set True,
    # or min replicas 1 and POST /bootstrap / per-replica warm via traffic.
    warm_llm_when_index_ready: bool = False

    chunk_chars: int = 4000
    chunk_overlap_chars: int = 400
    top_k: int = 6

    auto_ingest_on_startup: bool = False
    auto_ingest_if_db_empty: bool = True
    # When True, lifespan blocks until ingest/LLM warm-up finishes (single-process
    # friendly; avoids serving before the model is usable). FastAPI Cloud Dockerfile
    # sets this true with RAG_PRELOAD_MODELS_ON_STARTUP.
    blocking_startup_warmup: bool = False


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
        warm_llm_when_index_ready=_getenv_bool(
            "RAG_WARM_LLM_WHEN_INDEX_READY", base.warm_llm_when_index_ready
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
        blocking_startup_warmup=_getenv_bool(
            "RAG_BLOCKING_STARTUP_WARMUP", base.blocking_startup_warmup
        ),
    )


def resolve_db_path(db_path: str) -> Path:
    p = Path(db_path)
    if p.is_absolute():
        return p
    return (Path(__file__).resolve().parent.parent / p).resolve()


def resolve_ingest_repo_root() -> Path:
    """
    Base directory for ingest default paths (``README.md``, ``docs/``).

    - If ``RAG_REPO_ROOT`` is set, that path is used (expanded and resolved).
    - Else, if the directory *above* the ``pydantable-rag`` folder contains a
      ``docs/`` tree (typical monorepo checkout), use it so local dev ingests the
      main library documentation.
    - Otherwise use the parent of ``app/`` (the service / project root). On
      FastAPI Cloud this is usually ``/app``, where ``README.md`` from the
      deployed project lives. Using ``parents[2]`` from ``app/main.py`` instead
      would incorrectly resolve to filesystem root ``/`` when only the service
      is deployed.
    """
    import os

    raw = os.getenv("RAG_REPO_ROOT")
    if raw:
        return Path(raw).expanduser().resolve()

    here = Path(__file__).resolve()
    service_root = here.parents[1]
    upstream = here.parents[2]
    if upstream != Path("/") and (upstream / "docs").is_dir():
        return upstream
    return service_root
