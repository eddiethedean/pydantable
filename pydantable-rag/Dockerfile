# Local / CI image similar to FastAPI Cloud: app root at /app, import main:app, port 8080.
# CPU PyTorch via tool.uv.sources in pyproject.toml (same as cloud uv install).
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md main.py ./
COPY app ./app
COPY scripts ./scripts
COPY data/ /app/data/
# Hub snapshots from CI (``scripts/prep_hf_cache_ci.py``). Local builds may only
# ship ``hf_baked/.gitkeep``; runtimes then download from the Hub as before.
COPY hf_baked/ /app/.cache/huggingface/

# Editable install pulls deps (torch from pytorch-cpu index per pyproject).
# Vector index: CI workflow downloads ``data/pydantable_vectors.db`` before
# ``fastapi deploy`` / ``docker build``; local builds can use an empty ``data/``.
RUN mkdir -p /app/data \
    && uv venv /app/.venv \
    && uv pip install --python /app/.venv/bin/python -e .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
# Limit BLAS/OpenMP threads so PyTorch CPU init uses less peak RAM on small plans.
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1
ENV TORCH_NUM_THREADS=1
# Baked Hub cache from image (see COPY hf_baked); avoids per-replica Hub downloads.
ENV HF_HOME=/app/.cache/huggingface
ENV HF_HUB_CACHE=/app/.cache/huggingface/hub
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface/hub
# Retrieval + MiniLM embeds only — no local ``transformers`` causal LM (avoids
# PyTorch RAM on small plans). Set ``RAG_LLM_BACKEND=hf`` when the host has enough
# memory for SmolLM2 (or another ``RAG_LLM_MODEL``).
ENV RAG_LLM_BACKEND=extractive
ENV RAG_PRELOAD_MODELS_ON_STARTUP=false
ENV RAG_WARM_LLM_WHEN_INDEX_READY=false
# Non-blocking: if you enable preload + warm-when-index, avoid blocking startup.
ENV RAG_BLOCKING_STARTUP_WARMUP=false
# Heroku-style stacks sometimes honor this; harmless elsewhere.
ENV WEB_CONCURRENCY=1

# Match platform listener; FastAPI Cloud and most hosts set ``PORT`` (default 8080).
EXPOSE 8080
# Single worker: in-memory LLM state must not be split across processes.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]
