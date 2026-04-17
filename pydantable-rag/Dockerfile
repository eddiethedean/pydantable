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

# Editable install pulls deps (torch from pytorch-cpu index per pyproject).
# Vector index: CI workflow downloads ``data/pydantable_vectors.db`` before
# ``fastapi deploy`` / ``docker build``; local builds can use an empty ``data/``.
RUN mkdir -p /app/data /tmp/hf \
    && uv venv /app/.venv \
    && uv pip install --python /app/.venv/bin/python -e .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
# Writable Hugging Face cache (some runtimes have a read-only home directory).
ENV HF_HOME=/tmp/hf
ENV HF_HUB_CACHE=/tmp/hf
ENV TRANSFORMERS_CACHE=/tmp/hf
# Serve chat after cold start without manual ``POST /bootstrap`` (override if RAM is tight).
ENV RAG_PRELOAD_MODELS_ON_STARTUP=true
# Non-blocking: blocking warmup can exceed platform health-check / grace windows and
# trigger restart loops before the LLM finishes downloading. Warm-up runs in a
# background thread; ``POST /chat`` still awaits ``warm_llm`` until ready.
ENV RAG_BLOCKING_STARTUP_WARMUP=false
# Heroku-style stacks sometimes honor this; harmless elsewhere.
ENV WEB_CONCURRENCY=1

# Match platform listener; FastAPI Cloud and most hosts set ``PORT`` (default 8080).
EXPOSE 8080
# Single worker: in-memory LLM state must not be split across processes.
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]
