# Local / CI image similar to FastAPI Cloud: app root at /app, import main:app, port 8080.
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

COPY pyproject.toml README.md main.py ./
COPY app ./app
COPY scripts ./scripts
COPY data/ /app/data/

RUN mkdir -p /app/data \
    && uv venv /app/.venv \
    && uv pip install --python /app/.venv/bin/python -e .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV WEB_CONCURRENCY=1
ENV RAG_LLM_BACKEND=extractive
ENV RAG_BLOCKING_STARTUP_WARMUP=false

EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]
