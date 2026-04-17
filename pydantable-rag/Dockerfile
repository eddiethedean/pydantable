# Local / CI image similar to FastAPI Cloud: app root at /app, import main:app, port 8080.
# CPU PyTorch via tool.uv.sources in pyproject.toml (same as cloud uv install).
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md main.py ./
COPY app ./app

# Editable install pulls deps (torch from pytorch-cpu index per pyproject).
RUN uv venv /app/.venv \
    && uv pip install --python /app/.venv/bin/python -e .

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Match platform listener; FastAPI Cloud and most hosts set ``PORT`` (default 8080).
EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
