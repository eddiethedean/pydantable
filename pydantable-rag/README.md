## pydantable-rag

FastAPI backend for a **documentation-grounded** assistant for **pydantable**. Retrieval uses **OpenAI embeddings** (`text-embedding-3-*` by default); optional answers use **OpenAI chat completions** when `RAG_LLM_BACKEND=openai`.

### Prerequisites

- Python 3.10+
- An **[OpenAI API key](https://platform.openai.com/api-keys)** (`OPENAI_API_KEY`) for embeddings and (if enabled) chat

### Setup

```bash
cd pydantable-rag
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Copy `.env.example` to `.env` and set at least **`OPENAI_API_KEY`**. See the file for `RAG_EMBED_MODEL`, `RAG_EMBED_DIMS`, `RAG_LLM_BACKEND`, etc.

**Breaking change (v0.3+):** Embeddings use the OpenAI API only (no Hugging Face / PyTorch). Re-build `data/pydantable_vectors.db` after upgrading if your index used the old 384-dim MiniLM vectors.

### Ingest docs

From `pydantable-rag/`:

```bash
python scripts/ingest.py
```

By default this indexes the repo `README.md` and `docs/` (see `resolve_ingest_repo_root` for monorepo layout).

### Run the API

```bash
uvicorn main:app --reload --port 8000
```

### Endpoints

- `GET /healthz` · `GET /readyz` · `GET /diag`
- `POST /chat` — retrieval (+ OpenAI answer if `RAG_LLM_BACKEND=openai`)
- `POST /ingest` — rebuild index
- `GET /chat-app` — minimal browser UI

### Docker

```bash
docker compose build
docker compose up
# http://localhost:8080/healthz
```

Set `OPENAI_API_KEY` (and optional `RAG_*`) via Compose `environment` or an env file.

### CI and FastAPI Cloud

- **`.github/workflows/rag-database.yml`** builds `data/pydantable_vectors.db` using **`OPENAI_API_KEY`** from repository secrets.
- **`.github/workflows/fastapi-cloud-deploy.yml`** builds the same DB and deploys with **`fastapi deploy`**.

Set **`OPENAI_API_KEY`** on the FastAPI Cloud app for runtime (embeddings on every query + chat if enabled).

### Generative vs extractive

- **`RAG_LLM_BACKEND=extractive`** (default): return ranked doc excerpts only.
- **`RAG_LLM_BACKEND=openai`**: synthesize answers with `RAG_LLM_MODEL` (default `gpt-5.4-nano`).

Both modes require **`OPENAI_API_KEY`** for query embeddings.
