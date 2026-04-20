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

The **vector index** is the committed file **`data/pydantable_vectors.db`**. Rebuild it locally when docs change (one-time cost for embeddings), then commit:

```bash
cd pydantable-rag
# from repo root; needs OPENAI_API_KEY in the environment
uv run python scripts/build_index_ci.py
git add data/pydantable_vectors.db && git commit -m "chore(rag): refresh vector index"
```

- **`.github/workflows/fastapi-cloud-deploy.yml`** checks that the DB is present and deploys with **`fastapi deploy`** (no OpenAI call in GitHub Actions).
- **`.github/workflows/rag-database.yml`** verifies the committed DB on push/PR; use **Actions → RAG vector database → Run workflow** if you ever want a **CI rebuild** (requires repository secret **`OPENAI_API_KEY`**) and download the artifact.

Set **`OPENAI_API_KEY`** on the FastAPI Cloud app for **runtime** (query embeddings on every request, and chat if `RAG_LLM_BACKEND=openai`).

### Generative vs extractive

- **`RAG_LLM_BACKEND=extractive`** (default): return ranked doc excerpts only.
- **`RAG_LLM_BACKEND=openai`**: synthesize answers with `RAG_LLM_MODEL` (default `gpt-5.4-nano`).

Both modes require **`OPENAI_API_KEY`** for query embeddings.
