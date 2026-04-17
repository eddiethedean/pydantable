 ## pydantable-rag
 
 FastAPI backend for a **100% local** RAG assistant that can answer questions about how to use **pydantable**.
 
 Inspired by [`private-rag-embeddinggemma`](https://github.com/LLM-Implementation/private-rag-embeddinggemma).
 
 ### Prereqs
 
 - **Python** 3.10+
 - Runs **natively in-process** using Transformers (no Ollama required).
 
 ### Setup
 
 From your repo root:
 
 ```bash
 cd pydantable-rag
 python -m venv .venv
 source .venv/bin/activate
 pip install -U pip
 pip install -e .
 ```
 
 ### Configure
 
Copy `.env.example` to `.env` and adjust (or export env vars):

- `RAG_DB_PATH` (default: `data/pydantable_vectors.db`)
- `RAG_EMBED_MODEL` (default: `sentence-transformers/all-MiniLM-L6-v2`)
- `RAG_EMBED_DIMS` (default: `384`)
- `RAG_LLM_MODEL` (default: `HuggingFaceTB/SmolLM2-135M-Instruct`)
- `RAG_PRELOAD_MODELS_ON_STARTUP=true` (recommended for FastAPI Cloud to avoid 502s on cold start)
 
 ### Ingest docs into SQLite-vec
 
 From `pydantable-rag/`:
 
 ```bash
 python scripts/ingest.py
 ```

By default this ingests your repo’s `README.md` and `docs/` directory (it does **not** ingest `posts/`).
 
 ### Run the API
 
 ```bash
 uvicorn app.main:app --reload --port 8000
 ```
 
 ### Endpoints
 
- `GET /healthz`
- `GET /readyz` (true once the vector DB has chunks)
 - `POST /chat` (retrieval + local LLM)
 - `POST /ingest` (rebuild index; useful for dev)

### Local Docker (simulate cloud)

The image uses **`/app`**, **`main:app`**, and port **8080** like FastAPI Cloud. First download is slow (HF models).

```bash
cd pydantable-rag
docker compose build
docker compose up
# http://localhost:8080/healthz
```

Pass the same env vars as production (e.g. `RAG_PRELOAD_MODELS_ON_STARTUP=true`) via `docker compose run` / Compose `environment:` or a file. To ingest the **monorepo** `docs/` from the parent repo, bind-mount and set `RAG_REPO_ROOT`, for example:

`docker compose run --rm -v "$(pwd)/../docs:/app/docs:ro" -e RAG_REPO_ROOT=/app rag`

(Optional: uncomment `deploy.resources.limits.memory` in `docker-compose.yml` to mimic a small instance.)

### FastAPI Cloud

- This project includes `fastapi[standard]`, so the **FastAPI Cloud CLI** is available.
- Deploy from the `pydantable-rag/` directory:

```bash
fastapi deploy
```

- Set these env vars in FastAPI Cloud:
  - `RAG_AUTO_INGEST_ON_STARTUP=true`
  - `RAG_DB_PATH=data/pydantable_vectors.db` (default; must be writable)
  - `RAG_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2`
  - `RAG_EMBED_DIMS=384`
  - `RAG_LLM_MODEL=HuggingFaceTB/SmolLM2-135M-Instruct`
  - `RAG_PRELOAD_MODELS_ON_STARTUP=true`

- Health endpoints:
  - `GET /healthz`
  - `GET /readyz`
