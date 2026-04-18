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
- `GET /readyz` (true once the vector DB has chunks **and** the LLM is loaded)
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

### Production: CI-built vector database (recommended)

Building the embedding index **on GitHub Actions** avoids doing heavy Hugging Face work on FastAPI Cloud (memory limits, cold start, multi-replica SQLite issues).

- Workflow **`.github/workflows/rag-database.yml`** runs `pydantable-rag/scripts/build_index_ci.py`, which indexes the monorepo **`docs/`** tree and writes **`pydantable-rag/data/pydantable_vectors.db`**. It uploads that file as a workflow artifact.
- **`.github/workflows/fastapi-cloud-deploy.yml`** builds the same DB, then **downloads it into `pydantable-rag/data/`** before `fastapi deploy`, so the uploaded package **includes the prebuilt SQLite file** (`.gitignore` allows this path when the file is present).
- Add repository secret **`HF_TOKEN`** so CI can download embedding and (if needed) other models reliably.

For **manual** deploys: run the **RAG vector database** workflow (or build locally with `uv run python scripts/build_index_ci.py` from `pydantable-rag/` with the monorepo checked out), download the artifact, place **`data/pydantable_vectors.db`** under `pydantable-rag/`, then `fastapi deploy`. To **bake Hugging Face weights into the image** (recommended), also run `uv run python scripts/prep_hf_cache_ci.py` so `pydantable-rag/hf_baked/` is populated before deploy (same as the **Deploy pydantable-rag** workflow).

At runtime, **`POST /bootstrap`** only needs to **warm the LLM** if you ship a ready DB (ingest can be skipped). Optional: set **`RAG_AUTO_INGEST_ON_STARTUP=false`** (default) and avoid re-ingesting on each boot.

### FastAPI Cloud

- **Set `HF_TOKEN` on the app** (Dashboard → your app → Environment / variables) if you rely on **runtime** Hub access (e.g. missing baked cache, private models). The repo’s **Deploy pydantable-rag** workflow runs **`scripts/prep_hf_cache_ci.py`** in CI (with GitHub **`HF_TOKEN`**) and ships snapshots under **`hf_baked/`** inside the Docker image, so replicas load **`transformers`** weights from disk and do **not** re-download from Hugging Face on every boot. GitHub’s secret still only applies to CI unless you set **`HF_TOKEN`** on the app.
- **Replicas and RAM:** Each instance loads its **own** LLM in memory. The image defaults **`RAG_WARM_LLM_WHEN_INDEX_READY=false`** so small plans do not **OOM** and crash-loop when several replicas all start `torch` at once. Use **min replicas 1** and **`POST /bootstrap`** to warm the LLM, or set **`RAG_WARM_LLM_WHEN_INDEX_READY=true`** only when **each** replica has enough memory. The Dockerfile sets **`OMP_NUM_THREADS=1`** (and related) plus app **`configure_torch_cpu()`** to limit CPU thread overhead.
- This project includes `fastapi[standard]`, so the **FastAPI Cloud CLI** is available.
- Deploy from the `pydantable-rag/` directory (after placing a CI-built DB as above, or use the repo’s deploy workflow):

```bash
fastapi deploy
```

- The **Dockerfile** copies a **baked Hub cache** to **`HF_HOME=/app/.cache/huggingface`**, sets **`RAG_PRELOAD_MODELS_ON_STARTUP=false`**, **`RAG_WARM_LLM_WHEN_INDEX_READY=false`**, **`RAG_BLOCKING_STARTUP_WARMUP=false`**, and single-thread BLAS/torch env vars. **`WEB_CONCURRENCY=1`**, **uvicorn `--workers 1`**. **`POST /chat`** returns **503** until the LLM is ready on that replica. Ship the **CI-built DB** and **CI-baked `hf_baked/`**. Use **`POST /bootstrap`** to queue a warm when startup warm is off. Use **`GET /diag`** (`llm_last_error`) if the LLM fails to load.
- Use a **writable** `RAG_DB_PATH` on **shared storage** if you run **multiple replicas**; otherwise prefer **one replica** for SQLite.
- Typical env vars (many match built-in defaults):

  - `RAG_DB_PATH=data/pydantable_vectors.db` (must be writable)
  - `RAG_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2`
  - `RAG_EMBED_DIMS=384`
  - `RAG_LLM_MODEL=HuggingFaceTB/SmolLM2-135M-Instruct`

- Health endpoints:
  - `GET /healthz`
  - `GET /readyz`
