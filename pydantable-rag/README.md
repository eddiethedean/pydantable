 ## pydantable-rag
 
 FastAPI backend for a **100% local** RAG assistant that can answer questions about how to use **pydantable**.
 
 Inspired by [`private-rag-embeddinggemma`](https://github.com/LLM-Implementation/private-rag-embeddinggemma).
 
 ### Prereqs
 
 - **Python** 3.10+
 - **Ollama** installed and running
 - (Optional) Hugging Face access for EmbeddingGemma if you choose to use it
 
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
 
 Create a `.env` in `pydantable-rag/` (or export env vars):
 
 - `RAG_DB_PATH` (default: `data/pydantable_vectors.db`)
 - `RAG_OLLAMA_MODEL` (default: `qwen3:4b`)
 - `RAG_EMBED_MODEL` (default: `google/embeddinggemma-300m`)
 - `RAG_EMBED_DIMS` (default: `768`)
 
 ### Ingest docs into SQLite-vec
 
 From `pydantable-rag/`:
 
 ```bash
 python scripts/ingest.py
 ```
 
 ### Run the API
 
 ```bash
 uvicorn app.main:app --reload --port 8000
 ```
 
 ### Endpoints
 
 - `GET /health`
 - `POST /chat` (retrieval + local LLM)
 - `POST /ingest` (rebuild index; useful for dev)
