from __future__ import annotations
 
from pathlib import Path
 
from fastapi import FastAPI
from pydantic import BaseModel, Field
 
from app.rag.chunking import chunk_text, read_text_file
from app.rag.embeddings import get_embedder
from app.rag.llm import ChatMessage
from app.rag.pipeline import rag_chat
from app.rag.store import reset_db, upsert_chunks
from app.settings import get_settings, resolve_db_path
 
 
app = FastAPI(title="pydantable-rag")
 
 
class ChatRequest(BaseModel):
     message: str = Field(min_length=1)
     history: list[ChatMessage] | None = None
 
 
class ChatResponse(BaseModel):
     answer: str
     sources: list[dict]
 
 
class IngestRequest(BaseModel):
     paths: list[str] | None = None
 
 
@app.get("/health")
def health() -> dict:
     s = get_settings()
     dbp = resolve_db_path(s.db_path)
     return {"ok": True, "db_path": str(dbp)}
 
 
def _default_ingest_paths(repo_root: Path) -> list[Path]:
     out: list[Path] = []
     for p in [repo_root / "README.md", repo_root / "posts", repo_root / "docs"]:
         if p.exists():
             out.append(p)
     return out
 
 
def _expand_paths(paths: list[Path]) -> list[Path]:
     files: list[Path] = []
     for p in paths:
         if p.is_dir():
             for ext in ("*.md", "*.txt", "*.rst"):
                 files.extend(sorted(p.rglob(ext)))
         elif p.is_file():
             files.append(p)
     return files
 
 
@app.post("/ingest")
def ingest(req: IngestRequest) -> dict:
     s = get_settings()
     repo_root = (Path(__file__).resolve().parents[2]).resolve()
     db_path = resolve_db_path(s.db_path)
 
     roots = (
         [repo_root / p for p in req.paths] if req.paths else _default_ingest_paths(repo_root)
     )
     files = _expand_paths(roots)
 
     reset_db(db_path)
 
     embedder = get_embedder(s.embed_model, s.embed_dims)
     all_rows: list[tuple[str, str, str]] = []
     all_texts: list[str] = []
 
     for fp in files:
         rel = str(fp.relative_to(repo_root))
         text = read_text_file(fp)
         chunks = chunk_text(
             source=rel,
             text=text,
             chunk_chars=s.chunk_chars,
             overlap_chars=s.chunk_overlap_chars,
         )
         for c in chunks:
             all_rows.append((c.chunk_id, c.source, c.text))
             all_texts.append(c.text)
 
     embeddings = embedder.embed(all_texts)
     upsert_chunks(
         db_path=db_path, dims=s.embed_dims, chunks=all_rows, embeddings=embeddings
     )
 
     return {"ok": True, "files": len(files), "chunks": len(all_rows), "db_path": str(db_path)}
 
 
@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
     s = get_settings()
     db_path = resolve_db_path(s.db_path)
 
     result = rag_chat(
         question=req.message,
         db_path=str(db_path),
         embed_model=s.embed_model,
         embed_dims=s.embed_dims,
         top_k=s.top_k,
         ollama_model=s.ollama_model,
         chat_history=req.history,
     )
 
     return ChatResponse(
         answer=result.answer,
         sources=[
             {
                 "source": c.source,
                 "chunk_id": c.chunk_id,
                 "distance": c.distance,
             }
             for c in result.retrieved
         ],
     )
