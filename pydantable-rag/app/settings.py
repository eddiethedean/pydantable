from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel

class Settings(BaseModel):
     db_path: str = "data/pydantable_vectors.db"
     ollama_model: str = "qwen3:4b"
     embed_model: str = "google/embeddinggemma-300m"
     embed_dims: int = 768
     chunk_chars: int = 4000
     chunk_overlap_chars: int = 400
     top_k: int = 6

def get_settings() -> Settings:
     load_dotenv()
     import os
 
     return Settings(
         db_path=os.getenv("RAG_DB_PATH", Settings().db_path),
         ollama_model=os.getenv("RAG_OLLAMA_MODEL", Settings().ollama_model),
         embed_model=os.getenv("RAG_EMBED_MODEL", Settings().embed_model),
         embed_dims=int(os.getenv("RAG_EMBED_DIMS", str(Settings().embed_dims))),
         chunk_chars=int(os.getenv("RAG_CHUNK_CHARS", str(Settings().chunk_chars))),
         chunk_overlap_chars=int(
             os.getenv("RAG_CHUNK_OVERLAP_CHARS", str(Settings().chunk_overlap_chars))
         ),
         top_k=int(os.getenv("RAG_TOP_K", str(Settings().top_k))),
     )

def resolve_db_path(db_path: str) -> Path:
     p = Path(db_path)
     if p.is_absolute():
         return p
     return (Path(__file__).resolve().parent.parent / p).resolve()
