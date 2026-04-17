from __future__ import annotations
 
from dataclasses import dataclass
from pathlib import Path
 

@dataclass(frozen=True)
class Chunk:
     source: str
     chunk_id: str
     text: str
 

def chunk_text(
     *, source: str, text: str, chunk_chars: int, overlap_chars: int
 ) -> list[Chunk]:
     cleaned = text.replace("\r\n", "\n").replace("\r", "\n").strip()
     if not cleaned:
         return []
 
     step = max(1, chunk_chars - overlap_chars)
     chunks: list[Chunk] = []
     i = 0
     n = len(cleaned)
     while i < n:
         j = min(n, i + chunk_chars)
         chunk = cleaned[i:j].strip()
         if chunk:
             chunks.append(
                 Chunk(source=source, chunk_id=f"{source}::c{len(chunks)}", text=chunk)
             )
         i += step
     return chunks
 

def read_text_file(path: Path) -> str:
     return path.read_text(encoding="utf-8", errors="ignore")
