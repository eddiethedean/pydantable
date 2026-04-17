from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from pathlib import Path

from app.rag.chunking import chunk_text, read_text_file
from app.rag.embeddings import get_embedder
from app.rag.store import reset_db, upsert_chunks
from app.settings import Settings, resolve_db_path


@dataclass(frozen=True)
class IngestResult:
    files: int
    chunks: int
    db_path: str


def default_ingest_roots(repo_root: Path) -> list[Path]:
    out: list[Path] = []
    for p in [repo_root / "README.md", repo_root / "docs"]:
        if p.exists():
            out.append(p)
    return out


@contextlib.contextmanager
def _ingest_lock(db_path: Path):
    """
    Best-effort cross-process lock when several replicas share the same DB path
    (e.g. mounted volume). Skipped on Windows (local dev).
    """
    if sys.platform == "win32":
        yield
        return
    import fcntl

    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.parent / ".rag_ingest.lock"
    with open(lock_path, "a", encoding="utf-8") as fp:
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)


def expand_paths(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            for ext in ("*.md", "*.txt", "*.rst"):
                files.extend(sorted(p.rglob(ext)))
        elif p.is_file():
            files.append(p)
    return files


def ingest_repo_docs(
    *, settings: Settings, repo_root: Path, paths: list[str] | None
) -> IngestResult:
    db_path = resolve_db_path(settings.db_path)

    roots = [repo_root / p for p in paths] if paths else default_ingest_roots(repo_root)
    files = expand_paths(roots)

    with _ingest_lock(db_path):
        reset_db(db_path)

        embedder = get_embedder(settings.embed_model, settings.embed_dims)

        all_rows: list[tuple[str, str, str]] = []
        all_texts: list[str] = []

        for fp in files:
            rel = str(fp.relative_to(repo_root))
            text = read_text_file(fp)
            chunks = chunk_text(
                source=rel,
                text=text,
                chunk_chars=settings.chunk_chars,
                overlap_chars=settings.chunk_overlap_chars,
            )
            for c in chunks:
                all_rows.append((c.chunk_id, c.source, c.text))
                all_texts.append(c.text)

        embeddings = embedder.embed(all_texts)
        upsert_chunks(
            db_path=db_path,
            dims=settings.embed_dims,
            chunks=all_rows,
            embeddings=embeddings,
        )
    return IngestResult(files=len(files), chunks=len(all_rows), db_path=str(db_path))
