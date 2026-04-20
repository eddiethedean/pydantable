from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from app.rag.chunking import chunk_text, read_text_file
from app.rag.embeddings import get_embedder
from app.rag.store import reset_db, upsert_chunks
from app.rag.upstream_fetch import (
    bundled_pydantable_root,
    ensure_upstream_bundle,
)
from app.settings import Settings, resolve_db_path


@dataclass(frozen=True)
class IngestResult:
    files: int
    chunks: int
    db_path: str


def default_ingest_roots(doc_root: Path, *, service_root: Path) -> list[Path]:
    """
    Default paths to index: library ``README.md`` + ``docs/``, and—when this service
    lives in a monorepo—``pydantable-rag/README.md`` (not the same file as the
    repo root README).
    """
    out: list[Path] = []
    for p in (doc_root / "README.md", doc_root / "docs"):
        if p.exists():
            out.append(p)

    svc_readme = service_root / "README.md"
    if not svc_readme.is_file():
        return out
    try:
        svc_resolved = svc_readme.resolve()
        svc_resolved.relative_to(doc_root.resolve())
    except ValueError:
        return out
    if svc_resolved == (doc_root / "README.md").resolve():
        return out
    out.append(svc_readme)
    return out


def _service_root() -> Path:
    # app/rag/ingest.py -> parents[2] == pydantable-rag project root
    return Path(__file__).resolve().parents[2]


def _resolve_doc_root(
    *,
    service_root: Path,
    repo_root: Path,
    paths: list[str] | None,
) -> tuple[Path, list[Path]]:
    """
    Choose markdown root and path list for ingest.

    Prefer GitHub-bundled ``bundled/pydantable`` when present (or after fetch).
    Otherwise use ``repo_root`` (monorepo parent ``docs/`` or service README).
    """
    if paths is not None:
        roots = [repo_root / p for p in paths]
        return repo_root, roots

    if not (repo_root / "docs").is_dir():
        ensure_upstream_bundle(service_root)

    bundled = bundled_pydantable_root(service_root)
    if (bundled / "docs").is_dir():
        doc_root = bundled
    else:
        doc_root = repo_root

    return doc_root, default_ingest_roots(doc_root, service_root=service_root)


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

    service_root = _service_root()
    doc_root, roots = _resolve_doc_root(
        service_root=service_root, repo_root=repo_root, paths=paths
    )
    files = expand_paths(roots)
    if not files:
        # Do not reset an existing shipped index when there is nothing to ingest
        # (e.g. minimal cloud image without monorepo ``docs/``).
        return IngestResult(files=0, chunks=0, db_path=str(db_path))

    with _ingest_lock(db_path):
        reset_db(db_path)

        all_rows: list[tuple[str, str, str]] = []
        all_texts: list[str] = []

        for fp in files:
            rel = str(fp.relative_to(doc_root))
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

        # Avoid calling the embedding API when there is nothing to embed (empty
        # deploy or missing README/docs). Saves RAM so bootstrap can load the LLM.
        if not all_texts:
            embeddings = np.zeros((0, settings.embed_dims), dtype=np.float32)
        else:
            embedder = get_embedder(settings.embed_model, settings.embed_dims)
            embeddings = embedder.embed(all_texts)

        upsert_chunks(
            db_path=db_path,
            dims=settings.embed_dims,
            chunks=all_rows,
            embeddings=embeddings,
        )
    return IngestResult(files=len(files), chunks=len(all_rows), db_path=str(db_path))
