from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass(frozen=True)
class RetrievedChunk:
    source: str
    chunk_id: str
    text: str
    distance: float


def _as_path(db_path: str | Path) -> Path:
    return db_path if isinstance(db_path, Path) else Path(db_path)


def _connect(db_path: str | Path) -> sqlite3.Connection:
    db_path = _as_path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path, dims: int) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
              chunk_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              text TEXT NOT NULL
            );
            """
        )
        # Pure-Python vector backend (portable to hosted environments).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs_emb (
              chunk_id TEXT PRIMARY KEY,
              embedding BLOB NOT NULL
            );
            """
        )


def reset_db(db_path: str | Path) -> None:
    with _connect(db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS docs;")
        conn.execute("DROP TABLE IF EXISTS docs_emb;")


def upsert_chunks(
    *,
    db_path: str | Path,
    dims: int,
    chunks: list[tuple[str, str, str]],
    embeddings: np.ndarray,
) -> None:
    if embeddings.dtype != np.float32:
        embeddings = embeddings.astype(np.float32)
    if embeddings.shape[0] != len(chunks):
        raise ValueError("embeddings row count must match chunk count")
    if embeddings.shape[1] != dims:
        raise ValueError(f"embeddings dim {embeddings.shape[1]} != {dims}")

    init_db(db_path, dims)

    with _connect(db_path) as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO docs(chunk_id, source, text) VALUES(?, ?, ?);",
            chunks,
        )
        conn.executemany(
            "INSERT OR REPLACE INTO docs_emb(chunk_id, embedding) VALUES(?, ?);",
            [
                (cid, sqlite3.Binary(embeddings[i].tobytes()))
                for i, (cid, _src, _txt) in enumerate(chunks)
            ],
        )


def search(*, db_path: Path, query_embedding: np.ndarray, top_k: int) -> list[RetrievedChunk]:
    if query_embedding.ndim != 1:
        raise ValueError("query_embedding must be 1D")

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT d.source AS source, d.chunk_id AS chunk_id, d.text AS text, e.embedding AS embedding
            FROM docs_emb e
            JOIN docs d ON d.chunk_id = e.chunk_id;
            """
        ).fetchall()

    if not rows:
        return []

    embs = np.vstack(
        [
            np.frombuffer(r["embedding"], dtype=np.float32, count=query_embedding.size)
            for r in rows
        ]
    )
    # embeddings are normalized; cosine distance = 1 - dot
    q = query_embedding.astype(np.float32)
    sims = embs @ q
    dists = 1.0 - sims
    idx = np.argsort(dists)[: int(top_k)]
    out: list[RetrievedChunk] = []
    for i in idx:
        r = rows[int(i)]
        out.append(
            RetrievedChunk(
                source=r["source"],
                chunk_id=r["chunk_id"],
                text=r["text"],
                distance=float(dists[int(i)]),
            )
        )
    return out


def get_counts(*, db_path: str | Path) -> dict:
    try:
        with _connect(db_path) as conn:
            docs = conn.execute("SELECT COUNT(1) AS n FROM docs;").fetchone()["n"]
            vecs = conn.execute("SELECT COUNT(1) AS n FROM docs_emb;").fetchone()["n"]
        return {"docs": int(docs), "vecs": int(vecs), "backend": "py"}
    except sqlite3.OperationalError as e:
        # Common on first boot before init_db runs.
        msg = str(e).lower()
        if "no such table" in msg:
            return {"docs": 0, "vecs": 0, "backend": "py", "uninitialized": True}
        return {"docs": 0, "vecs": 0, "error": f"OperationalError: {e}"}
    except Exception as e:
        return {"docs": 0, "vecs": 0, "error": f"{type(e).__name__}: {e}"}


def check_vector_backend(*, db_path: str | Path) -> dict:
    """
    Minimal diagnostic to surface sqlite-vec import/load issues in hosted envs.
    """
    try:
        with _connect(db_path):
            pass
        return {"ok": True, "backend": "py"}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
