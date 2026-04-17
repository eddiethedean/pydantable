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


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    import sqlite_vec

    sqlite_vec.load(conn)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db(db_path: Path, dims: int) -> None:
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
        conn.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS docs_vec USING vec0(
              chunk_id TEXT PRIMARY KEY,
              embedding FLOAT[{dims}]
            );
            """
        )


def reset_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS docs;")
        conn.execute("DROP TABLE IF EXISTS docs_vec;")


def upsert_chunks(
    *,
    db_path: Path,
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
            "INSERT OR REPLACE INTO docs_vec(chunk_id, embedding) VALUES(?, ?);",
            [(cid, embeddings[i].tolist()) for i, (cid, _src, _txt) in enumerate(chunks)],
        )


def search(*, db_path: Path, query_embedding: np.ndarray, top_k: int) -> list[RetrievedChunk]:
    if query_embedding.ndim != 1:
        raise ValueError("query_embedding must be 1D")

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              d.source AS source,
              d.chunk_id AS chunk_id,
              d.text AS text,
              v.distance AS distance
            FROM docs_vec v
            JOIN docs d ON d.chunk_id = v.chunk_id
            WHERE v.embedding MATCH ?
            ORDER BY v.distance
            LIMIT ?;
            """,
            (query_embedding.astype(np.float32).tolist(), int(top_k)),
        ).fetchall()

    return [
        RetrievedChunk(
            source=r["source"],
            chunk_id=r["chunk_id"],
            text=r["text"],
            distance=float(r["distance"]),
        )
        for r in rows
    ]
