#!/usr/bin/env python3
"""
Build ``data/pydantable_vectors.db`` in CI from the monorepo ``docs/`` tree.

Expects checkout at repo root (parent of ``pydantable-rag/``). Sets
``RAG_REPO_ROOT`` to that root so ingest uses ``../docs`` without fetching
from GitHub.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_RAG_ROOT = Path(__file__).resolve().parents[1]
_REPO_ROOT = _RAG_ROOT.parent


def main() -> int:
    os.environ.setdefault("RAG_FETCH_UPSTREAM_DOCS", "false")
    os.environ["RAG_REPO_ROOT"] = str(_REPO_ROOT)
    db = _RAG_ROOT / "data" / "pydantable_vectors.db"
    os.environ.setdefault("RAG_DB_PATH", str(db))

    (db.parent).mkdir(parents=True, exist_ok=True)

    from app.rag.ingest import ingest_repo_docs
    from app.settings import get_settings

    s = get_settings()
    res = ingest_repo_docs(settings=s, repo_root=_REPO_ROOT, paths=None)
    print(f"ingest: files={res.files} chunks={res.chunks} db={res.db_path}")
    if res.files == 0:
        print(
            "error: no source files ingested (check docs/ and RAG_REPO_ROOT)",
            file=sys.stderr,
        )
        return 1
    if res.chunks == 0:
        print("error: no chunks produced", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
