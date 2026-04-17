#!/usr/bin/env python3
"""Download pydantable README + docs/ from GitHub into bundled/pydantable/."""

from __future__ import annotations

import sys
from pathlib import Path

# Project root = parent of scripts/
_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    from app.rag.upstream_fetch import bundled_pydantable_root, fetch_upstream_docs

    ok = fetch_upstream_docs(repo_root=_ROOT)
    dest = bundled_pydantable_root(_ROOT)
    print(f"fetch_upstream_docs: {'ok' if ok else 'failed'} -> {dest}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
