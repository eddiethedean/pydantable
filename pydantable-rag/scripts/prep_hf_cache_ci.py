#!/usr/bin/env python3
"""
Populate ``hf_baked/`` with Hugging Face Hub snapshots for the embedding + chat
models. Used in CI before ``docker build`` / ``fastapi deploy`` so the image
carries weights and runtimes do not hammer the Hub on every replica boot.

Layout matches ``HF_HOME`` + ``HF_HUB_CACHE`` (``hub/``) used by ``transformers``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def main() -> int:
    rag = Path(__file__).resolve().parents[1]
    hf_home = rag / "hf_baked"
    hub = hf_home / "hub"
    hf_home.mkdir(parents=True, exist_ok=True)
    hub.mkdir(parents=True, exist_ok=True)

    os.environ["HF_HOME"] = str(hf_home)
    os.environ["HF_HUB_CACHE"] = str(hub)
    os.environ["TRANSFORMERS_CACHE"] = str(hub)

    os.environ.setdefault("RAG_FETCH_UPSTREAM_DOCS", "false")

    from huggingface_hub import snapshot_download

    from app.settings import get_settings

    s = get_settings()
    token = os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")

    for repo_id in (s.embed_model, s.llm_model):
        print(f"hf cache: snapshot_download {repo_id!r}")
        snapshot_download(repo_id, token=token)

    print(f"hf cache: done -> {hf_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
