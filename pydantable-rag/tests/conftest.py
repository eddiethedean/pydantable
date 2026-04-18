"""Pytest loads this first — disable heavy startup before ``app`` imports."""

from __future__ import annotations

import os

# FastAPI TestClient runs lifespan; defaults would download HF models and ingest.
os.environ.setdefault("RAG_AUTO_INGEST_ON_STARTUP", "false")
os.environ.setdefault("RAG_PRELOAD_MODELS_ON_STARTUP", "false")
os.environ.setdefault("RAG_WARM_LLM_WHEN_INDEX_READY", "false")
os.environ.setdefault("RAG_BLOCKING_STARTUP_WARMUP", "false")
os.environ.setdefault("RAG_FETCH_UPSTREAM_DOCS", "false")
os.environ.setdefault("RAG_LLM_BACKEND", "hf")
