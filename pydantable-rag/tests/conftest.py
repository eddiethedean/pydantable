"""Pytest loads this first — tame startup before ``app`` imports."""

from __future__ import annotations

import os

os.environ.setdefault("RAG_AUTO_INGEST_ON_STARTUP", "false")
os.environ.setdefault("RAG_BLOCKING_STARTUP_WARMUP", "false")
os.environ.setdefault("RAG_FETCH_UPSTREAM_DOCS", "false")
os.environ.setdefault("RAG_LLM_BACKEND", "extractive")
