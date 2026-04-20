"""Pytest loads this first — tame startup before ``app`` imports."""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest

os.environ.setdefault("RAG_AUTO_INGEST_ON_STARTUP", "false")
os.environ.setdefault("RAG_BLOCKING_STARTUP_WARMUP", "false")
os.environ.setdefault("RAG_FETCH_UPSTREAM_DOCS", "false")
os.environ.setdefault("RAG_LLM_BACKEND", "extractive")


@pytest.fixture(autouse=True)
def _disable_rate_limiting() -> Generator[None, None, None]:
    """Avoid flaky tests and 429s when the suite hammers endpoints."""
    import app.main as main

    lim = main.app.state.limiter
    prev = lim.enabled
    lim.enabled = False
    yield
    lim.enabled = prev
