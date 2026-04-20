"""Tests for extractive (no-LLM) RAG answer formatting."""

from __future__ import annotations

from app.rag.pipeline import _clean_chunk_text, _extractive_answer, _skip_chunk_source
from app.rag.store import RetrievedChunk


def test_skip_chunk_source_drops_rtd_build_mirrors() -> None:
    assert _skip_chunk_source("docs/_build/html/foo.md") is True
    assert _skip_chunk_source("docs/getting-started/quickstart.md") is False


def test_extractive_empty() -> None:
    out = _extractive_answer("hello", [])
    assert "No matching documentation chunks" in out


def test_extractive_formats_markdown_sections() -> None:
    chunks = [
        RetrievedChunk(
            source="docs/a.md",
            chunk_id="docs/a.md::c0",
            text="# Title\n\nHello",
            distance=0.1,
        ),
        RetrievedChunk(
            source="docs/b.md",
            chunk_id="docs/b.md::c0",
            text="More",
            distance=0.2,
        ),
    ]
    out = _extractive_answer("q", chunks)
    assert "closest matching excerpts" in out
    assert "### 1. `docs/a.md`" in out
    assert "### 2. `docs/b.md`" in out
    assert "docs/a.md::c0" not in out
    assert "0.1000" not in out  # distances live in API sources, not body


def test_extractive_prefers_non_build_sources() -> None:
    good = RetrievedChunk(
        source="docs/guide.md",
        chunk_id="x",
        text="from pydantable import DataFrame",
        distance=0.5,
    )
    build = RetrievedChunk(
        source="docs/_build/html/_sources/X.md.txt",
        chunk_id="y",
        text="duplicate",
        distance=0.1,
    )
    out = _extractive_answer("q", [build, good])
    assert "docs/guide.md" in out
    assert "_build" not in out


def test_clean_chunk_replaces_mkdocstrings_directive() -> None:
    raw = "# DataFrame\n\n::: pydantable.DataFrame\n"
    assert "::: pydantable.DataFrame" not in _clean_chunk_text(raw)
    assert "pydantable.DataFrame" in _clean_chunk_text(raw)
