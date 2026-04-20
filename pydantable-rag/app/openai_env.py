"""Shared OpenAI API key detection (embeddings + optional chat completions)."""

from __future__ import annotations


def openai_api_key_configured() -> bool:
    import os

    return bool(os.getenv("OPENAI_API_KEY", "").strip())
