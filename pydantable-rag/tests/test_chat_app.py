"""Tests for ``GET /chat-app`` (browser chat shell)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

# Structural anchors: if the UI template changes, update this list intentionally.
_CHAT_APP_REQUIRED_SUBSTRINGS: tuple[str, ...] = (
    "<!DOCTYPE html>",
    '<div id="app">',
    '<main id="thread">',
    '<div id="empty">',
    '<div id="messages"',
    '<textarea id="q"',
    'id="send"',
    'id="clear"',
    'id="starters"',
    'fetch("/chat"',
    'method: "POST"',
    "application/json",
    "POST /chat",
    "JSON.stringify",
    "What is pydantable?",
    "wireStarters",
    "aria-live",
    "renderAssistantMarkdown",
    "wrapCodeBlocksInBubble",
    "copyToClipboard",
    "applySyntaxHighlight",
    "highlight.min.js",
    "marked.min.js",
)


@pytest.fixture()
def chat_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Same lightweight stubs as ``test_api.client`` so app import stays cheap."""
    import app.main as main

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("RAG_LLM_BACKEND", "extractive")
    monkeypatch.setattr(main, "embed_deployment_ready", lambda _m, _d: True)
    monkeypatch.setattr(main, "embedder_is_loading", lambda _m, _d: False)
    monkeypatch.setattr(main, "embedding_compute_active", lambda: False)
    monkeypatch.setattr(main, "ingest_repo_docs", lambda **_kwargs: None)
    return TestClient(main.app)


def test_chat_app_get_returns_html_utf8(chat_client: TestClient) -> None:
    res = chat_client.get("/chat-app")
    assert res.status_code == 200
    ct = res.headers.get("content-type", "")
    assert "text/html" in ct
    assert "utf-8" in ct.lower()


@pytest.mark.parametrize("fragment", _CHAT_APP_REQUIRED_SUBSTRINGS)
def test_chat_app_contains_required_fragments(
    chat_client: TestClient, fragment: str
) -> None:
    res = chat_client.get("/chat-app")
    assert res.status_code == 200
    assert fragment in res.text, f"missing fragment: {fragment!r}"


def test_chat_app_version_placeholder_never_leaks(chat_client: TestClient) -> None:
    res = chat_client.get("/chat-app")
    assert res.status_code == 200
    assert "__VERSION__" not in res.text


def test_chat_app_substitutes_app_version(
    chat_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.main as main

    monkeypatch.setattr(main, "app_version", lambda: "9.9.9-testver")
    res = chat_client.get("/chat-app")
    assert res.status_code == 200
    assert "9.9.9-testver" in res.text
    assert "__VERSION__" not in res.text


def test_chat_app_fineprint_and_nav_links(chat_client: TestClient) -> None:
    body = chat_client.get("/chat-app").text
    assert 'href="/"' in body
    assert 'href="/docs"' in body
    assert "OpenAPI" in body
    assert "Retrieval RAG" in body


def test_chat_app_post_put_patch_delete_not_allowed(chat_client: TestClient) -> None:
    assert chat_client.post("/chat-app").status_code == 405
    assert chat_client.put("/chat-app").status_code == 405
    assert chat_client.patch("/chat-app").status_code == 405
    assert chat_client.delete("/chat-app").status_code == 405


def test_chat_app_head_not_implemented(chat_client: TestClient) -> None:
    """FastAPI only registers ``GET``; ``HEAD`` is not auto-mounted for this route."""
    res = chat_client.head("/chat-app")
    assert res.status_code == 405


def test_chat_app_listed_in_openapi(chat_client: TestClient) -> None:
    doc = chat_client.get("/openapi.json").json()
    entry = doc["paths"]["/chat-app"]
    assert "get" in entry
    assert "200" in entry["get"]["responses"]


def test_chat_app_response_substantial(chat_client: TestClient) -> None:
    """Catch accidental truncation or empty template."""
    text = chat_client.get("/chat-app").text
    assert len(text) > 8000
