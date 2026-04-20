from __future__ import annotations

from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient


@dataclass(frozen=True)
class _FakeRetrieved:
    source: str
    chunk_id: str
    distance: float


class _FakeRagResult:
    def __init__(self, answer: str):
        self.answer = answer
        self.retrieved = [
            _FakeRetrieved(
                source="docs/intro.md", chunk_id="docs/intro.md::c0", distance=0.1
            )
        ]


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """
    Realistic HTTP tests (routing/validation/serialization) without heavy model loads.
    """
    # Import inside fixture so monkeypatches apply per-test.
    import app.main as main

    # Make the app think the LLM is already warm.
    monkeypatch.setattr(main, "llm_is_loaded", lambda _m: True)
    monkeypatch.setattr(main, "llm_is_loading", lambda _m: False)
    monkeypatch.setattr(main, "embed_deployment_ready", lambda _m, _d: True)
    monkeypatch.setattr(main, "embedder_is_loading", lambda _m, _d: False)
    monkeypatch.setattr(main, "embedding_compute_active", lambda: False)
    monkeypatch.setattr(main, "warm_llm", lambda _m: None)

    # Avoid touching the filesystem/docs during tests.
    monkeypatch.setattr(main, "ingest_repo_docs", lambda **_kwargs: None)

    # Avoid embeddings / sqlite / transformers — return deterministic output.
    monkeypatch.setattr(
        main, "rag_chat", lambda **_kwargs: _FakeRagResult("pydantable is ...")
    )

    return TestClient(main.app)


def test_root_status_page(client: TestClient) -> None:
    res = client.get("/")
    assert res.status_code == 200
    assert "text/html" in res.headers.get("content-type", "")
    body = res.text
    assert "pydantable-rag" in body
    assert "Embedding model" in body
    assert "/docs" in body
    assert "/chat-app" in body


def test_healthz_has_version_and_config(client: TestClient) -> None:
    res = client.get("/healthz")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert "version" in body
    assert "db_path" in body
    assert "embed_model" in body
    assert "llm_model" in body
    assert body.get("llm_backend") == "extractive"
    assert "llm_loaded" in body
    assert body["llm_loading"] is False
    assert "embed_dims" in body
    assert body["embed_loaded"] is True
    assert body["embed_loading"] is False
    assert body["embed_computing"] is False


def test_readyz_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    import app.main as main

    monkeypatch.setattr(main, "get_counts", lambda **_kwargs: {"docs": 0, "vecs": 0})
    res = client.get("/readyz")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is False
    assert body["counts"]["docs"] == 0
    assert body["counts"]["vecs"] == 0
    assert body["llm_loaded"] is True
    assert body["llm_loading"] is False
    assert body["embed_loaded"] is True
    assert body["embed_loading"] is False
    assert body["embed_computing"] is False
    assert body.get("llm_backend") == "extractive"


def test_chat_success(client: TestClient) -> None:
    res = client.post("/chat", json={"message": "tell me about pydantable"})
    assert res.status_code == 200
    body = res.json()
    assert "answer" in body
    assert isinstance(body["sources"], list)
    assert body["sources"][0]["source"] == "docs/intro.md"


def test_chat_validates_request_body(client: TestClient) -> None:
    res = client.post("/chat", json={"message": ""})
    assert res.status_code == 422


def test_chat_extractive_skips_llm_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.main as main

    monkeypatch.setattr(main, "llm_is_loaded", lambda _m: False)
    monkeypatch.setattr(
        main, "rag_chat", lambda **_kwargs: _FakeRagResult("extractive ok")
    )

    c = TestClient(main.app)
    res = c.post("/chat", json={"message": "hello"})
    assert res.status_code == 200
    assert res.json()["answer"] == "extractive ok"


def test_chat_returns_503_when_llm_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.main as main

    real_get = main.get_settings

    def fake_get() -> object:
        s = real_get()
        return s.model_copy(update={"llm_backend": "hf"})

    monkeypatch.setattr(main, "get_settings", fake_get)
    monkeypatch.setattr(main, "llm_is_loaded", lambda _m: False)
    monkeypatch.setattr(main, "llm_is_loading", lambda _m: False)
    monkeypatch.setattr(main, "warm_llm", lambda _m: None)

    # Avoid any real work if warm-up is triggered.
    monkeypatch.setattr(main, "rag_chat", lambda **_kwargs: _FakeRagResult("x"))

    c = TestClient(main.app)
    res = c.post("/chat", json={"message": "hello"})
    assert res.status_code == 503
    detail = res.json()["detail"].lower()
    assert "load" in detail or "fail" in detail


def test_ingest_starts_background_job(client: TestClient) -> None:
    res = client.post("/ingest", json={})
    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert res.json()["started"] is True


def test_diag_has_backend_and_counts(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    import app.main as main

    monkeypatch.setattr(
        main, "check_vector_backend", lambda **_kwargs: {"ok": True, "backend": "py"}
    )
    monkeypatch.setattr(
        main, "get_counts", lambda **_kwargs: {"docs": 1, "vecs": 1, "backend": "py"}
    )

    res = client.get("/diag")
    assert res.status_code == 200
    body = res.json()
    assert "version" in body
    assert body["vector_backend"]["backend"] == "py"
    assert body["counts"]["docs"] == 1
    assert body["llm_loading"] is False
    assert body.get("llm_last_error") is None
    assert body.get("llm_backend") == "extractive"
    assert body["embed_loaded"] is True
    assert body["embed_loading"] is False
    assert body["embed_computing"] is False
