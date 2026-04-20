"""OpenAI API backend (no local generative weights)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


def test_chat_openai_returns_503_when_api_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.main as main

    real_get = main.get_settings

    def fake_get() -> object:
        s = real_get()
        return s.model_copy(
            update={"llm_backend": "openai", "llm_model": "gpt-5.4-nano"}
        )

    monkeypatch.setattr(main, "get_settings", fake_get)
    monkeypatch.setattr(main, "openai_api_key_configured", lambda: False)

    c = TestClient(main.app)
    res = c.post("/chat", json={"message": "hello"})
    assert res.status_code == 503
    assert "OPENAI_API_KEY" in res.json()["detail"]


def test_readyz_openai_ok_when_key_set(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.main as main

    real_get = main.get_settings

    def fake_get() -> object:
        s = real_get()
        return s.model_copy(
            update={"llm_backend": "openai", "llm_model": "gpt-5.4-nano"}
        )

    monkeypatch.setattr(main, "get_settings", fake_get)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(main, "get_counts", lambda **_kwargs: {"docs": 1, "vecs": 1})

    c = TestClient(main.app)
    res = c.get("/readyz")
    assert res.status_code == 200
    assert res.json()["ok"] is True
    assert res.json()["llm_loaded"] is True
