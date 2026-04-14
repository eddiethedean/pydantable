from __future__ import annotations

import os

import pytest
from pydantable.dataframe._streaming import (
    _is_bool_or_nullable_bool,
    _resolve_engine_streaming,
)


def test_resolve_engine_streaming_prefers_engine_streaming_over_streaming(monkeypatch):
    monkeypatch.setenv("PYDANTABLE_ENGINE_STREAMING", "true")

    assert _resolve_engine_streaming(engine_streaming=True, default=False) is True
    assert _resolve_engine_streaming(streaming=False, default=True) is False


def test_resolve_engine_streaming_rejects_both_engine_streaming_and_streaming():
    with pytest.raises(TypeError, match="either streaming= or engine_streaming="):
        _resolve_engine_streaming(streaming=True, engine_streaming=False)


def test_resolve_engine_streaming_default_then_env(monkeypatch):
    monkeypatch.delenv("PYDANTABLE_ENGINE_STREAMING", raising=False)
    assert _resolve_engine_streaming(default=True) is True
    assert _resolve_engine_streaming(default=False) is False

    monkeypatch.setenv("PYDANTABLE_ENGINE_STREAMING", "yes")
    assert _resolve_engine_streaming() is True

    monkeypatch.setenv("PYDANTABLE_ENGINE_STREAMING", "0")
    assert _resolve_engine_streaming() is False

    # ensure we don't leak env changes if this is run outside pytest isolation
    os.environ.pop("PYDANTABLE_ENGINE_STREAMING", None)


def test_is_bool_or_nullable_bool():
    assert _is_bool_or_nullable_bool(bool) is True
    assert _is_bool_or_nullable_bool(bool | None) is True
    assert _is_bool_or_nullable_bool(int) is False
