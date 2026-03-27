from __future__ import annotations

import pytest
from pydantable.observe import (
    emit,
    get_observer,
    set_observer,
    span,
    trace_enabled,
)


def test_get_set_observer_roundtrip() -> None:
    assert get_observer() is None
    seen: list[dict] = []

    def obs(e: dict) -> None:
        seen.append(e)

    set_observer(obs)
    try:
        assert get_observer() is obs
        emit({"k": 1})
        assert seen == [{"k": 1}]
    finally:
        set_observer(None)
    assert get_observer() is None


def test_emit_prints_when_trace_env_enabled(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    set_observer(None)
    monkeypatch.setenv("PYDANTABLE_TRACE", "1")
    assert trace_enabled() is True
    try:
        emit({"hello": "world"})
    finally:
        monkeypatch.delenv("PYDANTABLE_TRACE", raising=False)
    out = capsys.readouterr().out
    assert "pydantable.trace" in out
    assert "hello" in out


def test_span_records_error_type_on_exception() -> None:
    events: list[dict] = []

    set_observer(events.append)
    try:
        with pytest.raises(RuntimeError), span("boom", table="t"):
            raise RuntimeError("no")
    finally:
        set_observer(None)

    assert len(events) == 1
    ev = events[0]
    assert ev["op"] == "boom"
    assert ev["ok"] is False
    assert ev["error_type"] == "RuntimeError"
    assert ev["table"] == "t"
    assert "duration_ms" in ev
