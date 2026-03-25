"""Tests for optional PYDANTABLE_VERBOSE_ERRORS on execute_plan ValueError."""

from __future__ import annotations

import pydantable.rust_engine as rust_engine
import pytest


class _FakeRustCore:
    """Minimal stand-in: only ``execute_plan`` used by :func:`execute_plan`."""

    def execute_plan(
        self, plan: object, data: object, as_python_lists: bool, streaming: bool = False
    ) -> None:
        raise ValueError("simulated engine failure")


def test_verbose_errors_append_context_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_VERBOSE_ERRORS", "1")
    monkeypatch.setattr(rust_engine, "_RUST_CORE", _FakeRustCore())
    with pytest.raises(ValueError, match="simulated engine failure") as exc:
        rust_engine.execute_plan(
            None,
            None,
            error_context="schema=SomeModel",
        )
    assert "[context: schema=SomeModel]" in str(exc.value)


@pytest.mark.parametrize("flag", ("1", "true", "yes", "TRUE"))
def test_verbose_errors_truthy_env(monkeypatch: pytest.MonkeyPatch, flag: str) -> None:
    monkeypatch.setenv("PYDANTABLE_VERBOSE_ERRORS", flag)
    monkeypatch.setattr(rust_engine, "_RUST_CORE", _FakeRustCore())
    with pytest.raises(ValueError) as exc:
        rust_engine.execute_plan(None, None, error_context="ctx")
    assert "[context: ctx]" in str(exc.value)


def test_verbose_errors_off_does_not_append_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYDANTABLE_VERBOSE_ERRORS", raising=False)
    monkeypatch.setattr(rust_engine, "_RUST_CORE", _FakeRustCore())
    with pytest.raises(ValueError) as exc:
        rust_engine.execute_plan(None, None, error_context="schema=Ignored")
    assert "[context:" not in str(exc.value)


def test_verbose_errors_plain_without_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_VERBOSE_ERRORS", "1")
    monkeypatch.setattr(rust_engine, "_RUST_CORE", _FakeRustCore())
    with pytest.raises(ValueError) as exc:
        rust_engine.execute_plan(None, None, error_context=None)
    assert "[context:" not in str(exc.value)
