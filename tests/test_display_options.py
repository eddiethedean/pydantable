"""Tests for HTML preview limits (env + set_display_options)."""

from __future__ import annotations

import os

import pytest
from pydantable import DataFrame
from pydantable.display import (
    get_repr_html_limits,
    reset_display_options,
    set_display_options,
)
from pydantic import BaseModel


class _S(BaseModel):
    x: int


def test_default_limits_match_module_defaults() -> None:
    reset_display_options()
    for k in (
        "PYDANTABLE_REPR_HTML_MAX_ROWS",
        "PYDANTABLE_REPR_HTML_MAX_COLS",
        "PYDANTABLE_REPR_HTML_MAX_CELL_LEN",
    ):
        os.environ.pop(k, None)
    lim = get_repr_html_limits()
    assert lim.max_rows == 20
    assert lim.max_cols == 40
    assert lim.max_cell_len == 500


def test_env_overrides_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_ROWS", "3")
    lim = get_repr_html_limits()
    assert lim.max_rows == 3


def test_set_display_options_override(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    for k in (
        "PYDANTABLE_REPR_HTML_MAX_ROWS",
        "PYDANTABLE_REPR_HTML_MAX_COLS",
        "PYDANTABLE_REPR_HTML_MAX_CELL_LEN",
    ):
        monkeypatch.delenv(k, raising=False)
    set_display_options(max_rows=7)
    assert get_repr_html_limits().max_rows == 7
    reset_display_options()


def test_repr_mimebundle_has_plain_and_html() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    b = df._repr_mimebundle_()
    assert "text/plain" in b and "text/html" in b
    assert "DataFrame" in b["text/plain"]
    assert "pydantable" in b["text/html"].lower() or "<table" in b["text/html"].lower()


def test_value_counts_basic() -> None:
    df = DataFrame[_S]({"x": [1, 1, 2]})
    vc = df.value_counts("x")
    assert vc[1] == 2 and vc[2] == 1


def test_value_counts_normalize() -> None:
    df = DataFrame[_S]({"x": [1, 1, 2]})
    vc = df.value_counts("x", normalize=True)
    assert pytest.approx(sum(vc.values()), rel=1e-9) == 1.0
