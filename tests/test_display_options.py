"""Tests for HTML preview limits (env + set_display_options)."""

from __future__ import annotations

import os

import pytest
from pydantable import DataFrame, DataFrameModel
from pydantable.display import (
    get_repr_html_limits,
    reset_display_options,
    set_display_options,
)
from pydantic import BaseModel


class _S(BaseModel):
    x: int


class _Wide(BaseModel):
    a: int
    b: int
    c: int
    d: int
    e: int


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


def test_env_overrides_cols_and_cell_len(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_COLS", "2")
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_CELL_LEN", "99")
    lim = get_repr_html_limits()
    assert lim.max_cols == 2
    assert lim.max_cell_len == 99


def test_env_invalid_int_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_ROWS", "not-a-number")
    assert get_repr_html_limits().max_rows == 20


def test_env_non_positive_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_ROWS", "0")
    assert get_repr_html_limits().max_rows == 20


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


def test_programmatic_override_takes_precedence_over_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_ROWS", "99")
    set_display_options(max_rows=5, max_cols=10, max_cell_len=100)
    lim = get_repr_html_limits()
    assert lim.max_rows == 5
    assert lim.max_cols == 10
    assert lim.max_cell_len == 100
    reset_display_options()


def test_set_display_options_partial_updates_other_axes_from_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_display_options()
    monkeypatch.delenv("PYDANTABLE_REPR_HTML_MAX_ROWS", raising=False)
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_COLS", "12")
    set_display_options(max_rows=8)
    lim = get_repr_html_limits()
    assert lim.max_rows == 8
    assert lim.max_cols == 12
    reset_display_options()


def test_set_display_options_rejects_non_positive() -> None:
    reset_display_options()
    with pytest.raises(ValueError, match="positive"):
        set_display_options(max_rows=0)
    with pytest.raises(ValueError, match="positive"):
        set_display_options(max_cols=-1)
    reset_display_options()


def test_reset_restores_env_visibility(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    monkeypatch.setenv("PYDANTABLE_REPR_HTML_MAX_ROWS", "4")
    set_display_options(max_rows=50)
    assert get_repr_html_limits().max_rows == 50
    reset_display_options()
    assert get_repr_html_limits().max_rows == 4


def test_repr_html_respects_row_limit_in_note(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    monkeypatch.delenv("PYDANTABLE_REPR_HTML_MAX_ROWS", raising=False)
    set_display_options(max_rows=2, max_cols=40, max_cell_len=500)
    try:
        df = DataFrame[_S]({"x": list(range(10))})
        html = df._repr_html_()
        assert "up to 2 rows" in html
    finally:
        reset_display_options()


def test_repr_html_omits_extra_columns_in_note(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_display_options()
    for k in (
        "PYDANTABLE_REPR_HTML_MAX_ROWS",
        "PYDANTABLE_REPR_HTML_MAX_COLS",
        "PYDANTABLE_REPR_HTML_MAX_CELL_LEN",
    ):
        monkeypatch.delenv(k, raising=False)
    set_display_options(max_rows=20, max_cols=2, max_cell_len=500)
    try:
        df = DataFrame[_Wide](
            {
                "a": [1],
                "b": [2],
                "c": [3],
                "d": [4],
                "e": [5],
            }
        )
        html = df._repr_html_()
        assert "more column" in html or "more columns" in html
    finally:
        reset_display_options()


def test_repr_mimebundle_has_plain_and_html() -> None:
    df = DataFrame[_S]({"x": [1, 2]})
    b = df._repr_mimebundle_()
    assert "text/plain" in b and "text/html" in b
    assert "DataFrame" in b["text/plain"]
    assert "pydantable" in b["text/html"].lower() or "<table" in b["text/html"].lower()


def test_repr_mimebundle_accepts_include_exclude_kwargs() -> None:
    df = DataFrame[_S]({"x": [1]})
    b = df._repr_mimebundle_(include={"text/html"}, exclude=None)
    assert set(b.keys()) == {"text/plain", "text/html"}


def test_repr_mimebundle_dataframe_model_wraps_plain_and_html() -> None:
    class M(DataFrameModel):
        x: int

    m = M({"x": [1, 2]})
    b = m._repr_mimebundle_()
    assert "DataFrame[" in b["text/plain"] or "DataFrameModel" in b["text/plain"]
    assert "DataFrameModel" in b["text/html"] or "pydantable-render--context" in b[
        "text/html"
    ]
    assert "<table" in b["text/html"].lower() or "pydantable-render" in b["text/html"]


def test_repr_mimebundle_pandas_facade_inherits() -> None:
    from pydantable.pandas import DataFrame as PDF

    class P(BaseModel):
        n: int

    df = PDF[P]({"n": [1]})
    b = df._repr_mimebundle_()
    assert "text/plain" in b and "text/html" in b


def test_repr_mimebundle_pyspark_facade_inherits() -> None:
    from pydantable.pyspark import DataFrame as PSDataFrame

    class R(BaseModel):
        a: int

    df = PSDataFrame[R]({"a": [1]})
    b = df._repr_mimebundle_()
    assert "text/plain" in b and "text/html" in b


def test_value_counts_basic() -> None:
    df = DataFrame[_S]({"x": [1, 1, 2]})
    vc = df.value_counts("x")
    assert vc[1] == 2 and vc[2] == 1


def test_value_counts_normalize() -> None:
    df = DataFrame[_S]({"x": [1, 1, 2]})
    vc = df.value_counts("x", normalize=True)
    assert pytest.approx(sum(vc.values()), rel=1e-9) == 1.0


def test_value_counts_unknown_column_raises() -> None:
    df = DataFrame[_S]({"x": [1]})
    with pytest.raises(KeyError, match="Unknown column"):
        df.value_counts("y")


def test_value_counts_sorts_by_count_then_key_repr() -> None:
    """Tie on count: secondary sort ascending by ``repr(key)`` (``a`` before ``b``)."""

    class _K(BaseModel):
        k: str

    df = DataFrame[_K]({"k": ["b", "b", "a", "a"]})
    vc = df.value_counts("k")
    keys = list(vc.keys())
    assert keys[0] == "a" and keys[1] == "b"


def test_value_counts_filtered_plan() -> None:
    df = DataFrame[_S]({"x": [1, 2, 2, 3]})
    f = df.filter(df.x < 3)
    vc = f.value_counts("x")
    assert vc[1] == 1 and vc[2] == 2 and 3 not in vc


def test_value_counts_dropna_false_includes_null_keys() -> None:
    class _Opt(BaseModel):
        v: int | None

    df = DataFrame[_Opt]({"v": [1, None, 1]})
    vc_keep = df.value_counts("v", dropna=False)
    assert None in vc_keep
    vc_drop = df.value_counts("v", dropna=True)
    assert None not in vc_drop


def test_value_counts_normalize_empty_returns_zeros() -> None:
    class _Opt(BaseModel):
        v: int | None

    df = DataFrame[_Opt]({"v": [None, None]})
    vc = df.value_counts("v", normalize=True, dropna=True)
    assert vc == {}


def test_value_counts_dataframe_model_delegates() -> None:
    class M(DataFrameModel):
        x: int

    m = M({"x": [1, 1, 2]})
    assert m.value_counts("x") == {1: 2, 2: 1}
