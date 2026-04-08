"""PlanFrame string expression lowering via ``planframe_adapter.expr`` (issue #2)."""

from __future__ import annotations

from planframe.expr import api as pf
from pydantable import DataFrameModel
from pydantable.planframe_adapter.execute import execute_frame


class _Strings(DataFrameModel):
    s: str


class _StringsOpt(DataFrameModel):
    s: str | None


def _run_with_column(m: DataFrameModel, pf_expr: object) -> dict[str, list[object]]:
    out = execute_frame(m._pf.with_column("out", pf_expr))
    return out.to_dict()


def test_planframe_str_lower() -> None:
    d = _run_with_column(_Strings({"s": ["Hello", "WORLD"]}), pf.StrLower(pf.col("s")))
    assert d["out"] == ["hello", "world"]


def test_planframe_str_upper() -> None:
    d = _run_with_column(_Strings({"s": ["Hello", "WORLD"]}), pf.StrUpper(pf.col("s")))
    assert d["out"] == ["HELLO", "WORLD"]


def test_planframe_str_len() -> None:
    d = _run_with_column(_Strings({"s": ["hello", "world"]}), pf.StrLen(pf.col("s")))
    assert d["out"] == [5, 5]


def test_planframe_str_strip() -> None:
    d = _run_with_column(
        _Strings({"s": ["  ab  ", "  cd  "]}),
        pf.StrStrip(pf.col("s")),
    )
    assert d["out"] == ["ab", "cd"]


def test_planframe_str_replace_literal() -> None:
    d = _run_with_column(
        _Strings({"s": ["abc", "def"]}),
        pf.StrReplace(pf.col("s"), pattern="a", replacement="x", literal=True),
    )
    assert d["out"] == ["xbc", "def"]


def test_planframe_str_split() -> None:
    d = _run_with_column(
        _Strings({"s": ["a,b", "c,d"]}),
        pf.StrSplit(pf.col("s"), by=","),
    )
    assert d["out"] == [["a", "b"], ["c", "d"]]


def test_planframe_str_len_nullable_string() -> None:
    m = _StringsOpt({"s": ["ab", None]})
    out = execute_frame(m._pf.with_column("n", pf.StrLen(pf.col("s"))))
    assert out.to_dict()["n"] == [2, None]


def test_planframe_str_replace_regex_mode() -> None:
    """PlanFrame ``literal=False`` maps to pydantable regex replace."""
    d = _run_with_column(
        _Strings({"s": ["abc123", "x99y"]}),
        pf.StrReplace(pf.col("s"), pattern=r"\d+", replacement="", literal=False),
    )
    assert d["out"] == ["abc", "xy"]


def test_planframe_str_contains_literal_and_regex() -> None:
    d_lit = _run_with_column(
        _Strings({"s": ["abc", "def"]}),
        pf.StrContains(pf.col("s"), "b", literal=True),
    )
    assert d_lit["out"] == [True, False]

    d_re = _run_with_column(
        _Strings({"s": ["a1", "b2"]}),
        pf.StrContains(pf.col("s"), r"\d", literal=False),
    )
    assert d_re["out"] == [True, True]


def test_planframe_str_starts_ends_with() -> None:
    d_s = _run_with_column(
        _Strings({"s": ["abc", "xyz"]}),
        pf.StrStartsWith(pf.col("s"), "a"),
    )
    assert d_s["out"] == [True, False]

    d_e = _run_with_column(
        _Strings({"s": ["abc", "xyz"]}),
        pf.StrEndsWith(pf.col("s"), "z"),
    )
    assert d_e["out"] == [False, True]
