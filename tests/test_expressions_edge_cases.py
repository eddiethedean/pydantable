from __future__ import annotations

import pytest
from pydantable import DataFrame, Schema
from pydantable.expressions import (
    Expr,
    Literal,
    WhenChain,
    coalesce,
    concat,
    when,
)


class R(Schema):
    flag: bool
    s: str


class N(Schema):
    a: int
    s: str


def test_expr_repr_truncates_large_ast() -> None:
    class _HugeAst:
        dtype = "bool"

        def referenced_columns(self):
            return []

        def to_serializable(self):
            return {"blob": "x" * 400}

    r = repr(Expr(rust_expr=_HugeAst()))
    assert "…" in r or len(r) < 500


def test_expr_repr_ast_snippet_on_serializable_error() -> None:
    class _BadAst:
        dtype = "bool"

        def referenced_columns(self):
            return []

        def to_serializable(self):
            msg = "no serialize"
            raise RuntimeError(msg)

    r = repr(Expr(rust_expr=_BadAst()))
    assert "ast=?" in r


def test_logical_rand_ror_and_invert() -> None:
    df = DataFrame[R]({"flag": [True, False], "s": ["a", "b"]})
    a = True & df.flag
    b = df.flag & True
    c = False | df.flag
    d = df.flag | False
    e = ~df.flag
    assert all(isinstance(x, Expr) for x in (a, b, c, d, e))


def test_when_chain_type_errors() -> None:
    df = DataFrame[R]({"flag": [True], "s": ["x"]})
    with pytest.raises(TypeError, match="when\\(\\) expects Expr"):
        WhenChain(object(), object())  # type: ignore[arg-type]

    wc = when(df.flag, Literal(value=True))
    with pytest.raises(TypeError, match="when\\(\\)\\.when"):
        wc.when(object(), Literal(value=False))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="otherwise"):
        wc.otherwise(object())  # type: ignore[arg-type]


def test_coalesce_requires_exprs() -> None:
    with pytest.raises(TypeError, match="at least one"):
        coalesce()


def test_concat_requires_two_and_expr_types() -> None:
    df = DataFrame[R]({"flag": [True], "s": ["hi"]})
    with pytest.raises(TypeError, match="at least two"):
        concat(df.s)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="must be Expr"):
        concat(df.s, 1)  # type: ignore[arg-type]


def test_expr_truediv_ne_and_substr_with_length() -> None:
    df = DataFrame[N]({"a": [10, 12], "s": ["abcdef", "xyz"]})
    out = df.with_columns(
        half=df.a / 2,
        ne=df.a != 10,
        name=df.s.substr(2, 3),
    ).collect(as_lists=True)
    assert out["half"] == [5, 6]
    assert out["ne"] == [False, True]
    assert out["name"] == ["bcd", "yz"]


def test_expr_leq_and_isin_with_list_singleton() -> None:
    df = DataFrame[N]({"a": [1, 3, 5], "s": ["a", "b", "c"]})
    out = df.with_columns(
        ok=df.a <= 3,
        bag=df.a.isin([1, 5]),
    ).collect(as_lists=True)
    assert out["ok"] == [True, True, False]
    assert out["bag"] == [True, False, True]
