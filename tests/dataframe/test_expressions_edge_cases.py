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


def test_expr_over_with_no_partition_or_order_is_identity_in_pipeline() -> None:
    df = DataFrame[R]({"flag": [True], "s": ["a"]})
    out = df.with_columns(same=df.flag.over()).collect(as_lists=True)
    assert out["same"] == [True]


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


class _Tags(Schema):
    tags: list[str]


class _MapCol(Schema):
    meta: dict[str, int]


def test_expr_alias_rejects_empty_name() -> None:
    df = DataFrame[N]({"a": [1], "s": ["x"]})
    with pytest.raises(TypeError, match="non-empty string"):
        df.a.alias("")


def test_list_contains_any_rejects_expr_values() -> None:
    df = DataFrame[_Tags]({"tags": [["a", "b"]]})
    with pytest.raises(TypeError, match="literal"):
        df.tags.contains_any(df.tags)


def test_list_contains_any_requires_at_least_one_literal() -> None:
    df = DataFrame[_Tags]({"tags": [["a"]]})
    with pytest.raises(TypeError, match="at least one"):
        df.tags.contains_any([])


def test_list_contains_all_rejects_expr_values() -> None:
    df = DataFrame[_Tags]({"tags": [["a", "b"]]})
    with pytest.raises(TypeError, match="literal"):
        df.tags.contains_all(df.tags)


def test_map_has_any_key_rejects_expr_keys() -> None:
    df = DataFrame[_MapCol]({"meta": [{"k": 1}]})
    with pytest.raises(TypeError, match="literal"):
        df.meta.map_has_any_key(df.meta)


def test_map_has_any_key_requires_at_least_one_key() -> None:
    df = DataFrame[_MapCol]({"meta": [{"k": 1}]})
    with pytest.raises(TypeError, match="at least one key"):
        df.meta.map_has_any_key([])


def test_matches_rejects_empty_pattern() -> None:
    df = DataFrame[N]({"a": [1], "s": ["x"]})
    with pytest.raises(TypeError, match="non-empty string"):
        df.s.matches("")


def test_contains_all_with_multiple_literals_ands() -> None:
    df = DataFrame[_Tags]({"tags": [["a", "b", "c"]]})
    out = df.with_columns(
        ok=df.tags.contains_all(["a", "b"]),
    ).collect(as_lists=True)
    assert out["ok"] == [True]


def test_str_contains_pat_empty_pattern_regex_raises() -> None:
    df = DataFrame[N]({"a": [1], "s": ["hi"]})
    with pytest.raises(ValueError, match="empty"):
        df.s.str_contains_pat("", literal=False)
