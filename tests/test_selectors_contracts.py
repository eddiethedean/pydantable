import pytest
from pydantable import DataFrame
from pydantable import selectors as s
from pydantable.schema import Schema


def test_with_columns_cast_selector_happy_path_and_empty_match_strict() -> None:
    class S(Schema):
        x: int
        y: int

    df = DataFrame[S]({"x": [1], "y": [2]})
    out = df.with_columns_cast(s.by_name("x"), float).to_dict()
    assert out == {"x": [1.0], "y": [2]}

    with pytest.raises(ValueError, match=r"matched no columns.*Available columns"):
        df.with_columns_cast(s.starts_with("zzz"), float)

    out2 = df.with_columns_cast(s.starts_with("zzz"), float, strict=False).to_dict()
    assert out2 == {"x": [1], "y": [2]}


def test_with_columns_fill_null_selector_happy_path_and_empty_match_strict() -> None:
    class S(Schema):
        x: int | None
        y: int | None

    df = DataFrame[S]({"x": [None, 1], "y": [2, None]})
    out = df.with_columns_fill_null(s.by_name("x"), value=0).to_dict()
    assert out == {"x": [0, 1], "y": [2, None]}

    with pytest.raises(ValueError, match=r"matched no columns.*Available columns"):
        df.with_columns_fill_null(s.starts_with("zzz"), value=0)

    out2 = df.with_columns_fill_null(
        s.starts_with("zzz"), value=0, strict=False
    ).to_dict()
    assert out2 == {"x": [None, 1], "y": [2, None]}


def test_rename_selector_helpers_and_collision_errors() -> None:
    class S(Schema):
        a: int
        A: int
        spaced: int

    df = DataFrame[S]({"a": [1], "A": [2], "spaced": [3]})

    out = df.rename_strip(s.by_name("spaced"), chars=None).rename_prefix("x_").to_dict()
    assert set(out.keys()) == {"x_a", "x_A", "x_spaced"}

    with pytest.raises(ValueError, match="duplicate output column names"):
        df.rename_lower()


def test_select_schema_is_select_alias_with_typecheck() -> None:
    class S(Schema):
        a: int
        b: int

    df = DataFrame[S]({"a": [1], "b": [2]})
    assert df.select_schema(s.by_name("a")).to_dict() == {"a": [1]}

    with pytest.raises(TypeError, match="expects a Selector"):
        df.select_schema("a")  # type: ignore[arg-type]
