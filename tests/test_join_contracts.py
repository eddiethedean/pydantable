import pytest
from conftest import assert_table_eq_sorted
from pydantable import DataFrame
from pydantable.schema import Schema


class LeftSchema(Schema):
    id: int
    age: int | None
    score: int


class RightSchema(Schema):
    id: int
    age: int | None
    country: str
    score: int


def test_join_rejects_empty_on() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )

    with pytest.raises(ValueError, match=r"requires on=\.\.\. or both left_on"):
        left.join(right, on=[], how="inner")


def test_join_unknown_left_key() -> None:
    class LeftNoId(Schema):
        x: int
        age: int | None

    class RightHasId(Schema):
        id: int
        age: int | None

    left = DataFrame[LeftNoId]({"x": [1, 2], "age": [None, None]})
    right = DataFrame[RightHasId]({"id": [1, 2], "age": [None, None]})

    with pytest.raises(KeyError, match=r"join\(\) unknown left join key"):
        left.join(right, on="id", how="inner")


def test_join_unknown_right_key() -> None:
    class LeftHasId(Schema):
        id: int
        age: int | None

    class RightNoId(Schema):
        y: int
        age: int | None

    left = DataFrame[LeftHasId]({"id": [1, 2], "age": [None, None]})
    right = DataFrame[RightNoId]({"y": [1, 2], "age": [None, None]})

    with pytest.raises(KeyError, match=r"join\(\) unknown right join key"):
        left.join(right, on="id", how="inner")


def test_join_rejects_unsupported_how() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )

    with pytest.raises(ValueError, match=r"Unsupported join how"):
        left.join(right, on="id", how="median")


def test_join_supports_right_semi_anti_cross() -> None:
    left = DataFrame[LeftSchema]({"id": [1, 2], "age": [10, None], "score": [10, 20]})
    right = DataFrame[RightSchema](
        {"id": [2, 3], "age": [None, 30], "country": ["US", "CA"], "score": [200, 300]}
    )

    out_right = left.join(right, on="id", how="right", suffix="_r").collect()
    assert set(out_right.keys()) == {
        "id",
        "age",
        "score",
        "age_r",
        "country",
        "score_r",
    }

    out_semi = left.join(right, on="id", how="semi").collect()
    assert out_semi == {"id": [2], "age": [None], "score": [20]}

    out_anti = left.join(right, on="id", how="anti").collect()
    assert out_anti == {"id": [1], "age": [10], "score": [10]}

    out_cross = left.join(right, how="cross", suffix="_r").collect()
    assert len(out_cross["id"]) == 4


def test_cross_join_rejects_on_keys() -> None:
    left = DataFrame[LeftSchema]({"id": [1], "age": [None], "score": [10]})
    right = DataFrame[RightSchema](
        {"id": [1], "age": [None], "country": ["US"], "score": [100]}
    )
    with pytest.raises(ValueError, match="cross join does not accept"):
        left.join(right, on="id", how="cross")


def test_join_supports_expression_keys() -> None:
    left = DataFrame[LeftSchema]({"id": [1, 2], "age": [10, 20], "score": [10, 20]})
    right = DataFrame[RightSchema](
        {"id": [2, 1], "age": [20, 10], "country": ["US", "CA"], "score": [200, 100]}
    )
    out = left.join(right, left_on=left.id, right_on=right.id, how="inner").collect()
    assert_table_eq_sorted(
        out,
        {
            "id": [1, 2],
            "age": [10, 20],
            "score": [10, 20],
            "age_right": [10, 20],
            "country": ["CA", "US"],
            "score_right": [100, 200],
        },
        keys=["id"],
    )


def test_join_collision_suffixes_all_non_key_overlaps() -> None:
    class Left(Schema):
        id: int
        score: int
        age: int | None

    class Right(Schema):
        id: int
        score: int
        age: int | None

    left = DataFrame[Left]({"id": [2, 1], "score": [20, 10], "age": [200, 100]})
    right = DataFrame[Right]({"id": [2, 1], "score": [200, 100], "age": [2000, 1000]})

    joined = left.join(right, on="id", how="inner", suffix="_r")
    out = joined.collect()
    assert_table_eq_sorted(
        out,
        {
            "id": [1, 2],
            "score": [10, 20],
            "score_r": [100, 200],
            "age": [100, 200],
            "age_r": [1000, 2000],
        },
        keys=["id"],
    )
