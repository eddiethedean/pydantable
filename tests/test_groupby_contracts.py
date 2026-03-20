import pytest
from pydantable import DataFrame
from pydantable.schema import Schema


class UserSchema(Schema):
    id: int
    age: int | None


def test_groupby_rejects_empty_keys() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [20]})
    with pytest.raises(
        ValueError,
        match=r"group_by\(\.\.\.\) requires at least one key",
    ):
        df.group_by().agg(age_sum=("sum", "age"))


def test_agg_rejects_empty_aggregations() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [20]})
    with pytest.raises(
        ValueError,
        match=r"agg\(\.\.\.\) requires at least one aggregation",
    ):
        df.group_by("id").agg()


def test_groupby_rejects_unsupported_agg_op() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [20]})
    with pytest.raises(ValueError, match=r"Unsupported aggregation"):
        df.group_by("id").agg(age_median=("median", "age"))


def test_groupby_unknown_key_is_validated() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [20]})
    with pytest.raises(KeyError, match=r"group_by\(\) unknown key"):
        df.group_by("missing").agg(age_sum=("sum", "age"))


def test_agg_unknown_input_column_is_validated() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [20]})
    with pytest.raises(KeyError, match=r"agg\(\) unknown input column"):
        df.group_by("id").agg(missing_sum=("sum", "missing"))


def test_all_null_group_preserves_nullable_aggregate_schema() -> None:
    df = DataFrame[UserSchema]({"id": [1, 1], "age": [None, None]})
    grouped = df.group_by("id").agg(
        age_sum=("sum", "age"),
        age_mean=("mean", "age"),
        age_count=("count", "age"),
    )
    out = grouped.collect()

    assert out["id"] == [1]
    assert out["age_sum"] == [None]
    assert out["age_mean"] == [None]
    assert out["age_count"] == [0]

    schema = grouped.schema_fields()
    assert schema["age_sum"] == int | None
    assert schema["age_mean"] == float | None
    assert schema["age_count"] is int
