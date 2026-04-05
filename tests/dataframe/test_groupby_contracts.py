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
        df.group_by("id").agg(age_mode=("mode", "age"))


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
    out = grouped.collect(as_lists=True)

    assert out["id"] == [1]
    assert out["age_sum"] == [None]
    assert out["age_mean"] == [None]
    assert out["age_count"] == [0]

    schema = grouped.schema_fields()
    assert schema["age_sum"] == int | None
    assert schema["age_mean"] == float | None
    assert schema["age_count"] is int


def test_groupby_supports_phase4_aggregations() -> None:
    df = DataFrame[UserSchema]({"id": [1, 1, 2], "age": [10, 20, 30]})
    grouped = df.group_by("id").agg(
        age_min=("min", "age"),
        age_max=("max", "age"),
        age_median=("median", "age"),
        age_std=("std", "age"),
        age_var=("var", "age"),
        age_first=("first", "age"),
        age_last=("last", "age"),
        age_n_unique=("n_unique", "age"),
    )
    out = grouped.collect(as_lists=True)
    assert set(out.keys()) == {
        "id",
        "age_min",
        "age_max",
        "age_median",
        "age_std",
        "age_var",
        "age_first",
        "age_last",
        "age_n_unique",
    }


def test_groupby_phase4_numeric_rejections() -> None:
    class NameSchema(Schema):
        id: int
        name: str | None

    df = DataFrame[NameSchema]({"id": [1, 1], "name": ["a", "b"]})
    with pytest.raises(TypeError, match=r"median\(\) requires int or float"):
        df.group_by("id").agg(name_median=("median", "name"))
    with pytest.raises(TypeError, match=r"std\(\) requires int or float"):
        df.group_by("id").agg(name_std=("std", "name"))
    with pytest.raises(TypeError, match=r"var\(\) requires int or float"):
        df.group_by("id").agg(name_var=("var", "name"))


def test_groupby_phase4_all_null_semantics() -> None:
    df = DataFrame[UserSchema]({"id": [1, 1], "age": [None, None]})
    grouped = df.group_by("id").agg(
        age_min=("min", "age"),
        age_max=("max", "age"),
        age_median=("median", "age"),
        age_std=("std", "age"),
        age_var=("var", "age"),
        age_first=("first", "age"),
        age_last=("last", "age"),
        age_n_unique=("n_unique", "age"),
    )
    out = grouped.collect(as_lists=True)
    assert out["age_min"] == [None]
    assert out["age_max"] == [None]
    assert out["age_median"] == [None]
    assert out["age_std"] == [None]
    assert out["age_var"] == [None]
    assert out["age_first"] == [None]
    assert out["age_last"] == [None]
    assert out["age_n_unique"] == [0]
