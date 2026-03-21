import pytest
from pydantable import DataFrame
from pydantable.schema import Schema


class UserSchema(Schema):
    id: int
    age: int | None
    name: str | None


def test_fill_null_requires_value_or_strategy() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [None], "name": [None]})
    with pytest.raises(ValueError, match="requires either value or strategy"):
        df.fill_null()


def test_fill_null_rejects_unknown_strategy() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [None], "name": [None]})
    with pytest.raises(ValueError, match="unsupported value"):
        df.fill_null(strategy="median")


def test_drop_nulls_rejects_unknown_subset_column() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [None], "name": [None]})
    with pytest.raises(KeyError, match="unknown subset column"):
        df.drop_nulls(subset=["missing"])


def test_cast_unparseable_string_to_int_becomes_null() -> None:
    df = DataFrame[UserSchema]({"id": [1], "age": [None], "name": ["x"]})
    out = df.with_columns(name_i=df.name.cast(int)).collect()
    assert out["name_i"] == [None]
