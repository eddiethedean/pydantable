import pytest
from pydantic import ValidationError

from pydantable import DataFrame, Schema


class User(Schema):
    id: int
    age: int


def test_dataframe_construction_happy_path():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    assert df.schema_fields() == {"id": int, "age": int}


def test_dataframe_construction_missing_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        DataFrame[User]({"id": [1, 2]})


def test_dataframe_construction_unknown_columns():
    with pytest.raises(ValueError, match="Unknown columns"):
        DataFrame[User]({"id": [1, 2], "age": [20, 30], "extra": [1, 2]})


def test_dataframe_construction_type_validation():
    with pytest.raises(ValidationError):
        DataFrame[User]({"id": ["x"], "age": [1]})


def test_dataframe_construction_length_mismatch():
    with pytest.raises(ValueError, match="All columns must have the same length"):
        DataFrame[User]({"id": [1, 2], "age": [1]})

