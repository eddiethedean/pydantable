import pytest
from pydantable import DataFrame, Schema
from pydantable.schema import (
    descriptor_matches_column_annotation,
    dtype_descriptor_to_annotation,
    is_supported_column_annotation,
    is_supported_scalar_column_annotation,
    merge_field_types_preserving_identity,
)
from pydantic import BaseModel, ValidationError


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


class Inner(Schema):
    x: int


class Outer(Schema):
    inner: Inner


def test_is_supported_column_annotation_nested_models():
    assert is_supported_column_annotation(Inner)
    assert is_supported_column_annotation(Outer)
    assert not is_supported_scalar_column_annotation(Outer)


def test_descriptor_matches_column_annotation_nested():
    inner_struct = {
        "kind": "struct",
        "nullable": False,
        "fields": [{"name": "x", "dtype": {"base": "int", "nullable": False}}],
    }
    desc = {
        "kind": "struct",
        "nullable": False,
        "fields": [{"name": "inner", "dtype": inner_struct}],
    }
    assert descriptor_matches_column_annotation(desc, Outer)
    assert not descriptor_matches_column_annotation(
        {
            "kind": "struct",
            "nullable": False,
            "fields": [
                {
                    "name": "inner",
                    "dtype": {
                        "kind": "struct",
                        "nullable": False,
                        "fields": [
                            {"name": "x", "dtype": {"base": "str", "nullable": False}}
                        ],
                    },
                },
            ],
        },
        Outer,
    )


def test_merge_field_types_preserving_identity_restores_user_model():
    desc = {
        "addr": {
            "kind": "struct",
            "nullable": False,
            "fields": [{"name": "x", "dtype": {"base": "int", "nullable": False}}],
        }
    }
    derived = {k: dtype_descriptor_to_annotation(v) for k, v in desc.items()}
    prev = {"addr": Inner}
    merged = merge_field_types_preserving_identity(prev, desc, derived)
    assert merged["addr"] is Inner


def test_dtype_descriptor_to_annotation_struct():
    desc = {
        "kind": "struct",
        "nullable": False,
        "fields": [
            {"name": "x", "dtype": {"base": "int", "nullable": True}},
        ],
    }
    ann = dtype_descriptor_to_annotation(desc)
    assert isinstance(ann, type)
    assert issubclass(ann, BaseModel)
    assert ann.model_fields["x"].annotation == int | None


def test_descriptor_matches_column_annotation_list_int():
    desc = {
        "kind": "list",
        "nullable": False,
        "inner": {"base": "int", "nullable": False},
    }
    assert descriptor_matches_column_annotation(desc, list[int])
    assert not descriptor_matches_column_annotation(desc, list[str])
    assert not descriptor_matches_column_annotation(desc, int)


def test_descriptor_matches_column_annotation_optional_list():
    desc = {
        "kind": "list",
        "nullable": True,
        "inner": {"base": "int", "nullable": False},
    }
    assert descriptor_matches_column_annotation(desc, list[int] | None)


def test_dtype_descriptor_to_annotation_list_int():
    desc = {
        "kind": "list",
        "nullable": False,
        "inner": {"base": "str", "nullable": True},
    }
    ann = dtype_descriptor_to_annotation(desc)
    assert ann == list[str | None]


def test_merge_field_types_preserving_identity_restores_list_annotation():
    desc = {
        "tags": {
            "kind": "list",
            "nullable": False,
            "inner": {"base": "int", "nullable": False},
        }
    }
    derived = {k: dtype_descriptor_to_annotation(v) for k, v in desc.items()}
    prev = {"tags": list[int]}
    merged = merge_field_types_preserving_identity(prev, desc, derived)
    assert merged["tags"] == list[int]


def test_is_supported_column_annotation_list_variants():
    class Inner(Schema):
        x: int

    assert is_supported_column_annotation(list[int])
    assert is_supported_column_annotation(list[str])
    assert is_supported_column_annotation(list[Inner])
    assert is_supported_column_annotation(list[int] | None)
    assert not is_supported_column_annotation(list[int, str])
