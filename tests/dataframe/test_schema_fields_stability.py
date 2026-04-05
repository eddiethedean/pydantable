from __future__ import annotations


def test_schema_fields_order_follows_model_annotations() -> None:
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int
        city: str

    # Deliberately provide columns out of order; schema should follow annotations.
    df = Users({"city": ["x"], "id": [1], "age": [10]})
    assert list(df.schema_fields().keys()) == ["id", "age", "city"]


def test_schema_fields_order_is_stable_across_simple_transforms() -> None:
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int
        city: str

    df = Before({"city": ["x"], "id": [1], "age": [10]})

    renamed = df.rename({"age": "years"})
    assert list(renamed.schema_fields().keys()) == ["id", "years", "city"]

    dropped = renamed.drop("city")
    assert list(dropped.schema_fields().keys()) == ["id", "years"]

    selected = renamed.select("years", "id")
    # `select` order should match call order when literals are provided.
    assert list(selected.schema_fields().keys()) == ["years", "id"]
