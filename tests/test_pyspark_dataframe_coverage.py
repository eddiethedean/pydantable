from __future__ import annotations

import pytest
from pydantable.pyspark import DataFrame, DataFrameModel
from pydantable.pyspark.dataframe import _text_show_table
from pydantable.schema import Schema


class User(DataFrameModel):
    id: int
    name: str
    age: int | None


class Row(Schema):
    id: int
    name: str
    age: int | None


def test_pyspark_dataframe_transform_rejects_non_dataframe() -> None:
    df = DataFrame[Row]({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(TypeError, match="return a DataFrame"):
        df.transform(lambda x: "not-a-df")  # type: ignore[arg-type, return-value]


def test_pyspark_model_transform_rejects_non_model() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(TypeError, match="DataFrameModel"):
        df.transform(lambda m: None)  # type: ignore[arg-type, return-value]


def test_pyspark_select_typed_requires_columns() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="select_typed"):
        df.select_typed()


def test_pyspark_order_by_requires_columns() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="orderBy"):
        df.orderBy()


def test_pyspark_order_by_ascending_length_mismatch() -> None:
    df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    with pytest.raises(ValueError, match="ascending length"):
        df.orderBy("id", "name", ascending=[True])


def test_pyspark_limit_rejects_negative() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="limit"):
        df.limit(-1)


def test_pyspark_union_all_alias() -> None:
    a = User({"id": [1], "name": ["x"], "age": [1]})
    b = User({"id": [2], "name": ["y"], "age": [2]})
    out = a.unionAll(b).collect(as_lists=True)
    assert sorted(out["id"]) == [1, 2]


def test_pyspark_drop_duplicates_with_and_without_subset() -> None:
    df = User(
        {
            "id": [1, 1, 2],
            "name": ["a", "a", "b"],
            "age": [10, 10, 20],
        }
    )
    u1 = df.dropDuplicates()
    assert len(u1.collect(as_lists=True)["id"]) == 2
    u2 = df.dropDuplicates(["name"])
    assert u2.collect(as_lists=True)["name"] == ["a", "b"]


def test_pyspark_order_by_rejects_maintain_order_kwarg() -> None:
    df = User(
        {
            "id": [1, 2, 3, 4],
            "name": ["a", "b", "c", "d"],
            "age": [10, 10, 10, 20],
        }
    )
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        _ = df.orderBy("age", maintain_order=True)  # type: ignore[call-arg]


def test_pyspark_dataframe_getitem_errors() -> None:
    df = DataFrame[Row]({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="non-empty"):
        df[[]]  # type: ignore[index]
    with pytest.raises(TypeError, match="supports"):
        df[object()]  # type: ignore[index]


def test_pyspark_show_truncates_long_cell(capsys: pytest.CaptureFixture[str]) -> None:
    long = "y" * 80
    df = User({"id": [1], "name": [long], "age": [1]})
    df.show(n=5, truncate=True)
    out = capsys.readouterr().out
    assert "…" in out or "..." in out or len(out) < len(long) * 2


def test_text_show_table_empty_and_truncation() -> None:
    assert _text_show_table({}, truncate=True) == "(empty)"
    wide = {"c": ["x" * 100]}
    txt = _text_show_table(wide, truncate=True)
    assert "…" in txt or "..." in txt


def test_pyspark_model_union_accepts_bare_dataframe() -> None:
    m = User({"id": [1], "name": ["a"], "age": [10]})
    raw = DataFrame[Row]({"id": [2], "name": ["b"], "age": [20]})
    out = m.union(raw).collect(as_lists=True)
    assert sorted(out["id"]) == [1, 2]
