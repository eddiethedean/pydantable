import pytest
from pydantable import DataFrame, Schema
from pydantable.expressions import ColumnRef


class User(Schema):
    id: int
    age: int


def test_with_columns_and_collect_python():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    assert df2.schema_fields()["age2"] is int

    result = df2.collect()
    assert result == {"id": [1, 2], "age": [20, 30], "age2": [40, 60]}


def test_select_and_filter():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    df2 = df.with_columns(age2=df.age * 2)
    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": int}

    df4 = df3.filter(df3.age2 > 40)
    result = df4.to_dict()
    assert result == {"id": [2], "age2": [60]}


def test_with_columns_rejects_unknown_referenced_columns():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(ValueError, match="references unknown column"):
        df.with_columns(
            bad=df.col("age") + 1,
            missing=ColumnRef(name="missing", dtype=int) + 1,
        )


def test_select_requires_at_least_one_column():
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(ValueError, match="requires at least one column"):
        df.select()


def test_select_rejects_multi_column_expression() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    expr = df.age + df.id
    with pytest.raises(
        TypeError,
        match=r"select\(\) accepts column names or a ColumnRef expression",
    ):
        df.select(expr)


def test_with_columns_none_requires_destination_type() -> None:
    class UserNullable(Schema):
        id: int
        age: int | None

    df = DataFrame[UserNullable]({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match=r"cannot infer destination type"):
        df.with_columns(new=None)


def test_p1_sort_unique_drop_rename_slice_concat() -> None:
    class UserNullable(Schema):
        id: int
        age: int | None
        country: str

    df = DataFrame[UserNullable](
        {
            "id": [3, 1, 2, 2],
            "age": [30, None, 20, 20],
            "country": ["CA", "US", "US", "US"],
        }
    )

    sorted_df = df.sort("id")
    assert sorted_df.collect()["id"] == [1, 2, 2, 3]

    unique_df = sorted_df.unique(subset=["id", "age", "country"])
    assert unique_df.collect()["id"] == [1, 2, 3]

    dropped = unique_df.drop("country")
    assert set(dropped.schema_fields().keys()) == {"id", "age"}

    renamed = dropped.rename({"age": "years"})
    assert set(renamed.schema_fields().keys()) == {"id", "years"}
    assert renamed.schema_fields()["years"] == int | None

    sliced = renamed.slice(1, 2)
    assert sliced.collect() == {"id": [2, 3], "years": [20, 30]}
    assert renamed.head(2).collect() == {"id": [1, 2], "years": [None, 20]}
    assert renamed.tail(2).collect() == {"id": [2, 3], "years": [20, 30]}

    left = renamed.select("id")
    right = renamed.select("id")
    vcat = DataFrame.concat([left, right], how="vertical")
    assert vcat.collect() == {"id": [1, 2, 3, 1, 2, 3]}

    left_h = renamed.select("id")
    right_h = renamed.select("years")
    hcat = DataFrame.concat([left_h, right_h], how="horizontal")
    assert hcat.collect() == {"id": [1, 2, 3], "years": [None, 20, 30]}


def test_p2_fill_drop_nulls_and_cast_predicates() -> None:
    class S(Schema):
        id: int
        age: int | None
        score: float | None

    df = DataFrame[S](
        {"id": [1, 2, 3], "age": [10, None, 30], "score": [None, 1.5, None]}
    )
    filled = df.fill_null(0, subset=["age"])
    assert filled.collect()["age"] == [10, 0, 30]
    assert filled.schema_fields()["age"] is int

    dropped = df.drop_nulls(subset=["age"])
    assert dropped.collect() == {"id": [1, 3], "age": [10, 30], "score": [None, None]}

    casted = df.with_columns(age_f=df.age.cast(float))
    assert casted.schema_fields()["age_f"] == float | None
    out = casted.with_columns(age_is_null=casted.age.is_null()).collect()
    assert out["age_is_null"] == [False, True, False]
