from datetime import date, datetime, timedelta

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


def test_p5_melt_and_unpivot() -> None:
    class S(Schema):
        id: int
        a: int | None
        b: int | None

    df = DataFrame[S]({"id": [1, 2], "a": [10, None], "b": [20, 30]})
    melted = df.melt(id_vars=["id"], value_vars=["a", "b"])
    out = melted.collect()
    assert out["id"] == [1, 1, 2, 2]
    assert out["variable"] == ["a", "b", "a", "b"]
    assert out["value"] == [10, 20, None, 30]
    assert melted.schema_fields()["variable"] is str
    assert melted.schema_fields()["value"] == int | None

    unpivoted = df.unpivot(index=["id"], on=["a", "b"])
    assert unpivoted.collect() == out


def test_p5_pivot_single_and_multi_values() -> None:
    class S(Schema):
        id: int
        key: str
        x: int | None
        y: float | None

    df = DataFrame[S](
        {
            "id": [1, 1, 2, 2],
            "key": ["A", "B", "A", "B"],
            "x": [10, 20, None, 40],
            "y": [1.0, 2.0, 3.0, None],
        }
    )
    p1 = df.pivot(
        index="id", columns="key", values="x", aggregate_function="sum"
    ).collect()
    assert p1["id"] == [1, 2]
    assert p1["A_sum"] == [10, None]
    assert p1["B_sum"] == [20, 40]

    p2 = df.pivot(
        index=["id"], columns="key", values=["x", "y"], aggregate_function="first"
    ).collect()
    assert p2["A_x_first"] == [10, None]
    assert p2["B_x_first"] == [20, 40]
    assert p2["A_y_first"] == [1.0, 3.0]
    assert p2["B_y_first"] == [2.0, None]


def test_p5_explode_unnest_raise_not_implemented_for_scalar_schema() -> None:
    class S(Schema):
        id: int
        name: str

    df = DataFrame[S]({"id": [1], "name": ["a"]})
    with pytest.raises(NotImplementedError, match=r"explode\(\) requires list-like"):
        df.explode("name")
    with pytest.raises(NotImplementedError, match=r"unnest\(\) requires struct-like"):
        df.unnest("name")


def test_p6_rolling_agg_and_dynamic_groupby() -> None:
    class TS(Schema):
        id: int
        ts: int
        v: int | None

    df = DataFrame[TS](
        {
            "id": [1, 1, 1, 2],
            "ts": [0, 3600, 7200, 0],
            "v": [10, None, 30, 5],
        }
    )
    rolled = df.rolling_agg(
        on="ts",
        column="v",
        window_size="2h",
        op="sum",
        out_name="v_roll_sum",
        by=["id"],
    )
    out = rolled.collect()
    assert out["v_roll_sum"] == [10, 10, 40, 5]

    dgb = df.group_by_dynamic("ts", every="1h", period="2h", by=["id"]).agg(
        v_sum=("sum", "v"), v_count=("count", "v")
    )
    d_out = dgb.collect()
    assert "v_sum" in d_out and "v_count" in d_out


def test_p6_expr_over_api_surface() -> None:
    df = DataFrame[User]({"id": [1, 2], "age": [20, 30]})
    expr = (df.age + 1).over(partition_by="id", order_by="age")
    out = df.with_columns(age2=expr).collect()
    assert out["age2"] == [21, 31]


def test_temporal_columns_and_literals_core_paths() -> None:
    class T(Schema):
        id: int
        ts: datetime
        d: date
        dur: timedelta

    df = DataFrame[T](
        {
            "id": [1, 2],
            "ts": [datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 2, 0, 0, 0)],
            "d": [date(2024, 1, 1), date(2024, 1, 2)],
            "dur": [timedelta(hours=1), timedelta(hours=2)],
        }
    )
    out = df.collect()
    assert out["ts"][0] == datetime(2024, 1, 1, 0, 0, 0)
    assert out["d"][1] == date(2024, 1, 2)
    assert out["dur"][0] == timedelta(hours=1)

    filtered = df.filter(df.ts > datetime(2024, 1, 1, 12, 0, 0)).collect()
    assert filtered["id"] == [2]


def test_temporal_groupby_and_join_paths() -> None:
    class L(Schema):
        id: int
        ts: datetime
        v: int | None

    class R(Schema):
        id: int
        ts: datetime
        tag: str

    left = DataFrame[L](
        {
            "id": [1, 1, 2],
            "ts": [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 1)],
            "v": [10, 20, None],
        }
    )
    right = DataFrame[R](
        {
            "id": [1, 2],
            "ts": [datetime(2024, 1, 2), datetime(2024, 1, 1)],
            "tag": ["a", "b"],
        }
    )
    joined = left.join(right, on=["id", "ts"], how="inner").collect()
    assert joined["id"] == [1, 2]

    grouped = (
        left.group_by("id").agg(ts_min=("min", "ts"), ts_max=("max", "ts")).collect()
    )
    assert sorted(grouped["id"]) == [1, 2]
