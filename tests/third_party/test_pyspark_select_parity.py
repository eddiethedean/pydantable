from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from pydantable.pyspark import DataFrameModel

from tests._support.tables import assert_table_eq_sorted


class User(DataFrameModel):
    id: int
    name: str
    age: int | None


def test_pyspark_select_wrapper_methods() -> None:
    df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, None]})

    out = (
        df.withColumn("age_filled", df.age)
        .withColumns({"age2": df.age * 2})
        .withColumnRenamed("name", "name_new")
        .withColumnsRenamed({"age2": "age_twice"})
        .select("id", "name_new", "age_filled", "age_twice")
        .collect(as_lists=True)
    )
    assert out["id"] == [1, 2]
    assert out["name_new"] == ["a", "b"]


def test_pyspark_to_df_and_transform() -> None:
    df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    renamed = df.toDF("uid", "uname", "uage")
    assert list(renamed.schema_fields().keys()) == ["uid", "uname", "uage"]

    transformed = renamed.transform(
        lambda x: x.withColumn("uage2", x.uage * 2).select("uid", "uage2")
    )
    assert transformed.collect(as_lists=True) == {"uid": [1, 2], "uage2": [20, 40]}


def test_pyspark_to_df_arity_error() -> None:
    df = User({"id": [1], "name": ["a"], "age": [10]})
    with pytest.raises(ValueError, match="toDF\\(\\) expects 3 names, got 2"):
        df.toDF("a", "b")


def test_pyspark_select_typed_computed_projection() -> None:
    df = User({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    out = df.select_typed("id", age_doubled=df.age * 2).collect(as_lists=True)
    assert out == {"id": [1, 2], "age_doubled": [20, 40]}


class TemporalUser(DataFrameModel):
    id: int
    ts: datetime | None
    d: date | None
    dur: timedelta | None


def test_pyspark_select_temporal_wrappers_preserve_behavior() -> None:
    base = datetime(2024, 1, 1, 0, 0, 0)
    df = TemporalUser(
        {
            "id": [1, 2],
            "ts": [base, None],
            "d": [date(2024, 1, 1), None],
            "dur": [timedelta(minutes=5), None],
        }
    )
    out = (
        df.withColumn("is_ts_null", df.ts.is_null())
        .select_typed("id", "d", "dur", "is_ts_null")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out,
        {
            "id": [1, 2],
            "d": [date(2024, 1, 1), None],
            "dur": [timedelta(minutes=5), None],
            "is_ts_null": [False, True],
        },
        keys=["id"],
    )
