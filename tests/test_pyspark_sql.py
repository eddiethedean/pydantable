"""PySpark UI and ``sql.functions`` contracts.

Parity intent is summarized in ``docs/PYSPARK_PARITY.md``; add tests alongside
new façade APIs rather than chasing full Spark coverage here.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from conftest import assert_table_eq_sorted
from pydantable.pyspark import DataFrame, DataFrameModel
from pydantable.pyspark.sql import (
    ArrayType,
    BooleanType,
    Column,
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    annotation_to_data_type,
)
from pydantable.pyspark.sql import functions as F
from pydantable.schema import Schema


def test_pyspark_package_imports_regression() -> None:
    from pydantable import pyspark as ps

    assert hasattr(ps, "DataFrame")
    assert hasattr(ps, "DataFrameModel")
    assert hasattr(ps, "sql")


def test_sql_functions_lit_and_col_with_dtype() -> None:
    class S(Schema):
        x: int
        y: int | None

    df = DataFrame[S]({"x": [1, 2], "y": [10, None]})
    doubled = df.withColumn("z", F.col("x", dtype=int) * F.lit(2))
    out = doubled.select("x", "z").collect(as_lists=True)
    assert_table_eq_sorted(out, {"x": [1, 2], "z": [2, 4]}, keys=["x"])


def test_sql_col_without_dtype_raises() -> None:
    with pytest.raises(TypeError, match="dtype"):
        F.col("x")


def test_column_type_alias_is_expr() -> None:
    from pydantable.expressions import Expr

    assert Column is Expr


def test_dataframe_where_and_columns_schema() -> None:
    class S(Schema):
        id: int
        v: int

    df = DataFrame[S]({"id": [1, 2], "v": [10, 20]})
    assert df.columns == ["id", "v"]
    assert isinstance(df.schema, StructType)
    assert "id" in df.schema.names
    filtered = df.where(df.v > 15)
    assert filtered.collect(as_lists=True) == {"id": [2], "v": [20]}


def test_dataframe_model_with_column_where() -> None:
    class M(DataFrameModel):
        id: int
        v: int

    m = M({"id": [1, 2], "v": [10, 20]})
    m1 = m.withColumn("w", m.v + 1)
    m2 = m1.where(m1.w > 20)
    assert m2.collect(as_lists=True) == {"id": [2], "v": [20], "w": [21]}


def test_types_to_annotation() -> None:
    assert IntegerType().to_annotation() is int
    assert IntegerType(nullable=True).to_annotation() == int | None
    assert StringType().to_annotation() is str
    assert DoubleType().to_annotation() is float
    assert BooleanType().to_annotation() is bool


def test_annotation_to_data_type_nested_model() -> None:
    class Inner(Schema):
        x: int

    class Outer(Schema):
        id: int
        inner: Inner

    dt = annotation_to_data_type(Outer)
    assert isinstance(dt, StructType)
    assert len(dt.fields) == 2
    assert dt.fields[0].name == "id"
    assert isinstance(dt.fields[0].dataType, IntegerType)
    assert dt.fields[1].name == "inner"
    assert isinstance(dt.fields[1].dataType, StructType)


def test_annotation_to_data_type_deeply_nested_model() -> None:
    class Leaf(Schema):
        z: str

    class Mid(Schema):
        y: int
        leaf: Leaf

    class Root(Schema):
        id: int
        mid: Mid

    dt = annotation_to_data_type(Root)
    assert isinstance(dt, StructType)
    assert dt.fields[1].name == "mid"
    mid = dt.fields[1].dataType
    assert isinstance(mid, StructType)
    assert mid.fields[1].name == "leaf"
    assert isinstance(mid.fields[1].dataType, StructType)
    assert mid.fields[1].dataType.fields[0].name == "z"


def test_annotation_to_data_type_list_int() -> None:
    dt = annotation_to_data_type(list[int])
    assert isinstance(dt, ArrayType)
    assert isinstance(dt.element_type, IntegerType)


def test_annotation_to_data_type_list_nested_and_optional() -> None:
    nested = annotation_to_data_type(list[list[int]])
    assert isinstance(nested, ArrayType)
    assert isinstance(nested.element_type, ArrayType)
    assert isinstance(nested.element_type.element_type, IntegerType)

    opt = annotation_to_data_type(list[str] | None)
    assert isinstance(opt, ArrayType)
    assert isinstance(opt.element_type, StringType)
    assert opt.nullable is True


def test_annotation_to_data_type_roundtrip() -> None:
    assert isinstance(annotation_to_data_type(int), IntegerType)
    assert isinstance(annotation_to_data_type(int | None), IntegerType)
    assert annotation_to_data_type(int | None).nullable is True


def test_functions_coalesce_requires_args() -> None:
    with pytest.raises(TypeError, match="at least one"):
        F.coalesce()


def test_functions_when_cast_isin_between_concat_substring_length() -> None:
    class S(Schema):
        x: int
        name: str

    df = DataFrame[S]({"x": [1, 2, 3], "name": ["hi", "there", "bob"]})

    w = (
        F.when(F.col("x", dtype=int) == 1, F.lit("one"))
        .when(F.col("x", dtype=int) == 2, F.lit("two"))
        .otherwise(F.lit("many"))
    )
    out_w = df.withColumn("w", w).select("x", "w").collect(as_lists=True)
    assert_table_eq_sorted(
        out_w, {"x": [1, 2, 3], "w": ["one", "two", "many"]}, keys=["x"]
    )

    out_cast = (
        df.withColumn("xf", F.cast(F.col("x", dtype=int), float))
        .select("x", "xf")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out_cast, {"x": [1, 2, 3], "xf": [1.0, 2.0, 3.0]}, keys=["x"]
    )

    out_isin = (
        df.withColumn("inq", F.isin(F.col("x", dtype=int), 1, 3))
        .select("x", "inq")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out_isin, {"x": [1, 2, 3], "inq": [True, False, True]}, keys=["x"]
    )

    out_bet = (
        df.withColumn("b", F.between(F.col("x", dtype=int), F.lit(2), F.lit(3)))
        .select("x", "b")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out_bet, {"x": [1, 2, 3], "b": [False, True, True]}, keys=["x"]
    )

    out_cat = (
        df.withColumn(
            "c",
            F.concat(
                F.col("name", dtype=str),
                F.lit("_"),
                F.cast(F.col("x", dtype=int), str),
            ),
        )
        .select("x", "c")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out_cat, {"x": [1, 2, 3], "c": ["hi_1", "there_2", "bob_3"]}, keys=["x"]
    )

    out_sub = (
        df.withColumn("s", F.substring(F.col("name", dtype=str), F.lit(1), F.lit(2)))
        .select("x", "s")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(
        out_sub, {"x": [1, 2, 3], "s": ["hi", "th", "bo"]}, keys=["x"]
    )

    out_len = (
        df.withColumn("ln", F.length(F.col("name", dtype=str)))
        .select("x", "ln")
        .collect(as_lists=True)
    )
    assert_table_eq_sorted(out_len, {"x": [1, 2, 3], "ln": [2, 5, 3]}, keys=["x"])


def test_functions_coalesce_isnull_orderby_limit() -> None:
    class S(Schema):
        x: int | None
        y: int

    df = DataFrame[S]({"x": [None, 2, 1], "y": [10, 20, 30]})
    filled = df.withColumn(
        "z",
        F.coalesce(F.col("x", dtype=int | None), F.col("y", dtype=int)),
    )
    out_z = filled.select("z").collect(as_lists=True)
    assert_table_eq_sorted(out_z, {"z": [10, 2, 1]}, keys=["z"])

    null_x = df.where(F.isnull(F.col("x", dtype=int | None)))
    assert null_x.collect(as_lists=True) == {"x": [None], "y": [10]}

    sorted_df = df.orderBy("y", ascending=False).limit(2)
    assert_table_eq_sorted(
        sorted_df.collect(as_lists=True), {"x": [1, 2], "y": [30, 20]}, keys=["y"]
    )

    renamed = df.withColumnRenamed("y", "yy")
    assert renamed.collect(as_lists=True) == {"x": [None, 2, 1], "yy": [10, 20, 30]}

    dropped = df.drop("y")
    assert dropped.collect(as_lists=True) == {"x": [None, 2, 1]}

    dup = DataFrame[S]({"x": [1, 1], "y": [1, 1]})
    assert dup.distinct().collect(as_lists=True) == {"x": [1], "y": [1]}


def test_aggregate_functions_global_sum_in_select() -> None:
    class S(Schema):
        v: int

    df = DataFrame[S]({"v": [1, 2, 3]})
    out = df.select(F.sum(F.col("v", dtype=int))).collect(as_lists=True)
    assert out == {"sum_v": [6]}


def test_aggregate_count_star_global_select() -> None:
    """``F.count()`` with no column is ``count(*)`` / row count (0.8.0)."""
    class S(Schema):
        v: int

    df = DataFrame[S]({"v": [1, 2, 3]})
    assert df.select(F.count()).collect(as_lists=True) == {"row_count": [3]}


def test_aggregate_count_star_vs_count_column_with_nulls() -> None:
    class S(Schema):
        v: int | None

    df = DataFrame[S]({"v": [1, None, 3]})
    assert df.select(F.count()).collect(as_lists=True) == {"row_count": [3]}
    assert df.select(F.count(F.col("v", dtype=int | None))).collect(
        as_lists=True
    ) == {"count_v": [2]}


def test_aggregate_sum_requires_column() -> None:
    with pytest.raises(TypeError):
        F.sum()  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        F.avg()  # type: ignore[call-arg]


def test_drop_duplicates_subset_keeps_first_row_per_key() -> None:
    class S(Schema):
        a: int
        b: int

    df = DataFrame[S]({"a": [1, 1, 2], "b": [1, 2, 2]})
    out = df.dropDuplicates(["a"]).collect(as_lists=True)
    assert_table_eq_sorted(out, {"a": [1, 2], "b": [1, 2]}, keys=["a"])


def test_functions_to_date_year_month_on_datetime() -> None:
    class S(Schema):
        ts: datetime

    df = DataFrame[S]({"ts": [datetime(2024, 6, 15, 12, 30, 45)]})
    ts = F.col("ts", dtype=datetime)
    out = (
        df.withColumn("d", F.to_date(ts))
        .withColumn("y", F.year(ts))
        .withColumn("mo", F.month(ts))
        .collect(as_lists=True)
    )
    assert out["d"] == [date(2024, 6, 15)]
    assert out["y"] == [2024]
    assert out["mo"] == [6]


def test_functions_to_date_string_with_format_uses_strptime() -> None:
    """PySpark ``to_date(col, format=...)`` maps to ``strptime`` (0.7.0)."""
    class S(Schema):
        s: str

    df = DataFrame[S]({"s": ["2024-06-01"]})
    s = F.col("s", dtype=str)
    out = df.withColumn("d", F.to_date(s, format="%Y-%m-%d")).collect(as_lists=True)
    assert out["d"] == [date(2024, 6, 1)]


def test_functions_unix_timestamp_seconds_and_ms() -> None:
    import calendar

    class S(Schema):
        ts: datetime

    ts = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    df = DataFrame[S]({"ts": [ts]})
    col = F.col("ts", dtype=datetime)
    sec = df.withColumn("u", F.unix_timestamp(col, unit="seconds")).collect(
        as_lists=True
    )
    ms = df.withColumn("u", F.unix_timestamp(col, unit="ms")).collect(as_lists=True)
    exp = int(calendar.timegm((2020, 1, 1, 12, 0, 0, 0, 0, 0)))
    assert sec["u"] == [exp]
    assert ms["u"] == [exp * 1000]


def test_functions_nanosecond_on_datetime() -> None:
    class S(Schema):
        ts: datetime

    df = DataFrame[S]({"ts": [datetime(2024, 1, 1, 0, 0, 0, 500000)]})
    out = df.withColumn("ns", F.nanosecond(F.col("ts", dtype=datetime))).collect(
        as_lists=True
    )
    assert out["ns"] == [500_000_000]


def test_functions_avg_mean_global_select_match() -> None:
    class S(Schema):
        v: int

    df = DataFrame[S]({"v": [10, 20, 30]})
    out_avg = df.select(F.avg(F.col("v", dtype=int))).collect(as_lists=True)
    out_mean = df.select(F.mean(F.col("v", dtype=int))).collect(as_lists=True)
    assert out_avg == out_mean == {"mean_v": [20.0]}


def test_window_functions_reexported_from_sql_functions() -> None:
    from pydantable.pyspark.sql import Window

    class S(Schema):
        g: int
        v: int

    df = DataFrame[S]({"g": [1, 1], "v": [1, 2]})
    w = Window.partitionBy("g").orderBy("v", ascending=True)
    out = df.withColumn("rn", F.row_number().over(w)).collect(as_lists=True)
    assert out["rn"] == [1, 2]
