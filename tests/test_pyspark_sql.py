from __future__ import annotations

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


def test_aggregate_function_stubs_raise() -> None:
    with pytest.raises(NotImplementedError, match="group_by"):
        F.sum()
    with pytest.raises(NotImplementedError, match="group_by"):
        F.avg()
