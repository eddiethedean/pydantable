from __future__ import annotations


def test_pandas_facade_exports_spark_and_mongo_wrappers() -> None:
    from pydantable import pandas as pd

    assert pd.SparkDataFrame is not None
    assert pd.SparkDataFrameModel is not None
    assert pd.MongoDataFrame is not None
    assert pd.MongoDataFrameModel is not None


def test_pyspark_facade_exports_spark_and_mongo_wrappers() -> None:
    from pydantable import pyspark as ps

    assert ps.SparkDataFrame is not None
    assert ps.SparkDataFrameModel is not None
    assert ps.MongoDataFrame is not None
    assert ps.MongoDataFrameModel is not None


def test_pandas_wrappers_have_pandas_ui_methods() -> None:
    from pydantable.pandas_mongo_dataframe import MongoDataFrame as PandasMongoDataFrame
    from pydantable.pandas_spark_dataframe import SparkDataFrame as PandasSparkDataFrame

    assert hasattr(PandasSparkDataFrame, "assign")
    assert hasattr(PandasSparkDataFrame, "merge")
    assert hasattr(PandasMongoDataFrame, "assign")
    assert hasattr(PandasMongoDataFrame, "merge")


def test_pyspark_wrappers_have_pyspark_ui_methods() -> None:
    from pydantable.pyspark.mongo_dataframe import (
        MongoDataFrame as PySparkMongoDataFrame,
    )
    from pydantable.pyspark.spark_dataframe import (
        SparkDataFrame as PySparkSparkDataFrame,
    )

    assert hasattr(PySparkSparkDataFrame, "withColumn")
    assert hasattr(PySparkSparkDataFrame, "orderBy")
    assert hasattr(PySparkMongoDataFrame, "withColumn")
    assert hasattr(PySparkMongoDataFrame, "orderBy")


def test_pyspark_spark_wrapper_accepts_typed_expr_filter_and_withcolumn() -> None:
    from pydantable.pyspark import SparkDataFrame
    from pydantable.schema import Schema

    class Row(Schema):
        x: int

    df = SparkDataFrame[Row]({"x": [1, 2, 3]})
    out = df.filter(df.col("x") > 1).withColumn("y", df.col("x"))

    # We don't assert execution here (Spark extras may not be installed); just ensure
    # the chained plan object is created and has the expected schema fields.
    assert "x" in out.schema_fields()
    assert "y" in out.schema_fields()
