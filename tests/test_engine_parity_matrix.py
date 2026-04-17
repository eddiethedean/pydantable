from __future__ import annotations

import inspect

import pytest

from tests._support.engine_parity_matrix import MATRIX


@pytest.mark.parametrize("row", MATRIX, ids=lambda r: r.method)
def test_method_exists_on_engine_backed_dataframes(row) -> None:
    from pydantable.dataframe import DataFrame as CoreDataFrame
    from pydantable.mongo_dataframe import MongoDataFrame
    from pydantable.spark_dataframe import SparkDataFrame
    from pydantable.sql_dataframe import SqlDataFrame

    for cls in (CoreDataFrame, SqlDataFrame, MongoDataFrame, SparkDataFrame):
        assert hasattr(cls, row.method), f"{cls.__name__} missing {row.method!r}"


def test_spark_engine_backed_rejects_typed_expr_for_filter() -> None:
    from pydantable.schema import Schema
    from pydantable.spark_dataframe import SparkDataFrame

    class Row(Schema):
        x: int

    df = SparkDataFrame[Row]({"x": [1, 2]})
    with pytest.raises(TypeError, match=r"expects a pyspark Column"):
        _ = df.filter(df.col("x") > 1)


def test_spark_ui_adapter_roundtrip_creates_wrapper_instances() -> None:
    from pydantable.schema import Schema
    from pydantable.spark_dataframe import SparkDataFrame

    class Row(Schema):
        x: int

    base = SparkDataFrame[Row]({"x": [1, 2]})
    ps = base.pyspark_ui()
    pd = base.pandas_ui()

    assert ps.__class__.__name__ == "SparkDataFrame"
    assert ps.__class__.__module__.endswith("pyspark.spark_dataframe")
    assert pd.__class__.__name__ == "SparkDataFrame"
    assert pd.__class__.__module__.endswith("pandas_spark_dataframe")


def test_mongo_ui_adapter_roundtrip_creates_wrapper_instances() -> None:
    from pydantable.mongo_dataframe import MongoDataFrame
    from pydantable.schema import Schema

    class Row(Schema):
        x: int

    base = MongoDataFrame[Row]({"x": [1, 2]})
    ps = base.pyspark_ui()
    pd = base.pandas_ui()

    assert ps.__class__.__name__ == "MongoDataFrame"
    assert ps.__class__.__module__.endswith("pyspark.mongo_dataframe")
    assert pd.__class__.__name__ == "MongoDataFrame"
    assert pd.__class__.__module__.endswith("pandas_mongo_dataframe")


def test_unsupported_engine_operation_helper_is_public() -> None:
    from pydantable.errors import unsupported_engine_operation

    sig = inspect.signature(unsupported_engine_operation)
    assert "operation" in sig.parameters
