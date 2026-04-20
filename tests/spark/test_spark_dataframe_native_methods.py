from __future__ import annotations

import pytest

pytest.importorskip("raikou_core")
pytest.importorskip("pyspark")

from pydantable.schema import Schema
from pydantable.spark_dataframe import SparkDataFrame


class Row(Schema):
    x: int


@pytest.mark.spark
def test_spark_dataframe_where_native_accepts_spark_column(spark) -> None:
    # Minimal smoke: verify method accepts a Spark Column and returns a new frame.
    sdf = spark.createDataFrame([{"x": 1}, {"x": 2}, {"x": 3}])
    df = SparkDataFrame[Row].from_spark_dataframe(sdf)
    cond = df.spark_col("x") > 1
    out = df.where_native(cond)
    assert out.to_dict() == {"x": [2, 3]}
