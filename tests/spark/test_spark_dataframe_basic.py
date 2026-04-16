from __future__ import annotations

import os
import sys

import pytest
from pydantable import Schema

# SparkDataFrame needs raikou-core (``pydantable[spark]``), not only pyspark.
pytest.importorskip(
    "raikou_core",
    reason='pip install "pydantable[spark]"',
)

pytestmark = pytest.mark.spark


def _spark_session():
    pytest.importorskip("pyspark")
    # Force worker python to match the test interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("pydantable-spark-tests")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Spark is not available in this environment: {exc!r}")


@pytest.fixture(scope="session")
def spark():
    s = _spark_session()
    yield s
    s.stop()


class Row(Schema):
    x: int
    y: str


def test_from_spark_dataframe_select_filter(spark) -> None:
    from pydantable import SparkDataFrame

    sdf = spark.createDataFrame([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
    df = SparkDataFrame[Row].from_spark_dataframe(sdf)
    out = df.filter(df.spark_col("x") > 1).select("y").to_dict()
    assert out == {"y": ["b"]}


def test_spark_col_returns_pyspark_column(spark) -> None:
    pytest.importorskip("raikou_core")
    from pydantable import SparkDataFrame
    from pyspark.sql import Column

    sdf = spark.createDataFrame([{"x": 1, "y": "a"}])
    df = SparkDataFrame[Row].from_spark_dataframe(sdf)
    c = df.spark_col("x")
    assert isinstance(c, Column)


def test_filter_rejects_native_pydantable_expr(spark) -> None:
    pytest.importorskip("raikou_core")
    from pydantable import DataFrame, SparkDataFrame

    sdf = spark.createDataFrame([{"x": 1, "y": "a"}])
    dfp = SparkDataFrame[Row].from_spark_dataframe(sdf)
    core = DataFrame[Row]({"x": [1], "y": ["a"]})
    native_expr = core.x > 0
    with pytest.raises(TypeError, match="pyspark Column"):
        dfp.filter(native_expr)


def test_with_columns_accepts_spark_column(spark) -> None:
    pytest.importorskip("raikou_core")
    from pydantable import SparkDataFrame
    from pyspark.sql import functions as F

    sdf = spark.createDataFrame([{"x": 1, "y": "a"}])
    df = SparkDataFrame[Row].from_spark_dataframe(sdf)
    out = df.with_columns(z=F.lit(99)).select("x", "z").to_dict()
    assert out == {"x": [1], "z": [99]}
