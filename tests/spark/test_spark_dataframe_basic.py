from __future__ import annotations

import os
import sys

import pytest
from pydantable import Schema

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
