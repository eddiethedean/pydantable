from __future__ import annotations

import os
import sys

import pytest

# SparkDataFrame needs raikou-core (``pydantable[spark]``), not only pyspark.
pytest.importorskip(
    "raikou_core",
    reason='pip install "pydantable[spark]"',
)


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
