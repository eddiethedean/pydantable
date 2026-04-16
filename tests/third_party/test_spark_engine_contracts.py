"""Contracts for :class:`~pydantable.spark_dataframe.SparkDataFrame` (no JVM)."""

from __future__ import annotations

import pytest


def test_spark_dataframe_from_spark_requires_schema_parameterization() -> None:
    from pydantable import SparkDataFrame

    with pytest.raises(TypeError, match=r"SparkDataFrame\[Schema\]"):
        SparkDataFrame.from_spark_dataframe(object())


def test_spark_dataframe_model_uses_spark_dataframe_class() -> None:
    from pydantable.spark_dataframe import SparkDataFrame, SparkDataFrameModel

    assert SparkDataFrameModel._dataframe_cls is SparkDataFrame
