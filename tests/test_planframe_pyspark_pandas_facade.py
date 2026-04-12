"""Smoke tests for PlanFrame APIs re-exported from pydantable pyspark/pandas facades."""

from __future__ import annotations


def test_pyspark_planframe_reexports_planframe_spark() -> None:
    from planframe.spark import SparkFrame as _SparkFrame
    from pydantable.pyspark import planframe as pf_spark

    assert pf_spark.SparkFrame is _SparkFrame
    assert pf_spark.functions is not None


def test_pydantable_pandas_exposes_planframe_pandas_module() -> None:
    import pydantable.pandas as pdp
    from planframe.pandas import PandasLikeFrame as _PLF

    assert pdp.planframe.PandasLikeFrame is _PLF
