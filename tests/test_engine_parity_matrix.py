from __future__ import annotations

import inspect
from typing import Any

import pytest

from tests._support.engine_parity_matrix import MATRIX
from tests._support.engine_parity_scenarios import (
    BEHAVIORAL_SCENARIOS,
    ParityJoinRight,
    apply_parity_transform,
    make_in_memory_frame,
    parity_payload,
    parity_schema,
)
from tests._support.tables import assert_table_eq_sorted


@pytest.mark.parametrize("row", MATRIX, ids=lambda r: r.method)
def test_method_exists_on_engine_backed_dataframes(row) -> None:
    from pydantable.dataframe import DataFrame as CoreDataFrame
    from pydantable.mongo_dataframe import MongoDataFrame
    from pydantable.spark_dataframe import SparkDataFrame
    from pydantable.sql_dataframe import SqlDataFrame

    for cls in (CoreDataFrame, SqlDataFrame, MongoDataFrame, SparkDataFrame):
        assert hasattr(cls, row.method), f"{cls.__name__} missing {row.method!r}"


def _sql_sync_config() -> Any:
    pytest.importorskip("moltres_core")
    from moltres_core import EngineConfig

    return EngineConfig(dsn="sqlite:///:memory:")


def _build_engine_frames(method: str) -> dict[str, Any]:
    from pydantable.dataframe import DataFrame as CoreDataFrame

    schema = parity_schema(method)
    payload = parity_payload(method)
    frames: dict[str, Any] = {"core": CoreDataFrame[schema](payload)}

    scenario = BEHAVIORAL_SCENARIOS[method]
    if "mongo" in scenario["engines"]:
        pytest.importorskip("entei_core")
        from pydantable.mongo_dataframe import MongoDataFrame

        frames["mongo"] = MongoDataFrame[schema](payload)

    if "sql" in scenario["engines"]:
        from pydantable.sql_dataframe import SqlDataFrame

        frames["sql"] = make_in_memory_frame(
            SqlDataFrame[schema],
            payload,
            sql_config=_sql_sync_config(),
        )

    if method == "join":
        right_payload = {"k": [2, 1], "w": [200, 100]}
        frames["join_right"] = CoreDataFrame[ParityJoinRight](right_payload)
        if "mongo" in scenario["engines"]:
            from pydantable.mongo_dataframe import MongoDataFrame

            frames["mongo_join_right"] = MongoDataFrame[ParityJoinRight](right_payload)

    if method == "concat":
        tail = {"id": [2], "v": [20]}
        frames["concat_tail"] = CoreDataFrame[schema](tail)
        if "mongo" in scenario["engines"]:
            from pydantable.mongo_dataframe import MongoDataFrame

            frames["mongo_concat_tail"] = MongoDataFrame[schema](tail)

    return frames


def _materialize(df: Any) -> dict[str, list[Any]]:
    return df.collect(as_lists=True)


@pytest.mark.parametrize("row", MATRIX, ids=lambda r: r.method)
def test_engine_backed_dataframes_behavioral_parity(row) -> None:
    method = row.method
    scenario = BEHAVIORAL_SCENARIOS[method]
    expected = scenario["expected"]
    frames = _build_engine_frames(method)

    if method == "join":
        core_out = apply_parity_transform(
            method, frames["core"], join_right=frames["join_right"]
        )
        assert_table_eq_sorted(_materialize(core_out), expected, keys=["k"])
        if "mongo" in scenario["engines"]:
            mongo_out = apply_parity_transform(
                method,
                frames["mongo"],
                join_right=frames["mongo_join_right"],
            )
            assert_table_eq_sorted(_materialize(mongo_out), expected, keys=["k"])
        return

    if method == "concat":
        from pydantable.dataframe import DataFrame as CoreDataFrame

        core_out = CoreDataFrame.concat(
            [frames["core"], frames["concat_tail"]], how="vertical"
        )
        assert _materialize(core_out) == expected
        if "mongo" in scenario["engines"]:
            from pydantable.mongo_dataframe import MongoDataFrame

            mongo_out = MongoDataFrame.concat(
                [frames["mongo"], frames["mongo_concat_tail"]], how="vertical"
            )
            assert _materialize(mongo_out) == expected
        return

    core_out = apply_parity_transform(method, frames["core"])
    assert _materialize(core_out) == expected

    if "mongo" in scenario["engines"]:
        mongo_out = apply_parity_transform(method, frames["mongo"])
        assert _materialize(mongo_out) == expected

    if "sql" in scenario["engines"]:
        sql_out = apply_parity_transform(method, frames["sql"])
        assert _materialize(sql_out) == expected


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
