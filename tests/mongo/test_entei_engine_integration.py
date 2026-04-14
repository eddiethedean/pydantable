"""Integration tests: ``entei-core`` as a pydantable ``ExecutionEngine`` (mongomock)."""

from __future__ import annotations

import asyncio

import mongomock
import pydantable
import pytest
from pydantable import Schema
from pydantable.engine import NativePolarsEngine, native_engine_capabilities
from pydantable.engine.protocols import ExecutionEngine, PlanExecutor, SinkWriter
from pydantable.schema import field_types_for_rust, schema_field_types
from typing_extensions import get_protocol_members

pytest.importorskip("entei_core")

from entei_core import MongoRoot
from pydantable.mongo_entei import (
    EnteiDataFrame,
    EnteiDataFrameModel,
    EnteiPydantableEngine,
)


class Row(Schema):
    x: int
    y: str | None = None


class RowEnteiModel(EnteiDataFrameModel):
    """Concrete model; bridge class is not ``DataFrameModel[T]``-subscriptable."""

    x: int
    y: str | None = None


def test_entei_exposes_all_execution_engine_protocol_members() -> None:
    """Protocol drift guard for ``EnteiPydantableEngine`` vs ``ExecutionEngine``."""
    names = get_protocol_members(ExecutionEngine)
    eng = EnteiPydantableEngine()
    missing = [n for n in names if not hasattr(eng, n)]
    assert not missing, f"EnteiPydantableEngine missing protocol members: {missing}"


def test_entei_is_structural_execution_engine_plan_executor_sink_writer() -> None:
    eng = EnteiPydantableEngine()
    assert isinstance(eng, ExecutionEngine)
    assert isinstance(eng, PlanExecutor)
    assert isinstance(eng, SinkWriter)


def test_entei_capabilities_custom_backend_reflects_native_feature_flags() -> None:
    """``backend`` is ``custom``; flags mirror native (delegation)."""
    entei = EnteiPydantableEngine()
    native = native_engine_capabilities()
    c = entei.capabilities
    assert c.backend == "custom"
    assert c.extension_loaded == native.extension_loaded
    assert c.has_execute_plan == native.has_execute_plan
    assert c.has_async_execute_plan == native.has_async_execute_plan
    assert c.has_async_collect_plan_batches == native.has_async_collect_plan_batches
    assert c.has_sink_parquet == native.has_sink_parquet
    assert c.has_sink_csv == native.has_sink_csv
    assert c.has_sink_ipc == native.has_sink_ipc
    assert c.has_sink_ndjson == native.has_sink_ndjson
    assert c.has_collect_plan_batches == native.has_collect_plan_batches
    assert c.has_execute_join == native.has_execute_join
    assert c.has_execute_groupby_agg == native.has_execute_groupby_agg


def test_entei_rust_core_is_native_binding() -> None:
    entei = EnteiPydantableEngine()
    native = NativePolarsEngine()
    assert entei.rust_core is native.rust_core


def test_entei_async_flags_match_instance_methods() -> None:
    eng = EnteiPydantableEngine()
    caps = native_engine_capabilities()
    assert eng.has_async_execute_plan() == caps.has_async_execute_plan
    assert eng.has_async_collect_plan_batches() == caps.has_async_collect_plan_batches


def test_execute_plan_in_memory_dict_matches_native_polars() -> None:
    """Non-``MongoRoot`` data must pass through and match native execution."""
    fts = schema_field_types(Row)
    field_types = field_types_for_rust(fts)
    data = {"x": [3, 1, 2], "y": ["a", None, "c"]}
    entei = EnteiPydantableEngine()
    native = NativePolarsEngine()
    pe = entei.make_plan(field_types)
    pn = native.make_plan(field_types)
    out_e = entei.execute_plan(pe, data, as_python_lists=True)
    out_n = native.execute_plan(pn, data, as_python_lists=True)
    assert out_e == out_n


def test_execute_plan_mongo_root_materializes_like_column_dict() -> None:
    client = mongomock.MongoClient()
    coll = client.db.t
    coll.insert_many([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
    fts = schema_field_types(Row)
    field_types = field_types_for_rust(fts)
    entei = EnteiPydantableEngine()
    plan = entei.make_plan(field_types)
    root = MongoRoot(coll, fields=("x", "y"))
    out_root = entei.execute_plan(plan, root, as_python_lists=True)
    out_dict = entei.execute_plan(
        plan,
        {"x": [1, 2], "y": ["a", "b"]},
        as_python_lists=True,
    )
    assert out_root == out_dict


def test_pydantable_lazy_exports_alias_entei_core() -> None:
    assert pydantable.EnteiDataFrame is EnteiDataFrame
    assert pydantable.EnteiDataFrameModel is EnteiDataFrameModel
    assert pydantable.EnteiPydantableEngine is EnteiPydantableEngine
    assert pydantable.MongoRoot is MongoRoot


def test_entei_dataframe_sort_head_collect() -> None:
    client = mongomock.MongoClient()
    coll = client.db.items
    coll.insert_many([{"x": 3}, {"x": 1}, {"x": 2}])
    df = EnteiDataFrame[Row].from_collection(coll)
    out = df.sort("x", descending=True).head(2).collect(as_lists=True)
    assert out["x"] == [3, 2]


def test_entei_dataframe_model_from_collection_rows() -> None:
    client = mongomock.MongoClient()
    coll = client.db.items
    coll.insert_many([{"x": 10, "y": "z"}])
    m = RowEnteiModel.from_collection(coll)
    rows = m.rows()
    assert len(rows) == 1
    assert rows[0].x == 10
    assert rows[0].y == "z"


@pytest.mark.asyncio
async def test_entei_dataframe_acollect() -> None:
    client = mongomock.MongoClient()
    coll = client.db.items
    coll.insert_many([{"x": 7}])
    df = EnteiDataFrame[Row].from_collection(coll)
    out = await df.acollect(as_lists=True)
    assert out == {"x": [7], "y": [None]}


def test_async_execute_plan_matches_sync_for_mongo_root() -> None:
    client = mongomock.MongoClient()
    coll = client.db.t
    coll.insert_many([{"x": 5}])
    fts = schema_field_types(Row)
    field_types = field_types_for_rust(fts)
    eng = EnteiPydantableEngine()
    plan = eng.make_plan(field_types)
    root = MongoRoot(coll, fields=("x", "y"))

    async def _run() -> object:
        return await eng.async_execute_plan(plan, root, as_python_lists=True)

    sync_out = eng.execute_plan(plan, root, as_python_lists=True)
    async_out = asyncio.run(_run())
    assert sync_out == async_out
