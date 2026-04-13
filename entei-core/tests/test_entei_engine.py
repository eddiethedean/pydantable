"""Tests for entei-core Mongo materialization and EnteiDataFrame."""

from __future__ import annotations

import mongomock
from entei_core import EnteiDataFrame, EnteiPydantableEngine, MongoRoot
from entei_core._materialize import materialize_root_data, mongo_root_to_column_dict
from pydantable import Schema


class Row(Schema):
    x: int
    y: str | None


def test_mongo_root_to_column_dict_orders_fields() -> None:
    client = mongomock.MongoClient()
    coll = client.db.t
    coll.insert_many([{"x": 2, "y": "b"}, {"x": 1, "y": "a"}])
    got = mongo_root_to_column_dict(MongoRoot(coll))
    assert got["x"] == [2, 1]
    assert got["y"] == ["b", "a"]


def test_mongo_root_empty_with_explicit_fields() -> None:
    client = mongomock.MongoClient()
    coll = client.db.empty
    got = mongo_root_to_column_dict(MongoRoot(coll, fields=("x", "y")))
    assert got == {"x": [], "y": []}


def test_materialize_root_data_passthrough() -> None:
    d = {"a": [1]}
    assert materialize_root_data(d) is d


def test_entei_capabilities_backend_custom() -> None:
    eng = EnteiPydantableEngine()
    assert eng.capabilities.backend == "custom"


def test_entei_dataframe_from_collection_collect() -> None:
    client = mongomock.MongoClient()
    coll = client.db.items
    coll.insert_many([{"x": 3}, {"x": 4}])
    df = EnteiDataFrame[Row].from_collection(coll)
    out = df.collect(as_lists=True)
    assert out["x"] == [3, 4]


def test_entei_dataframe_select_filter() -> None:
    client = mongomock.MongoClient()
    coll = client.db.items
    coll.insert_many([{"x": 1}, {"x": 5}, {"x": 3}])
    df = EnteiDataFrame[Row].from_collection(coll)
    f = df.filter(df.x > 2)
    out = f.select("x").collect(as_lists=True)
    assert sorted(out["x"]) == [3, 5]
