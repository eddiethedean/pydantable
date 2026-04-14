from __future__ import annotations

import pytest

mongomock = pytest.importorskip("mongomock")
pytest.importorskip("pymongo")

from pydantable import fetch_mongo, iter_mongo, write_mongo


@pytest.fixture
def coll() -> mongomock.collection.Collection:
    client = mongomock.MongoClient()
    return client.db.more_cases


def test_fetch_mongo_fields_limits_and_orders_columns(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"a": 1, "b": 2}, {"a": 3}])
    out = fetch_mongo(coll, sort=[("a", 1)], fields=["b", "a"])
    assert list(out.keys()) == ["b", "a"]
    assert out["a"] == [1, 3]
    assert out["b"] == [2, None]


def test_fetch_mongo_limit_and_projection(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"k": 1, "drop": "x"}, {"k": 2, "drop": "y"}])
    out = fetch_mongo(coll, sort=[("k", 1)], limit=1, projection={"k": 1})
    # Mongo includes `_id` by default unless explicitly excluded.
    assert out["k"] == [1]
    assert "_id" in out


def test_iter_mongo_merges_keys_per_batch_sorted_union(coll: mongomock.collection.Collection) -> None:
    # First doc has only 'a', second doc introduces 'b' (same batch).
    coll.insert_many([{"i": 0, "a": 1}, {"i": 1, "b": 2}])
    batches = list(iter_mongo(coll, batch_size=10, sort=[("i", 1)]))
    assert len(batches) == 1
    b = batches[0]
    assert list(b.keys()) == ["_id", "a", "b", "i"]  # sorted union
    assert b["a"] == [1, None]
    assert b["b"] == [None, 2]


def test_iter_mongo_fields_fixes_schema_and_ignores_extra_keys(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"x": 1, "y": 2}, {"x": 3, "z": 4}])
    batches = list(iter_mongo(coll, batch_size=10, fields=["x"]))
    assert batches == [{"x": [1, 3]}]


def test_write_mongo_empty_is_noop(coll: mongomock.collection.Collection) -> None:
    assert write_mongo(coll, {}) == 0


def test_write_mongo_rejects_non_rectangular(coll: mongomock.collection.Collection) -> None:
    with pytest.raises(ValueError):
        write_mongo(coll, {"a": [1, 2], "b": [3]})

