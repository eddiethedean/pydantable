"""Tests for ``fetch_mongo`` / ``iter_mongo`` / ``write_mongo`` and async mirrors."""

from __future__ import annotations

import pytest

mongomock = pytest.importorskip("mongomock")
pytest.importorskip("pymongo")

from pydantable import (  # noqa: E402
    afetch_mongo,
    aiter_mongo,
    awrite_mongo,
    fetch_mongo,
    iter_mongo,
    write_mongo,
)


@pytest.fixture
def coll() -> mongomock.collection.Collection:
    client = mongomock.MongoClient()
    return client.db.test_coll


def test_fetch_mongo_empty(coll: mongomock.collection.Collection) -> None:
    assert fetch_mongo(coll) == {}


def test_write_fetch_roundtrip(coll: mongomock.collection.Collection) -> None:
    data = {"x": [1, 2], "y": ["a", "b"]}
    n = write_mongo(coll, data)
    assert n == 2
    out = fetch_mongo(coll, sort=[("x", 1)])
    assert out["x"] == [1, 2]
    assert out["y"] == ["a", "b"]


def test_iter_mongo_batches(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"k": i} for i in range(5)])
    batches = list(iter_mongo(coll, batch_size=2, sort=[("k", 1)]))
    assert len(batches) == 3
    assert sum(len(next(iter(b.values()))) for b in batches) == 5


def test_fetch_mongo_match(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"x": 1, "tag": "a"}, {"x": 2, "tag": "b"}])
    out = fetch_mongo(coll, match={"tag": "a"})
    assert out["x"] == [1]
    assert out["tag"] == ["a"]


@pytest.mark.asyncio
async def test_afetch_mongo(coll: mongomock.collection.Collection) -> None:
    coll.insert_one({"z": 7})
    out = await afetch_mongo(coll)
    assert out["z"] == [7]


@pytest.mark.asyncio
async def test_awrite_mongo(coll: mongomock.collection.Collection) -> None:
    n = await awrite_mongo(coll, {"a": [True]})
    assert n == 1
    assert coll.count_documents({}) == 1


@pytest.mark.asyncio
async def test_aiter_mongo(coll: mongomock.collection.Collection) -> None:
    coll.insert_many([{"i": 0}, {"i": 1}])
    batches: list[dict[str, list]] = []
    async for b in aiter_mongo(coll, batch_size=1, sort=[("i", 1)]):
        batches.append(b)
    assert len(batches) == 2


def test_pydantable_root_exports_mongo_io() -> None:
    import pydantable

    assert pydantable.fetch_mongo is fetch_mongo
    assert pydantable.write_mongo is write_mongo
