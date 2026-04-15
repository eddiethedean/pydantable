from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.network


def _mongo_uri() -> str | None:
    return os.environ.get("MONGO_URI") or os.environ.get("PYDANTABLE_TEST_MONGO_URI")


@pytest.fixture(scope="module")
def mongo_uri() -> str:
    uri = _mongo_uri()
    if not uri:
        pytest.skip(
            "Set MONGO_URI or PYDANTABLE_TEST_MONGO_URI to run real Mongo tests."
        )
    return uri


@pytest.fixture(scope="module")
def db_name() -> str:
    return f"pydantable_test_{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def _require_native_engine() -> None:
    # Lazy Entei needs the native engine.
    from pydantable.engine import native_engine_capabilities

    if not native_engine_capabilities().extension_loaded:
        pytest.skip("Native extension not installed; skipping real Mongo Entei tests.")


def test_real_mongo_sync_io_roundtrip(mongo_uri: str, db_name: str) -> None:
    pytest.importorskip("pymongo")
    from pydantable import fetch_mongo, iter_mongo, write_mongo
    from pymongo import MongoClient

    client = MongoClient(mongo_uri)
    db = client[db_name]
    db.drop_collection("items_sync")
    coll = db["items_sync"]

    n = write_mongo(coll, {"id": [1, 2, 3], "x": [10, 20, 30]})
    assert n == 3

    out = fetch_mongo(coll, sort=[("id", 1)], fields=["id", "x"])
    assert out == {"id": [1, 2, 3], "x": [10, 20, 30]}

    batches = list(iter_mongo(coll, batch_size=2, sort=[("id", 1)], fields=["id"]))
    assert batches == [{"id": [1, 2]}, {"id": [3]}]


@pytest.mark.asyncio
async def test_real_mongo_beanie_fetch_links_flattening(
    mongo_uri: str, db_name: str
) -> None:
    beanie = pytest.importorskip("beanie")
    _ = beanie

    from beanie import Document, Link, init_beanie
    from pydantable import afetch_beanie
    from pymongo import AsyncMongoClient

    class Door(Document):
        height: int

        class Settings:
            name = "doors"

    class House(Document):
        name: str
        door: Link[Door]

        class Settings:
            name = "houses"

    aclient = AsyncMongoClient(mongo_uri)
    adb = aclient[db_name]
    await init_beanie(database=adb, document_models=[Door, House])

    # Clean slate
    await adb["doors"].delete_many({})
    await adb["houses"].delete_many({})

    d1 = Door(height=2)
    await d1.insert()
    await House(name="alpha", door=d1).insert()

    d2 = Door(height=3)
    await d2.insert()
    await House(name="beta", door=d2).insert()

    # No projection: flatten expands linked docs into dot-path keys.
    cols = await afetch_beanie(House, fetch_links=True, flatten=True, id_column="id")
    assert cols["door.height"] == [2, 3]

    # With projection that includes `door`, Beanie may keep `door` as a single value;
    # pydantable does not expand it further in that mode.
    cols2 = await afetch_beanie(
        House,
        fetch_links=True,
        flatten=True,
        id_column="id",
        fields=["id", "name", "door"],
    )
    assert "door" in cols2
    assert "door.height" not in cols2

    await aclient.close()


@pytest.mark.asyncio
async def test_real_mongo_entei_from_beanie_async(mongo_uri: str, db_name: str) -> None:
    pytest.importorskip("entei_core")
    pytest.importorskip("beanie")

    from beanie import Document, init_beanie
    from pydantable import EnteiDataFrame, Schema
    from pymongo import AsyncMongoClient

    class Item(Document):
        name: str

        class Settings:
            name = "items_beanie_async"

    aclient = AsyncMongoClient(mongo_uri)
    adb = aclient[db_name]
    await init_beanie(database=adb, document_models=[Item])
    await adb["items_beanie_async"].delete_many({})

    await Item(name="alpha").insert()
    await Item(name="beta").insert()

    class Row(Schema):
        name: str

    df = EnteiDataFrame[Row].from_beanie_async(Item, criteria=(Item.name == "alpha"))
    out = await df.ato_dict()
    assert out == {"name": ["alpha"]}

    # Pre-built Beanie query object (same semantics as ``afetch_beanie``).
    q = Item.find(Item.name == "beta")
    df2 = EnteiDataFrame[Row].from_beanie_async(q)
    out2 = await df2.ato_dict()
    assert out2 == {"name": ["beta"]}

    await aclient.close()


@pytest.mark.asyncio
async def test_real_mongo_afetch_mongo_uses_native_async_driver(
    mongo_uri: str, db_name: str
) -> None:
    """PyMongo ``AsyncCollection`` uses native async I/O (not threads)."""
    pytest.importorskip("pymongo")
    from pydantable import (
        afetch_mongo,
        aiter_mongo,
        awrite_mongo,
        is_async_mongo_collection,
    )
    from pymongo.asynchronous.mongo_client import AsyncMongoClient

    client = AsyncMongoClient(mongo_uri)
    coll = client[db_name]["pydantable_afetch_native"]
    await coll.delete_many({})

    assert is_async_mongo_collection(coll) is True

    n = await awrite_mongo(coll, {"k": [1, 2, 3], "v": [10, 20, 30]})
    assert n == 3

    out = await afetch_mongo(coll, sort=[("k", 1)], skip=1, limit=1, fields=["k", "v"])
    assert out == {"k": [2], "v": [20]}

    batches = [b async for b in aiter_mongo(coll, batch_size=2)]
    assert sum(len(next(iter(b.values()))) for b in batches) == 3

    await client.aclose()
