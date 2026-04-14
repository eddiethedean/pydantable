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
        pytest.skip("Set MONGO_URI or PYDANTABLE_TEST_MONGO_URI to run real Mongo tests.")
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
    from pymongo import MongoClient

    from pydantable import fetch_mongo, iter_mongo, write_mongo

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
async def test_real_mongo_beanie_fetch_links_flattening(mongo_uri: str, db_name: str) -> None:
    beanie = pytest.importorskip("beanie")
    _ = beanie

    from pymongo import AsyncMongoClient

    from pydantable import afetch_beanie

    from beanie import Document, Link, init_beanie

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

    from pymongo import AsyncMongoClient

    from pydantable import EnteiDataFrame, Schema

    from beanie import Document, init_beanie

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

    await aclient.close()

