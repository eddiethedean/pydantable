"""Tests for :mod:`pydantable.mongo_beanie` and :meth:`EnteiDataFrame.from_beanie`."""

from __future__ import annotations

import mongomock
import pytest

pytest.importorskip("entei_core")

from pydantable.mongo_beanie import sync_pymongo_collection


class _FakeBeanieDocument:
    """Minimal stand-in for a Beanie :class:`Document`.

    Only provides ``get_collection_name``.
    """

    @classmethod
    def get_collection_name(cls) -> str:
        return "items"


def test_sync_pymongo_collection_resolves_name() -> None:
    db = mongomock.MongoClient().shop
    coll = sync_pymongo_collection(_FakeBeanieDocument, db)
    assert coll.name == "items"
    assert coll.full_name == "shop.items"


def test_sync_pymongo_collection_rejects_non_database() -> None:
    with pytest.raises(TypeError, match="sync pymongo"):
        sync_pymongo_collection(_FakeBeanieDocument, object())


def test_sync_pymongo_collection_requires_get_collection_name() -> None:
    class Bad:
        pass

    db = mongomock.MongoClient().db
    with pytest.raises(TypeError, match="get_collection_name"):
        sync_pymongo_collection(Bad, db)


def test_sync_pymongo_collection_rejects_empty_name() -> None:
    class EmptyName:
        @classmethod
        def get_collection_name(cls) -> str:
            return ""

    db = mongomock.MongoClient().db
    with pytest.raises(TypeError, match="non-empty str"):
        sync_pymongo_collection(EmptyName, db)


def test_entei_from_beanie_collects_like_from_collection() -> None:
    import pydantable
    from pydantable import EnteiDataFrame, Schema
    from pydantable.engine import NativePolarsEngine

    if NativePolarsEngine is None:
        pytest.skip("pydantable-native not available")

    db = mongomock.MongoClient().app
    coll = db.items
    coll.insert_many([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])

    class Row(Schema):
        x: int
        y: str | None = None

    class ItemDoc(_FakeBeanieDocument):
        pass

    df = EnteiDataFrame[Row].from_beanie(ItemDoc, database=db)
    out = df.sort("x").collect(as_lists=True)
    assert out["x"] == [1, 2]
    assert out["y"] == ["a", "b"]

    assert pydantable.sync_pymongo_collection(ItemDoc, db).full_name == "app.items"
