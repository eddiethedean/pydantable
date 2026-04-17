from __future__ import annotations

import pytest
from pydantable.mongo_dataframe import MongoDataFrame
from pydantable.schema import Schema

mongomock = pytest.importorskip("mongomock")
pytest.importorskip("entei_core")


class Row(Schema):
    id: int
    label: str


def test_mongo_dataframe_match_pushdown_filters_documents() -> None:
    coll = mongomock.MongoClient().db.items
    coll.insert_many(
        [
            {"id": 1, "label": "a"},
            {"id": 2, "label": "b"},
            {"id": 3, "label": "c"},
        ]
    )
    df = MongoDataFrame[Row].from_collection(coll, fields=["id", "label"])
    out = df.match({"id": 2}).to_dict()
    assert out == {"id": [2], "label": ["b"]}


def test_mongo_dataframe_project_pushdown_updates_schema_and_values() -> None:
    coll = mongomock.MongoClient().db.items2
    coll.insert_many([{"id": 1, "label": "a"}])
    df = MongoDataFrame[Row].from_collection(coll, fields=["id", "label"])
    slim = df.project(["id"])
    assert slim.to_dict() == {"id": [1]}
