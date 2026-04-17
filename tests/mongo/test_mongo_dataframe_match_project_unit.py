from __future__ import annotations

import pytest
from pydantable.mongo_dataframe import MongoDataFrame
from pydantable.schema import Schema


class Row(Schema):
    id: int
    label: str


def test_mongo_match_rejects_non_dict() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(TypeError, match="expects a dict"):
        df.match([("id", 1)])  # type: ignore[arg-type]


def test_mongo_match_rejects_unknown_column() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(KeyError, match="unknown"):
        df.match({"nope": 1})


def test_mongo_match_requires_collection_root() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(TypeError, match="collection roots"):
        df.match({"id": 1})


def test_mongo_project_requires_fields() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(ValueError, match="at least one field"):
        df.project([])


def test_mongo_project_rejects_unknown_column() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(KeyError, match="unknown"):
        df.project(["nope"])


def test_mongo_project_requires_collection_root() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(TypeError, match="collection roots"):
        df.project(["id"])
