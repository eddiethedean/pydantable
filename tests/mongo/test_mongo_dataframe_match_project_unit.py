from __future__ import annotations

import pytest
from pydantable.mongo_dataframe import MongoDataFrame
from pydantable.schema import Schema


class Row(Schema):
    id: int
    label: str


def test_mongo_match_rejects_unknown_column() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(KeyError, match="unknown"):
        df.match({"nope": 1})


def test_mongo_project_updates_schema_and_rejects_unknown() -> None:
    df = MongoDataFrame[Row]({"id": [1], "label": ["x"]})
    with pytest.raises(KeyError, match="unknown"):
        df.project(["nope"])
