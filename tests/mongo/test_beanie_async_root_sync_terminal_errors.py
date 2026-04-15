from __future__ import annotations

import pytest

pytest.importorskip("entei_core")


def test_beanie_async_root_rejects_sync_collect() -> None:
    # This test is intentionally shallow: it verifies the engine boundary behavior
    # without requiring a live Mongo server or Beanie itself.
    from pydantable import Schema
    from pydantable.errors import UnsupportedEngineOperationError
    from pydantable.mongo_dataframe import BeanieAsyncRoot, MongoDataFrame

    class Row(Schema):
        x: int

    root = BeanieAsyncRoot(document_or_query=type("Doc", (), {}))
    # Need a real engine instance for `_from_plan` construction; the error is raised
    # by MongoPydantableEngine when it sees BeanieAsyncRoot on sync terminals.
    from pydantable.mongo_dataframe import MongoPydantableEngine

    df = MongoDataFrame[Row]._from_plan(  # type: ignore[attr-defined]
        root_data=root,
        root_schema_type=Row,
        current_schema_type=Row,
        rust_plan=object(),
        engine=MongoPydantableEngine(),
    )
    with pytest.raises(UnsupportedEngineOperationError):
        df.collect(as_lists=True)
