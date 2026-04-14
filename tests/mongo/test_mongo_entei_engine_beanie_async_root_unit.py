from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_amaterialize_root_data_beanie_async_root_uses_afetch_beanie(monkeypatch):
    from pydantable.mongo_entei import BeanieAsyncRoot
    from pydantable.mongo_entei_engine import _amaterialize_root_data

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def _fake_afetch_beanie(document_or_query, **kwargs):
        calls.append(((document_or_query,), dict(kwargs)))
        return {"x": [1], "y": [2]}

    monkeypatch.setattr("pydantable.io.beanie.afetch_beanie", _fake_afetch_beanie)

    class Doc:
        pass

    root = BeanieAsyncRoot(
        document_cls=Doc,
        criteria={"x": 1},
        fields=("x",),
        fetch_links=True,
        nesting_depth=3,
        nesting_depths_per_field={"a": 1},
        flatten=False,
        id_column="_id",
    )
    out = await _amaterialize_root_data(root)
    assert out == {"x": [1], "y": [2]}
    assert calls == [
        (
            (Doc,),
            {
                "criteria": {"x": 1},
                "fields": ["x"],
                "fetch_links": True,
                "nesting_depth": 3,
                "nesting_depths_per_field": {"a": 1},
                "flatten": False,
                "id_column": "_id",
            },
        )
    ]


def test_sync_engine_rejects_beanie_async_root_for_sync_terminals():
    from pydantable.errors import UnsupportedEngineOperationError
    from pydantable.mongo_entei import BeanieAsyncRoot
    from pydantable.mongo_entei_engine import EnteiPydantableEngine

    class Doc:
        pass

    root = BeanieAsyncRoot(document_cls=Doc)
    eng = EnteiPydantableEngine()

    with pytest.raises(UnsupportedEngineOperationError, match="async materialization"):
        eng.execute_plan(plan=object(), data=root)

    with pytest.raises(UnsupportedEngineOperationError, match="async materialization"):
        eng.collect_batches(plan=object(), root_data=root)

    with pytest.raises(UnsupportedEngineOperationError, match="async materialization"):
        eng.write_csv(plan=object(), root_data=root, path="x.csv")

    with pytest.raises(UnsupportedEngineOperationError, match="async materialization"):
        eng.write_ipc(plan=object(), root_data=root, path="x.ipc")

    with pytest.raises(UnsupportedEngineOperationError, match="async materialization"):
        eng.write_parquet(plan=object(), root_data=root, path="x.parquet")
