from __future__ import annotations

from typing import ClassVar

import pytest

beanie = pytest.importorskip("beanie")


class _Doc:
    def __init__(self, **data):
        self._data = data

    def model_dump(self, by_alias: bool = False):
        # simulate Beanie/Pydantic dump with alias for id
        d = dict(self._data)
        if by_alias and "id" in d and "_id" not in d:
            d["_id"] = d.pop("id")
        return d


class _Query:
    def __init__(self, docs):
        self._docs = docs
        self._projection = None

    def project(self, model):
        self._projection = model
        return self

    async def to_list(self):
        # naive projection: keep declared fields only
        if self._projection is not None:
            names = list(self._projection.model_fields.keys())
            out = []
            for d in self._docs:
                base = d.model_dump(by_alias=True)
                proj = {}
                for k in names:
                    fi = self._projection.model_fields[k]
                    src = getattr(fi, "alias", None) or k
                    proj[k] = base.get(src)
                out.append(_Doc(**proj))
            return out
        return list(self._docs)

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration from None


class _DocCls:
    __name__ = "MyDoc"
    model_fields: ClassVar[dict[str, object]] = {
        "id": type("F", (), {"annotation": object})(),
        "x": type("F", (), {"annotation": int})(),
    }

    @classmethod
    def find(cls, criteria=None, fetch_links=False):
        _ = (criteria, fetch_links)
        return _Query([_Doc(id=1, x=10), _Doc(id=2, x=20)])

    @classmethod
    def find_all(cls, fetch_links=False):
        _ = fetch_links
        return _Query([_Doc(id=1, x=10), _Doc(id=2, x=20)])


@pytest.mark.asyncio
async def test_afetch_beanie_flattens_and_normalizes_id():
    from pydantable.io.beanie import afetch_beanie

    cols = await afetch_beanie(_DocCls, flatten=True, id_column="id")
    assert cols["id"] == [1, 2]
    assert cols["x"] == [10, 20]


@pytest.mark.asyncio
async def test_afetch_beanie_fields_projection_preserves_order():
    from pydantable.io.beanie import afetch_beanie

    cols = await afetch_beanie(_DocCls, fields=["x", "id"], id_column="id")
    assert list(cols.keys()) == ["x", "id"]
    assert cols["x"] == [10, 20]
    assert cols["id"] == [1, 2]


@pytest.mark.asyncio
async def test_afetch_beanie_rejects_projection_and_fields_together() -> None:
    from pydantable.io.beanie import afetch_beanie

    with pytest.raises(TypeError, match="only one of projection_model= or fields="):
        await afetch_beanie(_DocCls, projection_model=object(), fields=["x"])


@pytest.mark.asyncio
async def test_afetch_beanie_rejects_query_without_project_support() -> None:
    from pydantable.io.beanie import afetch_beanie

    class NoProjectQuery:
        async def to_list(self):
            return []

    with pytest.raises(TypeError, match=r"project"):
        await afetch_beanie(NoProjectQuery(), projection_model=object())


@pytest.mark.asyncio
async def test_afetch_beanie_empty_result_is_empty_dict() -> None:
    from pydantable.io.beanie import afetch_beanie

    class EmptyQuery(_Query):
        async def to_list(self):
            return []

    cols = await afetch_beanie(EmptyQuery([]))
    assert cols == {}


@pytest.mark.asyncio
async def test_aiter_beanie_batches():
    from pydantable.io.beanie import aiter_beanie

    batches = []
    async for b in aiter_beanie(_DocCls, batch_size=1, id_column="id"):
        batches.append(b)
    assert len(batches) == 2
    assert batches[0]["id"] == [1]
    assert batches[1]["id"] == [2]


@pytest.mark.asyncio
async def test_aiter_beanie_rejects_non_positive_batch_size() -> None:
    from pydantable.io.beanie import aiter_beanie

    with pytest.raises(ValueError, match="positive"):
        async for _ in aiter_beanie(_DocCls, batch_size=0):
            pass


@pytest.mark.asyncio
async def test_aiter_beanie_rejects_projection_and_fields_together() -> None:
    from pydantable.io.beanie import aiter_beanie

    with pytest.raises(TypeError, match="only one of projection_model= or fields="):
        async for _ in aiter_beanie(_DocCls, projection_model=object(), fields=["x"]):
            pass


@pytest.mark.asyncio
async def test_aiter_beanie_rejects_query_criteria_misuse() -> None:
    from pydantable.io.beanie import aiter_beanie

    q = _DocCls.find({"x": 10})
    with pytest.raises(TypeError, match=r"criteria="):
        async for _ in aiter_beanie(q, criteria={"x": 20}):
            pass


@pytest.mark.asyncio
async def test_aiter_beanie_query_without_project_support_errors() -> None:
    from pydantable.io.beanie import aiter_beanie

    class NoProjectQuery:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    with pytest.raises(TypeError, match=r"project"):
        async for _ in aiter_beanie(NoProjectQuery(), projection_model=object()):
            pass


@pytest.mark.asyncio
async def test_afetch_beanie_query_rejects_criteria() -> None:
    from pydantable.io.beanie import afetch_beanie

    q = _DocCls.find({"x": 10})
    with pytest.raises(TypeError, match=r"criteria="):
        await afetch_beanie(q, criteria={"x": 20})


@pytest.mark.asyncio
async def test_awrite_beanie_ordered_and_unordered() -> None:
    from pydantable.io.beanie import awrite_beanie

    class Doc:
        def __init__(self, **row):
            self.row = row

        async def insert(self, **kwargs):
            _ = kwargs
            if self.row.get("x") == 2:
                raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await awrite_beanie(Doc, {"x": [1, 2, 3]}, ordered=True, chunk_size=10)

    n = await awrite_beanie(Doc, {"x": [1, 2, 3]}, ordered=False, chunk_size=10)
    assert n == 2
