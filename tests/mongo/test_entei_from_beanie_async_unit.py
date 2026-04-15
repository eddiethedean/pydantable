"""Unit tests for :meth:`MongoDataFrame.from_beanie_async` and :class:`BeanieAsyncRoot`.

These tests avoid a live MongoDB server; async materialization uses a patched
``afetch_beanie`` where execution coverage is needed.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("entei_core")

from pydantable.engine import NativePolarsEngine
from pydantable.mongo_dataframe import (
    BeanieAsyncRoot,
    MongoDataFrame,
    MongoDataFrameModel,
)
from pydantable.schema import Schema


class Row(Schema):
    x: int
    label: str | None = None


class _Doc:
    """Stand-in Beanie document class (``find`` only, for constructor wiring tests)."""

    @classmethod
    def find(cls, criteria: Any = None, fetch_links: bool = False) -> Any:
        _ = (criteria, fetch_links)
        return object()


def _require_native() -> None:
    if NativePolarsEngine is None:
        pytest.skip("pydantable-native not available")


def test_from_beanie_async_requires_schema_parameterization() -> None:
    _require_native()

    with pytest.raises(TypeError, match="from_beanie_async"):
        MongoDataFrame.from_beanie_async(_Doc)  # type: ignore[call-arg]


def test_from_beanie_async_beanie_async_root_document_class_and_kwargs() -> None:
    _require_native()

    crit = object()
    ndpf = {"a": 2}
    df = MongoDataFrame[Row].from_beanie_async(
        _Doc,
        criteria=crit,
        fields=("x", "label"),
        fetch_links=True,
        nesting_depth=2,
        nesting_depths_per_field=ndpf,
        flatten=False,
        id_column="_id",
    )
    root = df._root_data
    assert isinstance(root, BeanieAsyncRoot)
    assert root.document_or_query is _Doc
    assert root.criteria is crit
    assert root.fields == ("x", "label")
    assert root.fetch_links is True
    assert root.nesting_depth == 2
    assert root.nesting_depths_per_field == ndpf
    assert root.flatten is False
    assert root.id_column == "_id"


def test_from_beanie_async_beanie_async_root_query_object() -> None:
    _require_native()

    query = object()
    df = MongoDataFrame[Row].from_beanie_async(
        query,
        criteria=None,
        fields=None,
        fetch_links=False,
    )
    root = df._root_data
    assert isinstance(root, BeanieAsyncRoot)
    assert root.document_or_query is query
    assert root.criteria is None


def test_from_beanie_async_fields_none_passes_through() -> None:
    _require_native()

    df = MongoDataFrame[Row].from_beanie_async(_Doc, fields=None)
    assert df._root_data.fields is None


def test_from_beanie_async_uses_explicit_engine_instance() -> None:
    _require_native()

    from pydantable.mongo_dataframe import MongoPydantableEngine

    eng = MongoPydantableEngine()
    df = MongoDataFrame[Row].from_beanie_async(_Doc, engine=eng)
    assert df._engine is eng


class RowMongoModel(MongoDataFrameModel):
    x: int
    label: str | None = None


def test_entei_dataframe_model_from_beanie_async_wraps_inner_root() -> None:
    _require_native()

    m = RowMongoModel.from_beanie_async(_Doc, criteria=None, fields=("x",))
    inner = m._df
    root = inner._root_data
    assert isinstance(root, BeanieAsyncRoot)
    assert root.document_or_query is _Doc
    assert root.fields == ("x",)


@pytest.mark.asyncio
async def test_from_beanie_async_ato_dict_with_patched_afetch(monkeypatch: Any) -> None:
    _require_native()

    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def _fake_afetch(
        document_or_query: Any,
        **kwargs: Any,
    ) -> dict[str, list[Any]]:
        calls.append(((document_or_query,), dict(kwargs)))
        return {"x": [7, 8], "label": ["p", "q"]}

    monkeypatch.setattr("pydantable.io.beanie.afetch_beanie", _fake_afetch)

    crit = object()
    df = MongoDataFrame[Row].from_beanie_async(
        _Doc,
        criteria=crit,
        fetch_links=True,
    )
    out = await df.ato_dict()
    assert out["x"] == [7, 8]
    assert out["label"] == ["p", "q"]
    assert len(calls) == 1
    assert calls[0][0][0] is _Doc
    assert calls[0][1]["fetch_links"] is True
    assert calls[0][1]["criteria"] is crit


@pytest.mark.asyncio
async def test_from_beanie_async_acollect_with_patched_afetch(monkeypatch: Any) -> None:
    _require_native()

    async def _fake_afetch(
        _document_or_query: Any,
        **_kwargs: Any,
    ) -> dict[str, list[Any]]:
        return {"x": [1], "label": [None]}

    monkeypatch.setattr("pydantable.io.beanie.afetch_beanie", _fake_afetch)

    df = MongoDataFrame[Row].from_beanie_async(_Doc)
    rows = await df.acollect()
    assert len(rows) == 1
    assert rows[0].model_dump() == {"x": 1, "label": None}


@pytest.mark.asyncio
async def test_from_beanie_async_query_plus_criteria_raises_from_afetch(
    monkeypatch: Any,
) -> None:
    """``afetch_beanie`` rejects ``criteria=`` with a pre-built query object."""
    _require_native()

    # Ensure we exercise real validation in pydantable.io.beanie (imports beanie).
    pytest.importorskip("beanie")

    df = MongoDataFrame[Row].from_beanie_async(
        object(),
        criteria={"illegal": True},
    )
    with pytest.raises(TypeError, match="criteria"):
        await df.ato_dict()


@pytest.mark.asyncio
async def test_from_beanie_async_schema_mismatch_after_fetch_raises(
    monkeypatch: Any,
) -> None:
    """Patched fetch missing schema columns must fail materialization."""

    _require_native()

    async def _wrong_shape(
        _document_or_query: Any,
        **_kwargs: Any,
    ) -> dict[str, list[Any]]:
        return {"unexpected_column": [1]}

    monkeypatch.setattr("pydantable.io.beanie.afetch_beanie", _wrong_shape)

    df = MongoDataFrame[Row].from_beanie_async(_Doc)
    with pytest.raises(KeyError, match="required column"):
        await df.ato_dict()


@pytest.mark.asyncio
async def test_from_beanie_async_afetch_error_propagates(monkeypatch: Any) -> None:
    _require_native()

    async def _boom(*_a: Any, **_k: Any) -> dict[str, list[Any]]:
        raise RuntimeError("simulated fetch failure")

    monkeypatch.setattr("pydantable.io.beanie.afetch_beanie", _boom)

    df = MongoDataFrame[Row].from_beanie_async(_Doc)
    with pytest.raises(RuntimeError, match="simulated fetch failure"):
        await df.ato_dict()
