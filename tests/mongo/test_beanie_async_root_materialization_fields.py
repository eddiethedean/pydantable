from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_beanie_async_root_materialization_passes_fields_none() -> None:
    from pydantable.mongo_dataframe import BeanieAsyncRoot
    from pydantable.mongo_dataframe_engine import _amaterialize_root_data

    root = BeanieAsyncRoot(document_or_query=type("Doc", (), {}), fields=None)

    mock = AsyncMock(return_value={"x": [1]})
    with patch("pydantable.io.beanie.afetch_beanie", mock):
        out = await _amaterialize_root_data(root)

    assert out == {"x": [1]}
    _args, kwargs = mock.call_args
    assert kwargs["fields"] is None


@pytest.mark.asyncio
async def test_beanie_async_root_materialization_passes_fields_list() -> None:
    from pydantable.mongo_dataframe import BeanieAsyncRoot
    from pydantable.mongo_dataframe_engine import _amaterialize_root_data

    root = BeanieAsyncRoot(document_or_query=type("Doc", (), {}), fields=("a", "b"))

    mock = AsyncMock(return_value={"a": [1], "b": [2]})
    with patch("pydantable.io.beanie.afetch_beanie", mock):
        out = await _amaterialize_root_data(root)

    assert out == {"a": [1], "b": [2]}
    _args, kwargs = mock.call_args
    assert kwargs["fields"] == ["a", "b"]
