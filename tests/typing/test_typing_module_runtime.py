from __future__ import annotations

from pathlib import Path
from typing import Any

import pydantable.typing as typing_mod
import pytest
from pydantable import DataFrameModel
from pydantable.io import export_parquet


class _UserDF(DataFrameModel):
    id: int


def test_typing_module_is_importable_and_exports_protocol() -> None:
    assert set(typing_mod.__all__) == {
        "DataFrameModelWithRow",
        "RowT",
        "SupportsLazyAsyncMaterialize",
    }
    assert typing_mod.DataFrameModelWithRow.__name__ == "DataFrameModelWithRow"


def test_supports_lazy_async_materialize_isinstance(tmp_path: Path) -> None:
    path = tmp_path / "t.pq"
    export_parquet(path, {"id": [1]})
    df = _UserDF({"id": [1]})
    adf = _UserDF.aread_parquet(path, trusted_mode="shape_only")
    assert isinstance(df, typing_mod.SupportsLazyAsyncMaterialize)
    assert isinstance(adf, typing_mod.SupportsLazyAsyncMaterialize)
    assert not isinstance(object(), typing_mod.SupportsLazyAsyncMaterialize)


@pytest.mark.parametrize(
    "bad",
    [
        [],
        {},
        1,
        "x",
        type("NoCollect", (), {})(),
    ],
)
def test_supports_lazy_async_materialize_rejects_without_acollect(
    bad: object,
) -> None:
    assert not isinstance(bad, typing_mod.SupportsLazyAsyncMaterialize)


def test_supports_lazy_async_materialize_chained_lazy_isinstance(
    tmp_path: Path,
) -> None:
    path = tmp_path / "t.pq"
    export_parquet(path, {"id": [1, 2]})
    adf = _UserDF.aread_parquet(path, trusted_mode="shape_only")
    chained = adf.select("id")
    assert isinstance(chained, typing_mod.SupportsLazyAsyncMaterialize)


async def test_async_helper_awaits_acollect_on_protocol_shaped_object() -> None:
    """Contract: helpers typed with the protocol await ``acollect`` (no engine)."""

    class _Fake:
        def __init__(self, payload: dict[str, list[Any]]) -> None:
            self._payload = payload

        def acollect(
            self,
            *,
            as_lists: bool = False,
            as_numpy: bool = False,
            as_polars: bool | None = None,
            streaming: bool | None = None,
            engine_streaming: bool | None = None,
            executor: Any = None,
        ) -> Any:
            async def _run() -> dict[str, list[Any]]:
                return self._payload

            return _run()

    async def materialize(m: typing_mod.SupportsLazyAsyncMaterialize[Any]) -> Any:
        return await m.acollect()

    want = {"id": [1]}
    got = await materialize(_Fake(want))
    assert got == want


def test_runtime_checkable_accepts_minimal_acollect_callable() -> None:
    """``isinstance`` only checks for a callable ``acollect`` (see Protocol docs)."""

    class _Duck:
        def acollect(
            self,
            *,
            as_lists: bool = False,
            as_numpy: bool = False,
            as_polars: bool | None = None,
            streaming: bool | None = None,
            engine_streaming: bool | None = None,
            executor: Any = None,
        ) -> Any:
            async def _coro() -> None:
                return None

            return _coro()

    assert isinstance(_Duck(), typing_mod.SupportsLazyAsyncMaterialize)
