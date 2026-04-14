from __future__ import annotations

import pytest
from pydantable import Schema


def test_dataframe_impl_class_getitem_rejects_non_model() -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    with pytest.raises(TypeError, match=r"DataFrame\[Schema\] expects"):
        _ = ImplDataFrame[123]  # type: ignore[index]


def test_dataframe_impl_init_requires_subscripted_schema() -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    with pytest.raises(TypeError, match="Use DataFrame\\[SchemaType\\]"):
        ImplDataFrame({"x": [1]})  # type: ignore[call-arg]


def test_dataframe_impl_sync_wrapper_delegates_to_lazy_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    class Row(Schema):
        x: int

    DF = ImplDataFrame[Row]
    called: dict[str, object] = {}

    def _read_csv(
        cls: type[ImplDataFrame],
        path: object,
        *,
        columns: list[str] | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: str | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: object | None = None,
        **scan_kwargs: object,
    ):
        called["fn"] = "read_csv"
        called["cls"] = cls
        called["path"] = path
        called["columns"] = columns
        called["engine_streaming"] = engine_streaming
        called["trusted_mode"] = trusted_mode
        called["scan_kwargs"] = dict(scan_kwargs)
        return DF({"x": [1]})

    monkeypatch.setattr("pydantable.dataframe._impl_lazy_sources.read_csv", _read_csv)
    out = DF.read_csv(
        "file.csv",
        columns=["x"],
        engine_streaming=True,
        trusted_mode="shape_only",
        some_kw=1,
    )
    assert out.collect(as_lists=True) == {"x": [1]}
    assert called["fn"] == "read_csv"
    assert called["cls"] is DF
    assert called["path"] == "file.csv"
    assert called["columns"] == ["x"]
    assert called["engine_streaming"] is True
    assert called["trusted_mode"] == "shape_only"
    assert called["scan_kwargs"] == {"some_kw": 1}


@pytest.mark.asyncio
async def test_dataframe_impl_async_wrapper_delegates_to_lazy_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    class Row(Schema):
        x: int

    DF = ImplDataFrame[Row]
    called: dict[str, object] = {}

    async def _aread_csv(
        cls: type[ImplDataFrame],
        path: object,
        *,
        columns: list[str] | None = None,
        executor: object | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: str | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: object | None = None,
        **scan_kwargs: object,
    ):
        called["fn"] = "aread_csv"
        called["cls"] = cls
        called["path"] = path
        called["columns"] = columns
        called["executor"] = executor
        called["engine_streaming"] = engine_streaming
        called["trusted_mode"] = trusted_mode
        called["scan_kwargs"] = dict(scan_kwargs)
        return DF({"x": [2]})

    monkeypatch.setattr("pydantable.dataframe._impl_lazy_sources.aread_csv", _aread_csv)
    out = await DF.aread_csv(
        "file.csv",
        columns=["x"],
        engine_streaming=False,
        trusted_mode="strict",
        another=2,
    )
    assert out.collect(as_lists=True) == {"x": [2]}
    assert called["fn"] == "aread_csv"
    assert called["cls"] is DF
    assert called["path"] == "file.csv"
    assert called["columns"] == ["x"]
    assert called["engine_streaming"] is False
    assert called["trusted_mode"] == "strict"
    assert called["scan_kwargs"] == {"another": 2}


def test_dataframe_impl_iter_wrapper_yields_from_lazy_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    class Row(Schema):
        x: int

    DF = ImplDataFrame[Row]

    def _iter_csv(
        cls: type[ImplDataFrame],
        path: object,
        *,
        batch_size: int = 65_536,
        encoding: str = "utf-8",
        trusted_mode: str | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: object | None = None,
    ):
        assert cls is DF
        assert path == "file.csv"
        assert batch_size == 2
        assert encoding == "utf-8"
        yield DF({"x": [1]})
        yield DF({"x": [2]})

    monkeypatch.setattr("pydantable.dataframe._impl_lazy_sources.iter_csv", _iter_csv)
    batches = list(DF.iter_csv("file.csv", batch_size=2))
    assert [b.collect(as_lists=True) for b in batches] == [{"x": [1]}, {"x": [2]}]


@pytest.mark.asyncio
async def test_dataframe_impl_aread_parquet_url_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    class Row(Schema):
        x: int

    DF = ImplDataFrame[Row]

    async def _aread_parquet_url(
        cls: type[ImplDataFrame],
        url: str,
        *,
        experimental: bool = True,
        columns: list[str] | None = None,
        executor: object | None = None,
        engine_streaming: bool | None = None,
        trusted_mode: str | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: object | None = None,
        **kwargs: object,
    ):
        assert cls is DF
        assert url == "https://example.test/file.parquet"
        assert experimental is False
        assert columns == ["x"]
        assert engine_streaming is True
        assert trusted_mode == "shape_only"
        assert kwargs == {"foo": "bar"}
        return DF({"x": [5]})

    monkeypatch.setattr(
        "pydantable.dataframe._impl_lazy_sources.aread_parquet_url",
        _aread_parquet_url,
    )
    out = await DF.aread_parquet_url(
        "https://example.test/file.parquet",
        experimental=False,
        columns=["x"],
        engine_streaming=True,
        trusted_mode="shape_only",
        foo="bar",
    )
    assert out.collect(as_lists=True) == {"x": [5]}


def test_dataframe_impl_iter_parquet_wrapper(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantable.dataframe._impl import DataFrame as ImplDataFrame

    class Row(Schema):
        x: int

    DF = ImplDataFrame[Row]

    def _iter_parquet(
        cls: type[ImplDataFrame],
        path: object,
        *,
        batch_size: int = 65_536,
        columns: list[str] | None = None,
        trusted_mode: str | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: object | None = None,
    ):
        assert cls is DF
        assert path == "file.parquet"
        assert batch_size == 3
        assert columns == ["x"]
        assert trusted_mode == "strict"
        yield DF({"x": [10]})

    monkeypatch.setattr(
        "pydantable.dataframe._impl_lazy_sources.iter_parquet", _iter_parquet
    )
    out = list(
        DF.iter_parquet(
            "file.parquet", batch_size=3, columns=["x"], trusted_mode="strict"
        )
    )
    assert [b.collect(as_lists=True) for b in out] == [{"x": [10]}]
