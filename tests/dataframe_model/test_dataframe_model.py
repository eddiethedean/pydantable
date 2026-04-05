from __future__ import annotations

import warnings

import pytest
from pydantable import AwaitableDataFrameModel, DataFrame, DataFrameModel, Schema
from pydantable.io import (
    aiter_sql,
    amaterialize_parquet,
    export_ipc,
    export_json,
    export_ndjson,
    export_parquet,
    fetch_sql,
    iter_sql,
    materialize_csv,
    materialize_ipc,
    materialize_json,
    materialize_ndjson,
    materialize_parquet,
)
from pydantable.schema import DtypeDriftWarning, is_supported_scalar_column_annotation
from pydantic import ValidationError


class UserDF(DataFrameModel):
    id: int
    age: int | None


class _AddrNested(Schema):
    street: str
    zip_code: int | None


class _PersonWithAddrDF(DataFrameModel):
    id: int
    addr: _AddrNested


class _OptionalFieldDF(DataFrameModel):
    id: int
    note: str | None


class _OptionalFieldWithDefaultDF(DataFrameModel):
    id: int
    note: str | None = None


class _OptionalFieldWithStringDefaultDF(DataFrameModel):
    id: int
    note: str | None = "n/a"


def test_dataframe_model_column_input_happy_path():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    assert df.schema_fields() == {"id": int, "age": int | None}
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_happy_path():
    df = UserDF([{"id": 1, "age": 20}, {"id": 2, "age": None}])
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_trusted_shape_only_still_row_validates() -> None:
    """trusted_mode does not skip RowModel validation for list-of-rows input."""
    with pytest.raises(ValidationError):
        UserDF(
            [{"id": 1, "age": "not-an-int"}],
            trusted_mode="shape_only",
        )


def test_dataframe_model_row_list_shape_only_collects_like_default_rows() -> None:
    """Row validation runs first; shape_only then applies to the inner column pass."""
    row_df = UserDF(
        [{"id": 1, "age": 20}, {"id": 2, "age": None}],
        trusted_mode="shape_only",
    )
    assert row_df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_missing_optional_field_is_filled_columnar() -> None:
    df = _OptionalFieldDF({"id": [1, 2]})
    assert df.collect(as_lists=True) == {"id": [1, 2], "note": [None, None]}


def test_dataframe_model_missing_optional_field_is_filled_rows() -> None:
    df = _OptionalFieldDF([{"id": 1}, {"id": 2, "note": "x"}])
    assert df.collect(as_lists=True) == {"id": [1, 2], "note": [None, "x"]}


def test_dataframe_model_row_model_optional_defaults_to_none() -> None:
    assert _OptionalFieldDF.RowModel.model_fields["note"].default is None
    assert _OptionalFieldDF.RowModel.model_fields["note"].is_required() is False


def test_dataframe_model_missing_optional_field_can_error_rows() -> None:
    with pytest.raises(ValidationError):
        _OptionalFieldDF([{"id": 1}], fill_missing_optional=False)


def test_dataframe_model_missing_optional_field_can_error_columnar() -> None:
    with pytest.raises(ValueError, match="Missing optional"):
        _OptionalFieldDF({"id": [1]}, fill_missing_optional=False)


def test_dataframe_model_per_field_default_allows_missing_even_when_fill_false_rows() -> (  # noqa: E501
    None
):
    df = _OptionalFieldWithDefaultDF([{"id": 1}], fill_missing_optional=False)
    assert df.collect(as_lists=True) == {"id": [1], "note": [None]}


def test_dataframe_model_per_field_default_allows_missing_even_when_fill_false_columnar() -> (  # noqa: E501
    None
):
    df = _OptionalFieldWithDefaultDF({"id": [1]}, fill_missing_optional=False)
    assert df.collect(as_lists=True) == {"id": [1], "note": [None]}


def test_dataframe_model_non_none_default_applies_when_fill_false_rows() -> None:
    df = _OptionalFieldWithStringDefaultDF([{"id": 1}], fill_missing_optional=False)
    assert df.collect(as_lists=True) == {"id": [1], "note": ["n/a"]}


def test_dataframe_model_non_none_default_applies_when_fill_false_columnar() -> None:
    df = _OptionalFieldWithStringDefaultDF({"id": [1]}, fill_missing_optional=False)
    assert df.collect(as_lists=True) == {"id": [1], "note": ["n/a"]}


def test_dataframe_model_rejects_str_as_row_sequence() -> None:
    with pytest.raises(TypeError, match="columnar"):
        UserDF("not-a-row-sequence")  # type: ignore[arg-type]


def test_dataframe_model_rejects_bytes_as_row_sequence() -> None:
    with pytest.raises(TypeError, match="columnar"):
        UserDF(b"not-rows")  # type: ignore[arg-type]


def test_dataframe_model_constructor_from_io_materialize_parquet(tmp_path) -> None:
    path = tmp_path / "m.pq"
    export_parquet(path, {"id": [1, 2], "age": [10, None]})
    cols = materialize_parquet(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [10, None]}


def test_dataframe_model_read_parquet_lazy_matches_materialize(tmp_path) -> None:
    path = tmp_path / "lazy.pq"
    export_parquet(path, {"id": [1, 2], "age": [10, None]})
    lazy_df = UserDF.read_parquet(path, trusted_mode="shape_only")
    assert lazy_df.collect(as_lists=True) == {"id": [1, 2], "age": [10, None]}


@pytest.mark.asyncio
async def test_aread_parquet_chain_acollect(tmp_path) -> None:
    path = tmp_path / "chain.pq"
    export_parquet(path, {"id": [1, 2], "age": [10, None]})
    rows = (
        await UserDF.aread_parquet(path, trusted_mode="shape_only")
        .select("id", "age")
        .acollect()
    )
    assert [r.id for r in rows] == [1, 2]
    assert [r.age for r in rows] == [10, None]


@pytest.mark.asyncio
async def test_async_namespace_read_parquet_collect_aliases(tmp_path) -> None:
    path = tmp_path / "async_ns.pq"
    export_parquet(path, {"id": [1], "age": [2]})
    rows = await UserDF.Async.read_parquet(path, trusted_mode="shape_only").collect()
    assert [r.id for r in rows] == [1]
    d = await UserDF.Async.read_parquet(path, trusted_mode="shape_only").to_dict()
    assert d == {"id": [1], "age": [2]}


@pytest.mark.asyncio
async def test_async_namespace_export_parquet_and_aexport(
    tmp_path,
) -> None:
    data = {"id": [1], "age": [2]}
    path = tmp_path / "async_export.pq"
    await UserDF.Async.export_parquet(path, data)
    assert materialize_parquet(path) == data
    path2 = tmp_path / "aexport.pq"
    await UserDF.aexport_parquet(path2, data)
    assert materialize_parquet(path2) == data


@pytest.mark.asyncio
async def test_aread_parquet_await_alone_still_works(tmp_path) -> None:
    path = tmp_path / "one.pq"
    export_parquet(path, {"id": [3], "age": [4]})
    df = await UserDF.aread_parquet(path, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [3], "age": [4]}


def test_awaitable_dataframe_model_repr_includes_chain(tmp_path) -> None:
    path = tmp_path / "repr.pq"
    export_parquet(path, {"id": [1], "age": [2]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    r = repr(adf)
    assert "UserDF.aread_parquet" in r
    chained = adf.select("id")
    assert ".select(...)" in repr(chained)


@pytest.mark.asyncio
async def test_aread_parquet_await_lazy_metadata_properties(tmp_path) -> None:
    """``await adf.columns`` / ``dtypes`` work (lazy scan: shape row count is 0)."""
    path = tmp_path / "meta.pq"
    export_parquet(path, {"id": [1, 2], "age": [10, None]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    assert await adf.columns == ["id", "age"]
    assert set((await adf.dtypes).keys()) == {"id", "age"}
    assert await adf.shape == (0, 2)
    assert await adf.empty is True


@pytest.mark.asyncio
async def test_aread_parquet_then_acollect(tmp_path) -> None:
    path = tmp_path / "then.pq"
    export_parquet(path, {"id": [1], "age": [5]})
    rows = (
        await UserDF.aread_parquet(path, trusted_mode="shape_only")
        .then(lambda df: df.select("id"))
        .acollect()
    )
    assert [r.id for r in rows] == [1]


@pytest.mark.asyncio
async def test_aread_parquet_then_async_fn(tmp_path) -> None:
    path = tmp_path / "then_async.pq"
    export_parquet(path, {"id": [2], "age": [6]})

    async def pick_id(df: UserDF) -> UserDF:
        return df.select("id")

    rows = (
        await UserDF.aread_parquet(path, trusted_mode="shape_only")
        .then(pick_id)
        .acollect()
    )
    assert [r.id for r in rows] == [2]


@pytest.mark.asyncio
async def test_awaitable_concat_vertical(tmp_path) -> None:
    p1 = tmp_path / "a.pq"
    p2 = tmp_path / "b.pq"
    export_parquet(p1, {"id": [1], "age": [2]})
    export_parquet(p2, {"id": [3], "age": [4]})
    a = UserDF.aread_parquet(p1, trusted_mode="shape_only")
    b = UserDF.aread_parquet(p2, trusted_mode="shape_only")
    merged = await AwaitableDataFrameModel.concat(a, b)
    assert merged.collect(as_lists=True) == {"id": [1, 3], "age": [2, 4]}


@pytest.mark.asyncio
async def test_awaitable_concat_requires_two_frames() -> None:
    with pytest.raises(ValueError, match="at least two"):
        await AwaitableDataFrameModel.concat(UserDF({"id": [1], "age": [2]}))


@pytest.mark.asyncio
async def test_awaitable_chain_rejects_sync_export_methods(tmp_path) -> None:
    path = tmp_path / "sync_block.pq"
    export_parquet(path, {"id": [1], "age": [2]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    with pytest.raises(TypeError, match="cannot call export_parquet"):
        adf.export_parquet(tmp_path / "out.pq")  # type: ignore[call-arg]


def test_awaitable_dataframe_model_default_repr_without_label() -> None:
    async def _get():
        return UserDF({"id": [1], "age": [2]})

    plain = AwaitableDataFrameModel(_get)
    assert "pending lazy DataFrameModel" in repr(plain)


@pytest.mark.asyncio
async def test_then_nested_awaitable_chain(tmp_path) -> None:
    path = tmp_path / "nest_then.pq"
    export_parquet(path, {"id": [7], "age": [8]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    out = adf.then(lambda _df: UserDF.aread_parquet(path, trusted_mode="shape_only"))
    rows = await out.acollect()
    assert [r.id for r in rows] == [7]


@pytest.mark.asyncio
async def test_awaitable_group_by_agg_max(tmp_path) -> None:
    path = tmp_path / "gb_agg.pq"
    export_parquet(path, {"id": [1, 1], "age": [10, 30]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    g = adf.group_by("id")
    mdf = await g.agg(m=("max", "age"))
    rows = await mdf.acollect()
    assert [r.m for r in rows] == [30]


@pytest.mark.asyncio
async def test_awaitable_join_with_other_awaitable(tmp_path) -> None:
    p1 = tmp_path / "j1.pq"
    p2 = tmp_path / "j2.pq"
    export_parquet(p1, {"id": [1], "age": [10]})
    export_parquet(p2, {"id": [1], "age": [20]})
    left = UserDF.aread_parquet(p1, trusted_mode="shape_only")
    right = UserDF.aread_parquet(p2, trusted_mode="shape_only")
    joined = left.join(right, on="id", suffix="_r")
    rows = await joined.acollect()
    assert len(rows) == 1
    assert rows[0].id == 1


@pytest.mark.asyncio
async def test_awaitable_eager_model_async_terminals() -> None:
    pytest.importorskip("polars")
    pytest.importorskip("pyarrow")

    async def _get() -> UserDF:
        return UserDF({"id": [1], "age": [2]})

    adf = AwaitableDataFrameModel(_get, repr_label="test_eager")
    assert await adf.ato_dict() == {"id": [1], "age": [2]}
    assert await adf.to_dict() == {"id": [1], "age": [2]}
    pl_out = await adf.ato_polars()
    assert pl_out is not None
    assert await adf.to_polars() is not None
    assert await adf.ato_arrow() is not None
    assert await adf.to_arrow() is not None
    rows = await adf.arows()
    assert [r.id for r in rows] == [1]
    dicts = await adf.ato_dicts()
    assert dicts == [{"id": 1, "age": 2}]
    assert await adf.to_dicts() == dicts
    handle = await adf.submit(as_lists=True)
    got = await handle.result()
    assert got == {"id": [1], "age": [2]}
    batches: list[object] = []
    async for b in adf.astream(batch_size=1):
        batches.append(b)
    assert batches
    info = await adf.info()
    assert isinstance(info, str)
    desc = await adf.describe()
    assert isinstance(desc, str)


@pytest.mark.asyncio
async def test_awaitable_chain_collect_alias_matches_acollect(tmp_path) -> None:
    path = tmp_path / "alias.pq"
    export_parquet(path, {"id": [1], "age": [2]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    r1 = await adf.collect(as_lists=True)
    r2 = await adf.acollect(as_lists=True)
    assert r1 == r2 == {"id": [1], "age": [2]}


@pytest.mark.asyncio
async def test_awaitable_chain_rejects_write_parquet(tmp_path) -> None:
    path = tmp_path / "wp.pq"
    export_parquet(path, {"id": [1], "age": [2]})
    adf = UserDF.aread_parquet(path, trusted_mode="shape_only")
    with pytest.raises(TypeError, match="cannot call write_parquet"):
        adf.write_parquet(tmp_path / "out.pq")  # type: ignore[call-arg]


def test_awaitable_getattr_private_raises() -> None:
    async def _get() -> UserDF:
        return UserDF({"id": [1], "age": [2]})

    adf = AwaitableDataFrameModel(_get)
    with pytest.raises(AttributeError, match="no attribute '_x'"):
        _ = adf._x  # type: ignore[attr-defined]


def test_awaitable_grouped_repr_default() -> None:
    from pydantable.awaitable_dataframe_model import AwaitableGroupedDataFrameModel

    async def _g():
        return object()

    g = AwaitableGroupedDataFrameModel(_g)
    assert "pending" in repr(g)


def test_awaitable_dynamic_grouped_repr_and_agg() -> None:
    from pydantable.awaitable_dataframe_model import (
        AwaitableDynamicGroupedDataFrameModel,
    )

    class _G:
        def agg(self, **kwargs: object) -> UserDF:
            return UserDF({"id": [1], "age": [2]})

    async def _get() -> _G:
        return _G()

    dg = AwaitableDynamicGroupedDataFrameModel(_get)
    assert "AwaitableDynamicGroupedDataFrameModel" in repr(dg)
    out = dg.agg(m=("max", "age"))
    assert isinstance(out, AwaitableDataFrameModel)


def test_dataframe_model_constructor_from_io_materialize_ndjson(tmp_path) -> None:
    path = tmp_path / "m.ndjson"
    export_ndjson(path, {"id": [3], "age": [None]})
    cols = materialize_ndjson(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [3], "age": [None]}


def test_dataframe_model_constructor_from_io_materialize_ipc(tmp_path) -> None:
    path = tmp_path / "m.arrow"
    export_ipc(path, {"id": [4], "age": [12]})
    cols = materialize_ipc(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [4], "age": [12]}


def test_dataframe_model_constructor_from_io_materialize_csv(tmp_path) -> None:
    path = tmp_path / "m.csv"
    path.write_text("id,age\n5,7\n6,\n", encoding="utf-8")
    cols = materialize_csv(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [5, 6], "age": [7, None]}


def test_dataframe_model_constructor_from_io_materialize_json(tmp_path) -> None:
    path = tmp_path / "rows.json"
    export_json(path, {"id": [10, 11], "age": [3, None]})
    cols = materialize_json(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [10, 11], "age": [3, None]}


def test_dataframe_model_constructor_from_fetch_sql(tmp_path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "io.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, age INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (7, 8)"))
    cols = fetch_sql("SELECT id, age FROM t", eng)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [7], "age": [8]}


def test_dataframe_model_iter_sql_batches_via_io_and_constructor(tmp_path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "iter_sql.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, age INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (1, 10), (2, 20)"))

    batches = [
        UserDF(cols, trusted_mode="shape_only")
        for cols in iter_sql(
            "SELECT id, age FROM t ORDER BY id",
            eng,
            batch_size=1,
        )
    ]
    assert len(batches) == 2
    assert all(type(b) is UserDF for b in batches)
    flat = {
        "id": [x for b in batches for x in b.collect(as_lists=True)["id"]],
        "age": [x for b in batches for x in b.collect(as_lists=True)["age"]],
    }
    assert flat == {"id": [1, 2], "age": [10, 20]}


@pytest.mark.asyncio
async def test_dataframe_model_aiter_sql_batches_via_io(tmp_path) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    db = tmp_path / "aiter_sql.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER, age INTEGER)"))
        conn.execute(text("INSERT INTO t VALUES (3, 30), (4, 40)"))

    out: list[UserDF] = []
    async for cols in aiter_sql(
        "SELECT id, age FROM t ORDER BY id",
        eng,
        batch_size=2,
    ):
        out.append(UserDF(cols, trusted_mode="shape_only"))
    assert len(out) == 1
    assert out[0].collect(as_lists=True) == {"id": [3, 4], "age": [30, 40]}


@pytest.mark.asyncio
async def test_dataframe_model_amaterialize_parquet_via_io(tmp_path) -> None:
    path = tmp_path / "a.pq"
    export_parquet(path, {"id": [9], "age": [11]})
    cols = await amaterialize_parquet(path)
    df = UserDF(cols, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [9], "age": [11]}


def test_dataframe_model_io_classmethod_rejects_bridge_base(tmp_path) -> None:
    path = tmp_path / "nope.pq"
    path.write_bytes(b"")
    with pytest.raises(TypeError, match="concrete"):
        DataFrameModel.read_parquet(str(path))  # type: ignore[attr-defined]


def test_dataframe_model_row_input_strict_mode_still_raises() -> None:
    with pytest.raises(ValidationError):
        UserDF([{"id": 1, "age": 20}, {"id": "bad", "age": 30}])


def test_dataframe_model_ignore_errors_row_input_keeps_valid_rows() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [{"id": 1, "age": 20}, {"id": "bad", "age": 30}, {"id": 2, "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"id": "bad", "age": 30}
    assert isinstance(failures[0]["errors"], list)


def test_dataframe_model_ignore_errors_columnar_input_best_effort() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        {"id": [1, "bad", 2], "age": [20, 30, None]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"id": "bad", "age": 30}


def test_dataframe_model_ignore_errors_all_invalid_rows_returns_empty() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [{"id": "bad1", "age": 20}, {"id": "bad2", "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [], "age": []}
    assert len(failures) == 2


def test_dataframe_model_ignore_errors_callback_not_called_when_clean() -> None:
    called = False

    def on_fail(_items: list[dict[str, object]]) -> None:
        nonlocal called
        called = True

    df = UserDF(
        [{"id": 1, "age": 20}, {"id": 2, "age": None}],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert called is False


def test_dataframe_model_ignore_errors_callback_collects_multiple_row_failures() -> (
    None
):
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [
            {"id": 1, "age": 20},
            {"id": "bad-1", "age": 30},
            {"id": 2, "age": None},
            {"id": "bad-2", "age": None},
        ],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert [f["row_index"] for f in failures] == [1, 3]
    assert failures[0]["row"] == {"id": "bad-1", "age": 30}
    assert failures[1]["row"] == {"id": "bad-2", "age": None}


def test_dataframe_model_ignore_errors_callback_invoked_once_with_all_failures() -> (
    None
):
    invocations = 0
    seen_indices: list[int] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        nonlocal invocations
        invocations += 1
        seen_indices.extend(int(item["row_index"]) for item in items)

    _ = UserDF(
        {"id": ["bad-0", 1, "bad-2"], "age": [10, 20, 30]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert invocations == 1
    assert seen_indices == [0, 2]


def test_dataframe_model_ignore_errors_non_mapping_row_is_reported_and_skipped() -> (
    None
):
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        [
            {"id": 1, "age": 20},
            123,  # type: ignore[list-item]
            {"id": 2, "age": None},
        ],
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert len(failures) == 1
    assert failures[0]["row_index"] == 1
    assert failures[0]["row"] == {"_raw_row": 123}


def test_dataframe_model_ignore_errors_columnar_multiple_failures_payload() -> None:
    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = UserDF(
        {"id": [1, "bad-1", 2, "bad-2"], "age": [20, 30, None, 40]},
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}
    assert [f["row_index"] for f in failures] == [1, 3]
    assert failures[0]["row"] == {"id": "bad-1", "age": 30}
    assert failures[1]["row"] == {"id": "bad-2", "age": 40}


def test_dataframe_model_columnar_strict_mode_raises_without_ignore_errors() -> None:
    with pytest.raises(ValidationError):
        UserDF({"id": [1, "bad", 2], "age": [20, 30, None]})


def test_dataframe_model_ignore_errors_still_checks_column_lengths() -> None:
    with pytest.raises(ValueError, match="same length"):
        UserDF(
            {"id": [1, "bad", 2], "age": [20, 30]},
            ignore_errors=True,
        )


def test_dataframe_model_row_input_sequence_of_pydantic_models():
    rm = UserDF.row_model()
    rows = [rm(id=1, age=20), rm(id=2, age=None)]
    df = UserDF(rows)
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_input_mixed_dict_and_model():
    rm = UserDF.row_model()
    df = UserDF([{"id": 1, "age": 20}, rm(id=2, age=None)])
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_row_model_generation_and_validation():
    row_model = UserDF.row_model()
    ok = row_model.model_validate({"id": 1, "age": None})
    assert ok.id == 1

    with pytest.raises(ValidationError):
        row_model.model_validate({"id": "x", "age": 1})


def test_dataframe_model_transformations_return_derived_model():
    df = UserDF({"id": [1, 2, 3], "age": [10, 20, None]})

    df2 = df.with_columns(age2=df.age + 1)
    assert "age2" in df2.schema_fields()
    assert df2.schema_fields()["age2"] == int | None

    df3 = df2.select("id", "age2")
    assert df3.schema_fields() == {"id": int, "age2": int | None}

    df4 = df3.filter(df3.age2 > 11)
    assert df4.collect(as_lists=True) == {"id": [2], "age2": [21]}


def test_dataframe_model_pipe_and_clip() -> None:
    class S(DataFrameModel):
        x: int
        y: float

    df = S({"x": [1, 5], "y": [1.5, -2.0]})
    out = df.clip(lower=0, upper=3).to_dict()
    assert out == {"x": [1, 3], "y": [1.5, 0.0]}

    out2 = df.pipe(lambda d: d.clip(upper=0, subset="y")).to_dict()
    assert out2 == {"x": [1, 5], "y": [0.0, -2.0]}


def test_dataframe_model_with_row_count() -> None:
    class S(DataFrameModel):
        x: int

    df = S({"x": [10, 20, 30]})
    out = df.with_row_count().to_dict()
    assert out == {"x": [10, 20, 30], "row_nr": [0, 1, 2]}


def test_dataframe_model_selector_helpers() -> None:
    from pydantable import selectors as s

    class S(DataFrameModel):
        a: int | None
        b: int | None

    df = S({"a": [None, 1], "b": [2, None]})
    out = df.with_columns_fill_null(s.by_name("a"), value=0).to_dict()
    assert out == {"a": [0, 1], "b": [2, None]}

    out2 = df.with_columns_cast(s.by_name("b"), float).to_dict()
    assert out2 == {"a": [None, 1], "b": [2.0, None]}

    out3 = df.rename_upper(s.by_name("a")).to_dict()
    assert out3 == {"A": [None, 1], "b": [2, None]}

    assert df.select_schema(s.by_name("b")).to_dict() == {"b": [2, None]}


def test_dataframe_model_row_input_rejects_bad_item_type():
    with pytest.raises(TypeError, match="mapping objects or Pydantic models"):
        UserDF([1, 2, 3])  # type: ignore[arg-type]


def test_dataframe_model_rejects_unsupported_dict_type_at_class_definition():
    with pytest.raises(TypeError, match="unsupported type") as exc:

        class BadDict(DataFrameModel):
            m: dict[int, str]

    assert "BadDict" in str(exc.value)
    assert "m" in str(exc.value)
    assert "SUPPORTED_TYPES" in str(exc.value)


def test_dataframe_model_rejects_unsupported_union_of_two_scalars_at_class_definition():
    with pytest.raises(TypeError, match="unsupported type"):

        class BadUnion(DataFrameModel):
            x: int | str


def test_is_supported_scalar_column_annotation_smoke():
    assert is_supported_scalar_column_annotation(int)
    assert is_supported_scalar_column_annotation(int | None)
    assert not is_supported_scalar_column_annotation(list[int])
    assert not is_supported_scalar_column_annotation(dict[str, int])


def test_dataframe_model_parity_with_dataframe_core_expression_behavior():
    # DataFrameModel should expose the same expression typing behavior.
    df = UserDF({"id": [1, 2], "age": [20, 30]})
    with pytest.raises(TypeError, match="requires numeric operands"):
        _ = df.age + "x"


def test_dataframe_model_chained_schema_migration_dtypes():
    df = UserDF({"id": [1, 2, 3], "age": [20, None, 30]})
    df2 = df.with_columns(age2=df.age + 1, flag=df.age > 21)
    schema = df2.schema_fields()
    assert schema["age2"] == int | None
    assert schema["flag"] == bool | None


def test_rust_schema_descriptors_flow_into_derived_model_types():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    df2 = df.with_columns(age2=df.age + 1, flag=df.age > 10)
    # Validate descriptor contract from rust and python mapping.
    desc = df2._df._rust_plan.schema_descriptors()
    assert desc["age2"] == {"base": "int", "nullable": True}
    assert desc["flag"] == {"base": "bool", "nullable": True}
    assert df2.schema_fields()["age2"] == int | None
    assert df2.schema_fields()["flag"] == bool | None


def test_dataframe_model_with_columns_collision_replacement_semantics():
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 20]})
    df2 = df.with_columns(age=df.age + 1)
    assert df2.schema_fields()["age"] == int | None
    assert df2.collect(as_lists=True) == {"id": [1, 2, 3], "age": [11, None, 21]}


def test_dataframe_model_filter_preserves_schema_changes_rows_only():
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})
    before = df.schema_fields()
    df2 = df.filter(df.age > 20)
    after = df2.schema_fields()
    assert before == after
    assert df2.collect(as_lists=True) == {"id": [3], "age": [30]}


def test_dataframe_model_row_vs_column_input_transformation_parity():
    row_df = UserDF(
        [{"id": 1, "age": 10}, {"id": 2, "age": None}, {"id": 3, "age": 30}]
    )
    col_df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})

    row_df2 = row_df.with_columns(age2=row_df.age + 1)
    row_out = (
        row_df2.filter(row_df2.age2 > 20).select("id", "age2").collect(as_lists=True)
    )
    col_df2 = col_df.with_columns(age2=col_df.age + 1)
    col_out = (
        col_df2.filter(col_df2.age2 > 20).select("id", "age2").collect(as_lists=True)
    )
    assert row_out == col_out == {"id": [3], "age2": [31]}


def test_rows_materializes_row_models_with_nulls():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    rows = df.rows()

    assert len(rows) == 2
    assert isinstance(rows[0], df.schema_type)
    assert rows[0].id == 1
    assert rows[0].age == 20

    assert isinstance(rows[1], df.schema_type)
    assert rows[1].id == 2
    assert rows[1].age is None


def test_rows_and_to_dicts_materialize_derived_schema():
    df = UserDF({"id": [1, 2], "age": [20, None]})
    df2 = df.with_columns(age2=df.age + 1)

    rows = df2.rows()
    assert [r.id for r in rows] == [1, 2]
    assert [r.age2 for r in rows] == [21, None]

    got_dicts = df2.to_dicts()
    assert got_dicts == [
        {"id": 1, "age": 20, "age2": 21},
        {"id": 2, "age": None, "age2": None},
    ]


def test_rows_returns_empty_list_for_empty_dataframe():
    df = UserDF({"id": [], "age": []})
    assert df.rows() == []


def test_row_model_rejects_extra_fields():
    row_model = UserDF.row_model()
    with pytest.raises(ValidationError):
        row_model.model_validate({"id": 1, "age": None, "extra": "x"})


def test_p1_dataframe_model_methods_and_concat():
    df = UserDF({"id": [3, 1, 2, 2], "age": [30, None, 20, 20]})

    sorted_df = df.sort("id")
    assert sorted_df.collect(as_lists=True)["id"] == [1, 2, 2, 3]

    unique_df = sorted_df.unique(subset=["id", "age"])
    assert unique_df.collect(as_lists=True) == {"id": [1, 2, 3], "age": [None, 20, 30]}

    renamed = unique_df.rename({"age": "years"})
    assert set(renamed.schema_fields().keys()) == {"id", "years"}
    assert renamed.schema_fields()["years"] == int | None
    assert renamed.slice(1, 2).collect(as_lists=True) == {
        "id": [2, 3],
        "years": [20, 30],
    }
    assert renamed.head(1).collect(as_lists=True) == {"id": [1], "years": [None]}
    assert renamed.tail(1).collect(as_lists=True) == {"id": [3], "years": [30]}

    first = renamed.select("id")
    second = renamed.select("id")
    cat = DataFrameModel.concat([first, second], how="vertical")
    assert cat.collect(as_lists=True) == {"id": [1, 2, 3, 1, 2, 3]}


def test_p2_dataframe_model_fill_and_drop_nulls() -> None:
    df = UserDF({"id": [1, 2, 3], "age": [10, None, 30]})
    filled = df.fill_null(0, subset=["age"])
    assert filled.collect(as_lists=True) == {"id": [1, 2, 3], "age": [10, 0, 30]}
    assert filled.schema_fields()["age"] is int

    dropped = df.drop_nulls(subset=["age"])
    assert dropped.collect(as_lists=True) == {"id": [1, 3], "age": [10, 30]}


def test_p4_dataframe_model_groupby_aggregations_schema() -> None:
    df = UserDF({"id": [1, 1, 2], "age": [10, 20, 30]})
    grouped = df.group_by("id").agg(
        age_min=("min", "age"),
        age_max=("max", "age"),
        age_median=("median", "age"),
        age_std=("std", "age"),
        age_var=("var", "age"),
        age_first=("first", "age"),
        age_last=("last", "age"),
        age_n_unique=("n_unique", "age"),
    )
    schema = grouped.schema_fields()
    assert schema["age_min"] == int | None
    assert schema["age_max"] == int | None
    assert schema["age_median"] == float | None
    assert schema["age_std"] == float | None
    assert schema["age_var"] == float | None
    assert schema["age_first"] == int | None
    assert schema["age_last"] == int | None
    assert schema["age_n_unique"] is int


def test_p5_dataframe_model_reshape_methods() -> None:
    class SalesDF(DataFrameModel):
        id: int
        k: str
        v: int | None

    df = SalesDF({"id": [1, 1], "k": ["A", "B"], "v": [10, None]})
    melted = df.melt(
        id_vars=["id"], value_vars=["v"], variable_name="var", value_name="val"
    )
    out = melted.collect(as_lists=True)
    assert out == {"id": [1, 1], "var": ["v", "v"], "val": [10, None]}
    assert melted.schema_fields()["var"] is str
    assert melted.schema_fields()["val"] == int | None

    pivoted = df.pivot(index="id", columns="k", values="v", aggregate_function="first")
    p_out = pivoted.collect(as_lists=True)
    assert p_out["id"] == [1]
    assert p_out["A_first"] == [10]
    assert p_out["B_first"] == [None]


def test_nested_model_column_round_trip() -> None:
    df = _PersonWithAddrDF(
        {
            "id": [1, 2],
            "addr": [
                {"street": "Main", "zip_code": 12345},
                {"street": "Oak", "zip_code": None},
            ],
        }
    )
    assert df.collect(as_lists=True) == {
        "id": [1, 2],
        "addr": [
            {"street": "Main", "zip_code": 12345},
            {"street": "Oak", "zip_code": None},
        ],
    }
    desc = df._df._rust_plan.schema_descriptors()["addr"]
    assert desc["kind"] == "struct"
    assert desc["nullable"] is False


def test_p6_dataframe_model_rolling_and_dynamic() -> None:
    class TSModel(DataFrameModel):
        id: int
        ts: int
        v: int | None

    df = TSModel(
        {
            "id": [1, 1, 1],
            "ts": [0, 3600, 7200],
            "v": [10, None, 30],
        }
    )
    rolled = df.rolling_agg(
        on="ts",
        column="v",
        window_size="2h",
        op="sum",
        out_name="v_roll_sum",
        by=["id"],
    )
    assert rolled.collect(as_lists=True)["v_roll_sum"] == [10, 10, 40]

    grouped = df.group_by_dynamic("ts", every="1h", by=["id"]).agg(
        v_count=("count", "v")
    )
    assert "v_count" in grouped.collect(as_lists=True)


@pytest.mark.asyncio
async def test_aread_chain_group_by_dynamic_agg(tmp_path) -> None:
    class TSDyn(DataFrameModel):
        id: int
        ts: int
        v: int | None

    path = tmp_path / "dyn_ts.pq"
    export_parquet(
        path,
        {"id": [1, 1], "ts": [0, 3600], "v": [10, 20]},
    )
    adf = TSDyn.aread_parquet(path, trusted_mode="shape_only")
    dyn = adf.group_by_dynamic("ts", every="1h", by=["id"])
    mdf = await dyn.agg(v_sum=("sum", "v"))
    out = await mdf.acollect(as_lists=True)
    assert "v_sum" in out


def test_dataframe_model_accepts_polars_dataframe_trusted_shape_only() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, 2], "age": [20, None]})
    df = UserDF(pdf, trusted_mode="shape_only")
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [20, None]}


def test_dataframe_model_polars_dataframe_rejects_column_mismatch() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1], "bad": [2]})
    with pytest.raises(ValueError, match="columns exactly"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_polars_dataframe_rejects_null_in_non_nullable_column() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, None], "age": [10, 20]})
    with pytest.raises(ValueError, match="non-nullable"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_trusted_shape_only_allows_dtype_mismatch() -> None:
    with pytest.warns(DtypeDriftWarning, match="shape_only"):
        df = UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="shape_only")
    assert isinstance(df, UserDF)


def test_shape_only_drift_suppressed_by_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_SUPPRESS_SHAPE_ONLY_DRIFT_WARNINGS", "1")
    with warnings.catch_warnings():
        warnings.simplefilter("error", DtypeDriftWarning)
        UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="shape_only")


def test_dataframe_model_trusted_strict_rejects_dtype_mismatch() -> None:
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF({"id": ["1", "2"], "age": [20, None]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_rejects_null_in_non_nullable_column() -> None:
    with pytest.raises(ValueError, match="non-nullable"):
        UserDF({"id": [1, None], "age": [20, 30]}, trusted_mode="strict")


def test_validate_columns_strict_trusted_mode_conflicts_with_validate_elements() -> (
    None
):
    from pydantable.schema import validate_columns_strict

    with pytest.raises(ValueError, match="conflicts with trusted_mode"):
        validate_columns_strict(
            {"id": [1], "age": [10]},
            UserDF._SchemaModel,
            validate_elements=True,
            trusted_mode="shape_only",
        )


def test_validate_columns_strict_validate_elements_false_and_trusted_off_conflict() -> (
    None
):
    from pydantable.schema import validate_columns_strict

    with pytest.raises(ValueError, match="conflicts with trusted_mode"):
        validate_columns_strict(
            {"id": [1], "age": [10]},
            UserDF._SchemaModel,
            validate_elements=False,
            trusted_mode="off",
        )


def test_dataframe_model_trusted_shape_only_collect_roundtrip() -> None:
    data = {"id": [1, 2], "age": [20, None]}
    out = UserDF(data, trusted_mode="shape_only").collect(as_lists=True)
    assert out == data


def test_dataframe_model_rejects_validate_data_keyword() -> None:
    with pytest.raises(TypeError):
        UserDF({"id": [1], "age": [10]}, validate_data=True)
    with pytest.raises(TypeError):
        UserDF({"id": [1], "age": [10]}, validate_data=False)


def test_default_constructors_no_deprecation_warning() -> None:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        UserDF({"id": [1], "age": [10]})
        DataFrame[UserDF._SchemaModel]({"id": [1], "age": [10]})


def test_dataframe_model_polars_shape_only_warns_on_strict_dtype_mismatch() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": ["x", "y"], "age": [1, 2]})
    with pytest.warns(DtypeDriftWarning, match="shape_only"):
        UserDF(pdf, trusted_mode="shape_only")


def test_dataframe_model_polars_trusted_strict_rejects_wrong_scalar_dtype() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": ["x", "y"], "age": [1, 2]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF(pdf, trusted_mode="strict")


def test_dataframe_model_polars_trusted_strict_accepts_int_columns() -> None:
    pl = pytest.importorskip("polars")
    pdf = pl.DataFrame({"id": [1, 2], "age": [10, 20]})
    df = UserDF(pdf, trusted_mode="strict")
    assert df.collect(as_lists=True) == {"id": [1, 2], "age": [10, 20]}


def test_dataframe_model_numpy_trusted_strict_int_array() -> None:
    np = pytest.importorskip("numpy")
    df = UserDF(
        {"id": np.array([1, 2, 3], dtype=np.int64), "age": [10, 20, 30]},
        trusted_mode="strict",
    )
    assert df.collect(as_lists=True) == {"id": [1, 2, 3], "age": [10, 20, 30]}


def test_dataframe_model_numpy_trusted_strict_rejects_float_for_int_column() -> None:
    np = pytest.importorskip("numpy")
    with pytest.raises(ValueError, match="strict trusted mode"):
        UserDF(
            {
                "id": np.array([1.0, 2.0], dtype=np.float64),
                "age": [10, 20],
            },
            trusted_mode="strict",
        )


def test_dataframe_model_trusted_strict_nested_list_shape() -> None:
    class S(Schema):
        xs: list[int]

    DataFrame[S]({"xs": [[1, 2], [3]]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[S]({"xs": [[1.0, 2.0]]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_struct_polars() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    pdf = pl.DataFrame({"s": [{"a": 1}, {"a": 2}]})
    DataFrame[Outer](pdf, trusted_mode="strict")
    bad = pl.DataFrame({"s": [{"a": "x"}, {"a": "y"}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_map_entries_polars() -> None:
    pl = pytest.importorskip("polars")

    class M(Schema):
        m: dict[str, int]

    pdf = pl.DataFrame(
        {
            "m": [
                [{"key": "a", "value": 1}, {"key": "b", "value": 2}],
                [{"key": "c", "value": 3}],
            ]
        }
    )
    DataFrame[M](pdf, trusted_mode="strict")
    bad = pl.DataFrame(
        {
            "m": [
                [{"key": "a", "value": 1.5}],
            ]
        }
    )
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[M](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_dict_python_path() -> None:
    class M(Schema):
        m: dict[str, int]

    DataFrame[M]({"m": [{"a": 1}, {"b": 2}]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[M]({"m": [{"a": 1.0}]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_nested_list_of_lists() -> None:
    class S(Schema):
        xss: list[list[int]]

    DataFrame[S]({"xss": [[[1, 2], [3]], [[4]]]}, trusted_mode="strict")
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[S]({"xss": [[[1.0]]]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_optional_nested_list_cell() -> None:
    class S(Schema):
        xs: list[int] | None

    DataFrame[S]({"xs": [[1, 2], None]}, trusted_mode="strict")


def test_dataframe_model_trusted_strict_polars_struct_extra_field_rejected() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    bad = pl.DataFrame({"s": [{"a": 1, "b": 2}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_trusted_strict_polars_struct_missing_field_rejected() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int
        b: int

    class Outer(Schema):
        s: Inner

    bad = pl.DataFrame({"s": [{"a": 1}]})
    with pytest.raises(ValueError, match="strict trusted mode"):
        DataFrame[Outer](bad, trusted_mode="strict")


def test_dataframe_model_shape_only_allows_polars_dtype_mismatch_nested() -> None:
    pl = pytest.importorskip("polars")

    class Inner(Schema):
        a: int

    class Outer(Schema):
        s: Inner

    pdf = pl.DataFrame({"s": [{"a": "x"}, {"a": "y"}]})
    with pytest.warns(DtypeDriftWarning):
        DataFrame[Outer](pdf, trusted_mode="shape_only")
