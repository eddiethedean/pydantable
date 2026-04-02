"""DataFrameModel SQLModel I/O conveniences (Phase 3)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantable import DataFrameModel

pytest.importorskip("sqlmodel")

from sqlalchemy import bindparam
from sqlmodel import Field, Session, SQLModel, create_engine

if TYPE_CHECKING:
    from pathlib import Path


class _DfmWidget(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    label: str


class WidgetDF(DataFrameModel):
    id: int | None
    label: str


class WidgetIdDF(DataFrameModel):
    """Like :class:`WidgetDF` but ``label`` optional for column projection reads."""

    id: int | None
    label: str | None


def _empty_widget_engine(tmp_path: Path):
    db = tmp_path / "empty_dfm_sqlmodel.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    return eng


def _engine_with_widgets(tmp_path: Path):
    db = tmp_path / "dfm_sqlmodel.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    with Session(eng) as session:
        session.add_all(_DfmWidget(label=f"w{i}") for i in range(10))
        session.commit()
    return eng


def test_io_sqlmodel_classmethods_reject_bridge_base(tmp_path: Path) -> None:
    eng = _empty_widget_engine(tmp_path)
    with pytest.raises(TypeError, match="concrete"):
        DataFrameModel.fetch_sqlmodel(_DfmWidget, eng)  # type: ignore[attr-defined]
    with pytest.raises(TypeError, match="concrete"):
        DataFrameModel.write_sqlmodel_data({}, _DfmWidget, eng)  # type: ignore[attr-defined]


def test_fetch_sqlmodel_empty_table(tmp_path: Path) -> None:
    eng = _empty_widget_engine(tmp_path)
    df = WidgetDF.fetch_sqlmodel(_DfmWidget, eng)
    assert df.collect(as_lists=True) == {"id": [], "label": []}


def test_iter_sqlmodel_empty_table_yields_nothing(tmp_path: Path) -> None:
    eng = _empty_widget_engine(tmp_path)
    assert list(WidgetDF.iter_sqlmodel(_DfmWidget, eng)) == []


def test_fetch_sqlmodel_where_limit_parameters(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    df = WidgetDF.fetch_sqlmodel(
        _DfmWidget,
        eng,
        where=_DfmWidget.id >= bindparam("min_id"),
        parameters={"min_id": 8},
        order_by=[_DfmWidget.id],
        limit=2,
    )
    assert df.collect(as_lists=True) == {"id": [8, 9], "label": ["w7", "w8"]}


def test_fetch_sqlmodel_columns_projection_optional_missing(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    df = WidgetIdDF.fetch_sqlmodel(
        _DfmWidget,
        eng,
        columns=[_DfmWidget.id],
        order_by=[_DfmWidget.id],
        limit=3,
    )
    assert df.collect(as_lists=True) == {"id": [1, 2, 3], "label": [None, None, None]}


def test_iter_sqlmodel_url_string_bind(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    url = str(eng.url)
    flat = []
    for batch in WidgetDF.iter_sqlmodel(
        _DfmWidget,
        url,
        order_by=[_DfmWidget.id],
        limit=3,
        batch_size=2,
    ):
        flat.extend(batch.collect(as_lists=True)["id"])
    assert flat == [1, 2, 3]


def test_iter_sqlmodel_connection_bind(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    with eng.connect() as conn:
        batches = list(
            WidgetDF.iter_sqlmodel(
                _DfmWidget,
                conn,
                where=_DfmWidget.id.in_(bindparam("ids", expanding=True)),
                parameters={"ids": [2, 5, 9]},
                order_by=[_DfmWidget.id],
            )
        )
    flat = [x for b in batches for x in b.collect(as_lists=True)["id"]]
    assert flat == [2, 5, 9]


def test_iter_sqlmodel_batch_size_one_one_batch_per_row(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    batches = list(
        WidgetDF.iter_sqlmodel(
            _DfmWidget,
            eng,
            order_by=[_DfmWidget.id],
            batch_size=1,
        )
    )
    assert len(batches) == 10
    assert [b.collect(as_lists=True)["id"][0] for b in batches] == list(range(1, 11))


def test_iter_sqlmodel_batch_size_zero_raises(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    with pytest.raises(ValueError, match="batch_size"):
        next(iter(WidgetDF.iter_sqlmodel(_DfmWidget, eng, batch_size=0)))


def test_iter_sqlmodel_respects_fetch_batch_size_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PYDANTABLE_SQL_FETCH_BATCH_SIZE", "5")
    eng = _engine_with_widgets(tmp_path)
    batches = list(WidgetDF.iter_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id]))
    assert len(batches) == 2
    assert sum(len(b.collect(as_lists=True)["id"]) for b in batches) == 10


def test_fetch_sqlmodel_roundtrip(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    df = WidgetDF.fetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert df.collect(as_lists=True) == {
        "id": list(range(1, 11)),
        "label": [f"w{i}" for i in range(10)],
    }


def test_iter_sqlmodel_batches(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    batches = list(
        WidgetDF.iter_sqlmodel(
            _DfmWidget,
            eng,
            order_by=[_DfmWidget.id],
            batch_size=3,
        )
    )
    assert sum(len(b.collect(as_lists=True)["id"]) for b in batches) == 10
    assert len(batches) >= 2


def test_write_sqlmodel_instance_append(tmp_path: Path) -> None:
    db = tmp_path / "write.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    WidgetDF.write_sqlmodel_data(
        {"id": [1], "label": ["a"]},
        _DfmWidget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    df = WidgetDF({"id": [2], "label": ["b"]})
    df.write_sqlmodel(_DfmWidget, eng, if_exists="append")
    got = WidgetDF.fetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert got.collect(as_lists=True) == {"id": [1, 2], "label": ["a", "b"]}


async def test_afetch_sqlmodel_collect(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    df = await WidgetDF.afetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert df.collect(as_lists=True) == {
        "id": list(range(1, 11)),
        "label": [f"w{i}" for i in range(10)],
    }


async def test_afetch_sqlmodel_empty_table(tmp_path: Path) -> None:
    eng = _empty_widget_engine(tmp_path)
    df = await WidgetDF.afetch_sqlmodel(_DfmWidget, eng)
    assert df.collect(as_lists=True) == {"id": [], "label": []}


async def test_afetch_sqlmodel_to_dict_terminal(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    d = await WidgetDF.afetch_sqlmodel(
        _DfmWidget,
        eng,
        order_by=[_DfmWidget.id],
        limit=2,
    ).to_dict()
    assert d == {"id": [1, 2], "label": ["w0", "w1"]}


async def test_aiter_sqlmodel_empty_table(tmp_path: Path) -> None:
    eng = _empty_widget_engine(tmp_path)
    batches = []
    async for b in WidgetDF.aiter_sqlmodel(_DfmWidget, eng):
        batches.append(b)
    assert batches == []


async def test_aiter_sqlmodel_batch_size_zero_raises(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    with pytest.raises(ValueError, match="batch_size"):
        async for _ in WidgetDF.aiter_sqlmodel(_DfmWidget, eng, batch_size=0):
            pass


async def test_async_namespace_write_sqlmodel(tmp_path: Path) -> None:
    db = tmp_path / "async_ns_sqlmodel.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    WidgetDF.write_sqlmodel_data(
        {"id": [1], "label": ["a"]},
        _DfmWidget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    await WidgetDF.Async.write_sqlmodel(
        {"id": [2], "label": ["b"]},
        _DfmWidget,
        eng,
        if_exists="append",
    )
    df = WidgetDF.fetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert df.collect(as_lists=True) == {"id": [1, 2], "label": ["a", "b"]}


def test_write_sqlmodel_instance_empty_no_op(tmp_path: Path) -> None:
    db = tmp_path / "empty_write.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    WidgetDF.write_sqlmodel_data(
        {"id": [1], "label": ["a"]},
        _DfmWidget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    empty = WidgetDF({"id": [], "label": []})
    empty.write_sqlmodel(_DfmWidget, eng, if_exists="append")
    got = WidgetDF.fetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert got.collect(as_lists=True) == {"id": [1], "label": ["a"]}


async def test_awrite_sqlmodel_instance(tmp_path: Path) -> None:
    db = tmp_path / "awrite.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    SQLModel.metadata.create_all(eng)
    WidgetDF.write_sqlmodel_data(
        {"id": [1], "label": ["a"]},
        _DfmWidget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    df = WidgetDF({"id": [2], "label": ["b"]})
    await df.awrite_sqlmodel(_DfmWidget, eng, if_exists="append")
    got = await WidgetDF.afetch_sqlmodel(_DfmWidget, eng, order_by=[_DfmWidget.id])
    assert got.collect(as_lists=True) == {"id": [1, 2], "label": ["a", "b"]}


async def test_aiter_sqlmodel_batches(tmp_path: Path) -> None:
    eng = _engine_with_widgets(tmp_path)
    n = 0
    async for batch in WidgetDF.aiter_sqlmodel(
        _DfmWidget,
        eng,
        order_by=[_DfmWidget.id],
        batch_size=4,
    ):
        n += len(batch.collect(as_lists=True)["id"])
    assert n == 10
