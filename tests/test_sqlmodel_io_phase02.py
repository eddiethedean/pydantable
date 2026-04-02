"""Phase 2 SQLModel write I/O: write_sqlmodel, batches, async."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantable.errors import MissingOptionalDependency
from pydantable.io import (
    StreamingColumns,
    afetch_sqlmodel,
    awrite_sqlmodel,
    awrite_sqlmodel_batches,
    fetch_sqlmodel,
    iter_sqlmodel,
    write_sqlmodel,
    write_sqlmodel_batches,
)
from sqlalchemy import Column, String, create_engine
from sqlalchemy import inspect as sa_inspect

pytest.importorskip("sqlmodel")

from sqlmodel import Field, SQLModel

if TYPE_CHECKING:
    from pathlib import Path


class _Widget(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    label: str


class _CodeRow(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(sa_column=Column("code", String(5)))


def test_write_sqlmodel_empty_data_no_op(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'empty.sqlite'}")
    _Widget.metadata.create_all(eng)
    write_sqlmodel({}, _Widget, eng, if_exists="append")
    got = fetch_sqlmodel(_Widget, eng)
    assert got == {}


def test_write_sqlmodel_rectangular_rows_required(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'rect.sqlite'}")
    _Widget.metadata.create_all(eng)
    with pytest.raises(ValueError, match="same length"):
        write_sqlmodel(
            {"id": [1, 2], "label": ["a"]},
            _Widget,
            eng,
            if_exists="append",
        )


def test_write_sqlmodel_chunk_size_zero_raises(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'ch0.sqlite'}")
    _Widget.metadata.create_all(eng)
    with pytest.raises(ValueError, match="chunk_size"):
        write_sqlmodel(
            {"id": [1], "label": ["x"]},
            _Widget,
            eng,
            if_exists="append",
            chunk_size=0,
        )


def test_write_sqlmodel_respects_write_chunk_size_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYDANTABLE_SQL_WRITE_CHUNK_SIZE", "7")
    eng = create_engine(f"sqlite:///{tmp_path / 'envch.sqlite'}")
    n = 25
    write_sqlmodel(
        {"id": list(range(1, n + 1)), "label": [f"r{i}" for i in range(1, n + 1)]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert len(got["id"]) == n
    assert got["label"][0] == "r1"
    assert got["label"][-1] == f"r{n}"


def test_write_sqlmodel_url_string_bind(tmp_path: Path) -> None:
    db = tmp_path / "url.sqlite"
    url = f"sqlite:///{db}"
    write_sqlmodel(
        {"id": [1], "label": ["url"]},
        _Widget,
        url,
        if_exists="replace",
        replace_ok=True,
    )
    eng = create_engine(url)
    assert fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id]) == {
        "id": [1],
        "label": ["url"],
    }


def test_write_sqlmodel_append_after_replace(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'ap.sqlite'}")
    write_sqlmodel(
        {"id": [1], "label": ["first"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    write_sqlmodel(
        {"id": [2], "label": ["second"]},
        _Widget,
        eng,
        if_exists="append",
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert got == {"id": [1, 2], "label": ["first", "second"]}


def test_write_sqlmodel_replace_on_empty_database(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'fresh.sqlite'}")
    write_sqlmodel(
        {"id": [None], "label": ["only"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng)
    assert got["label"] == ["only"]


def test_write_sqlmodel_replace_drops_previous_table_definition(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'redef.sqlite'}")
    write_sqlmodel(
        {"id": [1], "label": ["old"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    write_sqlmodel(
        {"id": [2], "label": ["new"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert got == {"id": [2], "label": ["new"]}


def test_write_sqlmodel_batches_accepts_streaming_columns_batch(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'sc.sqlite'}")
    sc = StreamingColumns(
        [
            {"id": [1], "label": ["a"]},
            {"id": [2], "label": ["b"]},
        ]
    )
    write_sqlmodel_batches(
        [sc],
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert got == {"id": [1, 2], "label": ["a", "b"]}


def test_write_sqlmodel_non_table_model_raises_type_error() -> None:
    class _NotTable(SQLModel):
        x: int

    eng = create_engine("sqlite:///:memory:")
    with pytest.raises(TypeError, match="table=True"):
        write_sqlmodel(
            {"x": [1]},
            _NotTable,
            eng,
            if_exists="replace",
            replace_ok=True,
        )


def test_require_sqlmodel_for_write(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    import pydantable.io.sqlmodel_write as smw

    real_import = builtins.__import__

    def _deny_sqlmodel(
        name: str,
        globals=None,
        locals=None,
        fromlist=(),
        level: int = 0,
    ):
        if name == "sqlmodel" or name.startswith("sqlmodel."):
            raise ImportError("denied")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _deny_sqlmodel)
    with pytest.raises(MissingOptionalDependency, match=r"pydantable\[sql\]"):
        smw._require_sqlmodel()


def test_write_sqlmodel_replace_requires_replace_ok(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'r.sqlite'}")
    SQLModel.metadata.create_all(eng)
    with pytest.raises(ValueError, match="replace_ok"):
        write_sqlmodel(
            {"id": [1], "label": ["a"]},
            _Widget,
            eng,
            if_exists="replace",
            replace_ok=False,
        )


def test_write_sqlmodel_replace_uses_model_ddl_string5(tmp_path: Path) -> None:
    db = tmp_path / "ddl.sqlite"
    eng = create_engine(f"sqlite:///{db}")
    write_sqlmodel(
        {"id": [None], "code": ["abcde"]},
        _CodeRow,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    insp = sa_inspect(eng)
    col = next(
        c for c in insp.get_columns(_CodeRow.__table__.name) if c["name"] == "code"
    )
    assert "5" in str(col["type"]).lower() or "varchar" in str(col["type"]).lower()


def test_write_sqlmodel_then_fetch_sqlmodel_roundtrip(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'rt.sqlite'}")
    write_sqlmodel(
        {"id": [1, 2], "label": ["x", "y"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert got == {"id": [1, 2], "label": ["x", "y"]}


def test_write_sqlmodel_append_missing_table_raises(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'no.sqlite'}")
    with pytest.raises(ValueError, match="does not exist"):
        write_sqlmodel(
            {"id": [1], "label": ["z"]},
            _Widget,
            eng,
            if_exists="append",
        )


def test_write_sqlmodel_batches_first_replace_then_append(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'bat.sqlite'}")
    batches = [
        {"id": [1], "label": ["first"]},
        {"id": [2], "label": ["second"]},
    ]
    write_sqlmodel_batches(
        batches,
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = fetch_sqlmodel(_Widget, eng, order_by=[_Widget.id])
    assert got == {"id": [1, 2], "label": ["first", "second"]}


@pytest.mark.asyncio
async def test_awrite_sqlmodel_sqlite(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'aw.sqlite'}")
    await awrite_sqlmodel(
        {"id": [None], "label": ["async"]},
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = await afetch_sqlmodel(_Widget, eng)
    assert got["label"] == ["async"]
    assert len(got["id"]) == 1


@pytest.mark.asyncio
async def test_awrite_sqlmodel_batches(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'awb.sqlite'}")

    async def _gen():
        yield {"id": [1], "label": ["a"]}
        yield {"id": [2], "label": ["b"]}

    await awrite_sqlmodel_batches(
        _gen(),
        _Widget,
        eng,
        if_exists="replace",
        replace_ok=True,
    )
    got = list(iter_sqlmodel(_Widget, eng, order_by=[_Widget.id]))
    keys = [x for b in got for x in b["label"]]
    assert keys == ["a", "b"]


def test_write_sqlmodel_validate_rows_bad_row_index(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'val.sqlite'}")
    _Widget.metadata.create_all(eng)
    data = {"id": [1, 2], "label": ["ok", 123]}  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="row 1"):
        write_sqlmodel(
            data,
            _Widget,
            eng,
            if_exists="append",
            validate_rows=True,
        )


def test_write_sqlmodel_column_key_mismatch_extra(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'km.sqlite'}")
    _Widget.metadata.create_all(eng)
    with pytest.raises(ValueError, match="extra columns"):
        write_sqlmodel(
            {"id": [1], "label": ["a"], "nope": [1]},
            _Widget,
            eng,
            if_exists="append",
        )


def test_write_sqlmodel_column_key_mismatch_missing(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'km2.sqlite'}")
    _Widget.metadata.create_all(eng)
    with pytest.raises(ValueError, match="missing columns"):
        write_sqlmodel(
            {"id": [1]},
            _Widget,
            eng,
            if_exists="append",
        )


def test_write_sqlmodel_schema_mismatch_raises(tmp_path: Path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'sch.sqlite'}")
    _Widget.metadata.create_all(eng)
    with pytest.raises(ValueError, match="schema="):
        write_sqlmodel(
            {"id": [1], "label": ["a"]},
            _Widget,
            eng,
            schema="other",
            if_exists="append",
        )


def test_write_sqlmodel_if_exists_invalid() -> None:
    eng = create_engine("sqlite:///:memory:")
    with pytest.raises(ValueError, match="if_exists"):
        write_sqlmodel(
            {"id": [1], "label": ["a"]},
            _Widget,
            eng,
            if_exists="truncate",
            replace_ok=True,
        )
