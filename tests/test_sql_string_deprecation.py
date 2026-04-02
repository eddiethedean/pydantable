"""Runtime deprecation of legacy string-SQL I/O (Phase 4, v1.13.0)."""

from __future__ import annotations

import warnings

import pytest
from pydantable.io import (
    afetch_sql,
    aiter_sql,
    awrite_sql,
    fetch_sql,
    fetch_sql_raw,
    iter_sql,
    write_sql,
    write_sql_batches,
)
from sqlalchemy import create_engine

pytestmark = pytest.mark.filterwarnings("default::DeprecationWarning")


def test_fetch_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'a.sqlite'}")
    with pytest.warns(DeprecationWarning, match="fetch_sql"):
        fetch_sql("SELECT 1 AS x", eng)


def test_iter_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'b.sqlite'}")
    with pytest.warns(DeprecationWarning, match="iter_sql"):
        next(iter(iter_sql("SELECT 1 AS x", eng)))


def test_write_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'c.sqlite'}")
    with (
        pytest.raises(ValueError, match="does not exist"),
        pytest.warns(DeprecationWarning, match="write_sql"),
    ):
        write_sql({"n": [1]}, "missing", eng, if_exists="append")


def test_write_sql_batches_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'd.sqlite'}")
    with pytest.warns(DeprecationWarning, match="write_sql_batches"):
        write_sql_batches(iter(()), "t", eng, if_exists="append")


@pytest.mark.asyncio
async def test_afetch_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'e.sqlite'}")
    with pytest.warns(DeprecationWarning, match="afetch_sql"):
        await afetch_sql("SELECT 1 AS x", eng)


@pytest.mark.asyncio
async def test_aiter_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'f.sqlite'}")
    with pytest.warns(DeprecationWarning, match="aiter_sql"):
        async for _ in aiter_sql("SELECT 1 AS x", eng):
            break


@pytest.mark.asyncio
async def test_awrite_sql_deprecated_warns(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'g.sqlite'}")
    with (
        pytest.warns(DeprecationWarning, match="awrite_sql"),
        pytest.raises(ValueError, match="does not exist"),
    ):
        await awrite_sql({"n": [1]}, "ghost", eng, if_exists="append")


def test_fetch_sql_raw_no_deprecation_warning(tmp_path) -> None:
    eng = create_engine(f"sqlite:///{tmp_path / 'h.sqlite'}")
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        out = fetch_sql_raw("SELECT 1 AS x", eng)
    assert out == {"x": [1]}
    assert not recorded
