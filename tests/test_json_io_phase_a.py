"""Phase 1.10 JSON I/O: nested materialize/export, lazy NDJSON, struct/map columns."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

if TYPE_CHECKING:
    from pathlib import Path
from pydantable import DataFrame, Schema
from pydantable.io import export_json, materialize_json, read_json, read_ndjson


class _Meta(Schema):
    k: int


class _RowNested(Schema):
    """Lazy NDJSON: nested struct + list; map via materialize_json path."""

    id: int
    meta: _Meta
    nums: list[int]


class _RowWithMap(Schema):
    id: int
    meta: _Meta
    nums: list[int]
    tags: dict[str, int]


def test_materialize_json_array_nested_struct_list_map(tmp_path: Path) -> None:
    path = tmp_path / "arr.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "meta": {"k": 42},
                    "nums": [1, 2],
                    "tags": {"a": 10},
                },
                {
                    "id": 2,
                    "meta": {"k": 0},
                    "nums": [],
                    "tags": {},
                },
            ]
        ),
        encoding="utf-8",
    )
    got = materialize_json(path)
    assert got["id"] == [1, 2]
    assert got["meta"] == [{"k": 42}, {"k": 0}]
    assert got["nums"] == [[1, 2], []]
    assert got["tags"] == [{"a": 10}, {}]


def test_materialize_json_ndjson_nested_matches_array_shape(tmp_path: Path) -> None:
    lines = tmp_path / "lines.ndjson"
    lines.write_text(
        json.dumps(
            {"id": 1, "meta": {"k": 7}, "nums": [3], "tags": {"b": 2}},
        )
        + "\n",
        encoding="utf-8",
    )
    got_lines = materialize_json(lines)
    arr = tmp_path / "a.json"
    arr.write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "meta": {"k": 7},
                    "nums": [3],
                    "tags": {"b": 2},
                }
            ]
        ),
        encoding="utf-8",
    )
    got_arr = materialize_json(arr)
    assert got_lines == got_arr


def test_export_json_roundtrip_nested_dict_and_list(tmp_path: Path) -> None:
    # json.dump: default=str for odd scalars; nested dict/list stay native.
    path = tmp_path / "out.json"
    data = {
        "id": [1],
        "meta": [{"k": 99}],
        "nums": [[10, 20]],
        "tags": [{"x": 1}],
    }
    export_json(path, data)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded == [{"id": 1, "meta": {"k": 99}, "nums": [10, 20], "tags": {"x": 1}}]


def test_export_json_default_str_for_datetime_decimal_uuid(tmp_path: Path) -> None:
    """Locks contract: export_json uses json.dump(..., default=str)."""
    dt = datetime(2024, 1, 15, 12, 30, 45)
    dec = Decimal("3.14")
    uid = UUID("550e8400-e29b-41d4-a716-446655440000")
    path = tmp_path / "scalars.json"
    export_json(path, {"d": [dt], "m": [dec], "u": [uid]})
    text = path.read_text(encoding="utf-8")
    loaded = json.loads(text)
    assert loaded[0]["d"] == str(dt)
    assert loaded[0]["m"] == str(dec)
    assert loaded[0]["u"] == str(uid)


def test_read_json_alias_matches_ndjson_nested(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    p = tmp_path / "nested.ndjson"
    p.write_text(
        json.dumps({"id": 1, "meta": {"k": 1}, "nums": [1]}) + "\n",
        encoding="utf-8",
    )
    r1 = read_ndjson(p)
    r2 = read_json(p)
    assert type(r1) is type(r2)
    assert getattr(r1, "path", None) == getattr(r2, "path", None)


def test_lazy_read_ndjson_nested_collect_shape_only(tmp_path: Path) -> None:
    """Lazy NDJSON → struct + list; map JSON uses materialize_json path."""
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "n.ndjson"
    path.write_text(
        json.dumps(
            {
                "id": 1,
                "meta": {"k": 5},
                "nums": [1, 2, 3],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    df = DataFrame[_RowNested].read_ndjson(str(path), trusted_mode="shape_only")
    got = df.to_dict()
    assert got["id"] == [1]
    assert got["meta"] == [{"k": 5}]
    assert list(got["nums"][0]) == [1, 2, 3]


def test_materialize_json_then_constructor_includes_map_column(tmp_path: Path) -> None:
    """Eager materialize keeps JSON objects as dict cells (dict[str, T] maps)."""
    path = tmp_path / "m.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": 1,
                    "meta": {"k": 2},
                    "nums": [9],
                    "tags": {"a": 1, "b": 2},
                }
            ]
        ),
        encoding="utf-8",
    )
    cols = materialize_json(path)
    df = DataFrame[_RowWithMap](cols, trusted_mode="shape_only")
    got = df.collect(as_lists=True)
    assert got["id"] == [1]
    assert got["meta"] == [{"k": 2}]
    assert got["nums"] == [[9]]
    assert got["tags"] == [{"a": 1, "b": 2}]
