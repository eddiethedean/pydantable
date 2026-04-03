from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from pydantable import DataFrame
from pydantic import BaseModel

if TYPE_CHECKING:
    from pathlib import Path


class _TwoCols(BaseModel):
    id: int
    age: int


class _OptionalCol(BaseModel):
    id: int
    note: str | None


class _OptionalColWithDefault(BaseModel):
    id: int
    note: str | None = "n/a"


def test_lazy_read_missing_required_column_raises_csv(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "missing.csv"
    path.write_text("age\n10\n", encoding="utf-8")

    df = DataFrame[_TwoCols].read_csv(str(path))
    with pytest.raises(ValueError):
        _ = df.to_dict()


def test_lazy_read_missing_required_column_raises_ndjson(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "missing.ndjson"
    path.write_text(json.dumps({"age": 10}) + "\n", encoding="utf-8")

    df = DataFrame[_TwoCols].read_ndjson(str(path))
    with pytest.raises(ValueError):
        _ = df.to_dict()


def test_lazy_read_ignore_errors_payload_includes_full_row_dict(tmp_path: Path) -> None:
    """
    When ignore_errors=True, the failure payload should include the full row dict,
    including other columns besides the invalid one.
    """
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "two_cols.csv"
    path.write_text("id,age\n1,10\nbad,20\n2,30\n", encoding="utf-8")

    failures: list[dict[str, object]] = []

    def on_fail(items: list[dict[str, object]]) -> None:
        failures.extend(items)

    df = DataFrame[_TwoCols].read_csv(
        str(path),
        ignore_errors=True,
        on_validation_errors=on_fail,
    )
    assert df.to_dict() == {"id": [1, 2], "age": [10, 30]}
    assert len(failures) == 1
    row = failures[0]["row"]
    assert isinstance(row, dict)
    assert set(row.keys()) == {"id", "age"}
    assert row["age"] == 20


def test_lazy_read_missing_optional_column_fills_none_csv(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "opt.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    df = DataFrame[_OptionalCol].read_csv(str(path))
    assert df.to_dict() == {"id": [1, 2], "note": [None, None]}


def test_lazy_read_missing_optional_column_can_error_csv(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "opt_err.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    df = DataFrame[_OptionalCol].read_csv(str(path), fill_missing_optional=False)
    with pytest.raises(ValueError, match="Missing optional"):
        _ = df.to_dict()


def test_lazy_read_missing_optional_column_with_default_allows_fill_false_csv(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "opt_default.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    df = DataFrame[_OptionalColWithDefault].read_csv(
        str(path), fill_missing_optional=False
    )
    assert df.to_dict() == {"id": [1, 2], "note": ["n/a", "n/a"]}


def test_lazy_read_missing_optional_column_with_default_allows_fill_false_strict_mode(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pydantable_native._core")
    path = tmp_path / "opt_default_strict.csv"
    path.write_text("id\n1\n2\n", encoding="utf-8")

    df = DataFrame[_OptionalColWithDefault].read_csv(
        str(path), trusted_mode="strict", fill_missing_optional=False
    )
    assert df.to_dict() == {"id": [1, 2], "note": ["n/a", "n/a"]}


def test_lazy_read_missing_optional_column_error_message_variants_are_parsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    The missing-optional recovery for scan roots should not depend on a single
    brittle engine error string.
    """
    pytest.importorskip("pydantable_native._core")
    from pydantable.engine.native import NativePolarsEngine

    class ScanFileRoot:
        __module__ = "pydantable_native._core"

    root = ScanFileRoot()
    df = DataFrame[_OptionalCol]._from_scan_root(root)

    calls = {"n": 0}

    def fake_execute_plan(
        self: object,
        plan: object,
        data: object,
        *,
        as_python_lists: bool = False,
        streaming: bool = False,
        error_context: str | None = None,
    ) -> dict[str, list[object]]:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("ColumnNotFoundError: 'note'")
        return {"id": [1, 2]}

    monkeypatch.setattr(NativePolarsEngine, "execute_plan", fake_execute_plan)
    assert df.to_dict() == {"id": [1, 2], "note": [None, None]}
