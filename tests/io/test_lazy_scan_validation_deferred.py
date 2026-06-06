"""Lazy scan roots defer row validation until materialization."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path
from pydantable import DataFrame
from pydantic import BaseModel


class _Row(BaseModel):
    id: int


def test_lazy_csv_scan_construction_succeeds_before_validation(tmp_path: Path) -> None:
    pytest.importorskip("pydantable_native._core")

    path = tmp_path / "defer.csv"
    path.write_text("id\n1\nbad\n2\n", encoding="utf-8")

    df = DataFrame[_Row].read_csv(str(path))
    assert type(df._root_data).__name__ == "ScanFileRoot"

    with pytest.raises(ValueError):
        _ = df.to_dict()
