from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests._support.paths import repo_root


def _run_mypy_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    env = dict(os.environ)
    env.setdefault("MYPYPATH", "python")
    return subprocess.run(
        [sys.executable, "-m", "mypy", str(snippet)],
        cwd=repo_root(),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_mypy_structural_model_with_row_protocol_accepts_matching_row_model(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from __future__ import annotations

    from pydantic import BaseModel

    from pydantable import DataFrameModel
    from pydantable.typing import DataFrameModelWithRow

    class UserRow(BaseModel):
        id: int

    class Users(DataFrameModel):
        id: int
        RowModel: type[UserRow]

    def helper(m: DataFrameModelWithRow[UserRow]) -> list[UserRow]:
        return m.rows()

    out: list[UserRow] = helper(Users({"id": [1]}))
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_structural_model_with_row_protocol_rejects_wrong_row_model(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from __future__ import annotations

    from pydantic import BaseModel

    from pydantable import DataFrameModel
    from pydantable.typing import DataFrameModelWithRow

    class UserRow(BaseModel):
        id: int

    class OtherRow(BaseModel):
        order_id: int

    class Users(DataFrameModel):
        id: int
        RowModel: type[UserRow]

    class Orders(DataFrameModel):
        order_id: int
        RowModel: type[OtherRow]

    def helper(m: DataFrameModelWithRow[UserRow]) -> None:
        _ = m.rows()

    helper(Users({"id": [1]}))
    helper(Orders({"order_id": [1]}))
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Argument 1 to" in proc.stdout


def test_mypy_supports_lazy_async_materialize_rejects_without_acollect(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from __future__ import annotations

    from typing import Any

    from pydantable.typing import SupportsLazyAsyncMaterialize

    class NotOk:
        pass

    def f(m: SupportsLazyAsyncMaterialize[Any]) -> None:
        pass

    f(NotOk())
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Argument 1 to" in proc.stdout or "NotOk" in proc.stdout
