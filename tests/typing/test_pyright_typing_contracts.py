from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests._support.paths import repo_root


def _run_pyright_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    cfg = tmp_path / "pyrightconfig.json"
    # Point pyright at the active repo venv so third-party imports (e.g. pydantic)
    # resolve reliably in CI and local dev.
    cfg.write_text(
        json.dumps(
            {
                "venvPath": str(repo_root()),
                "venv": ".venv310",
                "pythonVersion": "3.10",
            }
        ),
        encoding="utf-8",
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pyright",
            "--project",
            str(cfg),
            str(snippet),
        ],
        cwd=repo_root(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_pyright_structural_model_with_row_protocol_accepts_matching_row_model(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
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

    out = helper(Users({"id": [1]}))
    reveal_type(out)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_structural_model_with_row_protocol_rejects_wrong_row_model(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
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
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode != 0


def test_pyright_supports_lazy_async_materialize_accepts_model_and_awaitable(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
    code = """
    from __future__ import annotations

    import asyncio
    import tempfile
    from pathlib import Path
    from typing import Any

    from pydantable import DataFrameModel
    from pydantable.io import export_parquet
    from pydantable.typing import SupportsLazyAsyncMaterialize

    class Users(DataFrameModel):
        id: int

    async def run(m: SupportsLazyAsyncMaterialize[Any]) -> Any:
        return await m.acollect()

    async def main() -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "t.pq"
            export_parquet(p, {"id": [1]})
            df = Users({"id": [1]})
            _aread = Users.aread_parquet  # type: ignore[attr-defined]
            adf = _aread(p, trusted_mode="shape_only")
            a: SupportsLazyAsyncMaterialize[Any] = df
            b: SupportsLazyAsyncMaterialize[Any] = adf
            await run(a)
            await run(b)

    asyncio.run(main())
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_supports_lazy_async_materialize_rejects_without_acollect(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
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
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode != 0
