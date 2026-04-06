from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from tests._support.paths import repo_root


def _run_pyright_snippet(tmp_path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-m", "pyright", str(snippet)],
        cwd=repo_root(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_pyright_dataframe_as_schema_and_escape_hatches(tmp_path) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrame, Schema

    class Before(Schema):
        id: int
        x: int
        y: int

    class AfterJoin(Schema):
        id: int
        x: int
        y: int
        z: int

    class Right(Schema):
        id: int
        z: int

    def as_schema_ok(df: DataFrame[Before]) -> DataFrame[Before]:
        return df.as_schema(Before)

    def join_ok(
        left: DataFrame[Before], right: DataFrame[Right]
    ) -> DataFrame[AfterJoin]:
        return left.join_as_schema(right, AfterJoin, on=\"id\")
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
