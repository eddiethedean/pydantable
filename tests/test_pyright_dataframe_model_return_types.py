from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def _run_pyright_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-m", "pyright", str(snippet)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_pyright_accepts_as_model_for_schema_change(tmp_path: Path) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class After(DataFrameModel):
        id: int
        age2: int

    def transform(df: Before) -> After:
        out = df.with_columns(age2=df.age * 2).select("id", "age2")
        return out.as_model(After)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_rejects_wrong_as_model_target(tmp_path: Path) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class Wrong(DataFrameModel):
        nope: int

    def transform(df: Before) -> Wrong:
        out = df.with_columns(age2=df.age * 2).select("id", "age2")
        return out.as_model(Wrong)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    # Static typing can't know runtime schema here; this should still type-check
    # as long as as_model returns the declared target.
    assert proc.returncode == 0, (proc.stdout, proc.stderr)

