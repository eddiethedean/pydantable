from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def _run_mypy_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    env = dict(os.environ)
    env.setdefault("MYPYPATH", "python")
    return subprocess.run(
        [sys.executable, "-m", "mypy", str(snippet)],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_mypy_accepts_matching_dataframe_model_return_type(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int

    def build_users() -> Users:
        return Users({"id": [1, 2]})
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_rejects_mismatched_dataframe_model_return_type(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int

    class Orders(DataFrameModel):
        order_id: int

    def build_users() -> Users:
        return Orders({"order_id": [1]})
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_accepts_transformed_schema_wrapped_in_new_model(tmp_path: Path) -> None:
    """Transforms can be wrapped by materializing then constructing a new model."""
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class UsersWithAge2(DataFrameModel):
        id: int
        age2: int

    def build_users_with_age2() -> UsersWithAge2:
        df = Users({"id": [1, 2], "age": [10, 20]})
        out = df.with_columns(age2=df.age * 2).select("id", "age2")
        return UsersWithAge2(out.to_dict())
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_rejects_wrong_wrapped_model_for_transformed_schema(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class UsersWithAge2(DataFrameModel):
        id: int
        age2: int

    class UsersWithWrongCols(DataFrameModel):
        id: int
        nope: int

    def build_users_with_age2() -> UsersWithAge2:
        df = Users({"id": [1, 2], "age": [10, 20]})
        out = df.with_columns(age2=df.age * 2).select("id", "age2")
        return UsersWithWrongCols(out.to_dict())
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_cannot_verify_schema_transform_return_model_without_materialize(
    tmp_path: Path,
) -> None:
    """Today, transform methods return DataFrameModel, not a declared after-model."""
    pytest.importorskip("mypy")
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
        return out
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout
