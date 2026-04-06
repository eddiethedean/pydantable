from __future__ import annotations

import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest

from tests._support.paths import repo_root

if TYPE_CHECKING:
    from pathlib import Path


def _run_ty_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
    snippet = tmp_path / "snippet.py"
    snippet.write_text(textwrap.dedent(code), encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-m", "ty", "check", str(snippet)],
        cwd=repo_root(),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_ty_keeps_concrete_model_type_through_schema_preserving_chains(
    tmp_path: Path,
) -> None:
    pytest.importorskip("ty")
    code = """
    from pydantable import DataFrameModel

    class U(DataFrameModel):
        id: int
        v: int | None

    def f(df: U) -> U:
        return (
            df.filter(df.id > 0)
            .sort("id")
            .distinct()
            .clip(lower=0, upper=10, subset="id")
            .fill_null(0)
            .drop_nulls(subset="id")
            .with_columns_fill_null("v", value=0)
            .explode("id")
            .unnest("id")
            .with_row_count()
            .explode_all()
            .unnest_all()
            .head(2)
        )
    """
    proc = _run_ty_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_ty_groupby_agg_as_model_helpers_return_target_type(tmp_path: Path) -> None:
    pytest.importorskip("ty")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        g: int
        v: int

    class After(DataFrameModel):
        g: int
        total: int

    def f(df: Before) -> After:
        grouped = df.group_by("g")
        return grouped.agg_as_model(After, total=("sum", "v"))
    """
    proc = _run_ty_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_ty_rolling_agg_as_model_helpers_return_target_type(tmp_path: Path) -> None:
    pytest.importorskip("ty")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        ts: str
        v: int

    class After(DataFrameModel):
        ts: str
        roll: int

    def f(df: Before) -> After:
        return df.rolling_agg_as_model(
            After,
            on="ts",
            column="v",
            window_size=3,
            op="sum",
            out_name="roll",
        )
    """
    proc = _run_ty_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_ty_melt_unpivot_join_as_model_helpers_return_target_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("ty")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        x: int
        y: int

    class AfterMelt(DataFrameModel):
        id: int
        variable: str
        value: int

    class AfterUnpivot(DataFrameModel):
        id: int
        variable: str
        value: int

    class Right(DataFrameModel):
        id: int
        z: int

    class AfterJoin(DataFrameModel):
        id: int
        x: int
        y: int
        z: int

    def melt_ok(df: Before) -> AfterMelt:
        return df.melt_as_model(AfterMelt, id_vars=["id"], value_vars=["x", "y"])

    def unpivot_ok(df: Before) -> AfterUnpivot:
        return df.unpivot_as_model(AfterUnpivot, index=["id"], on=["x", "y"])

    def join_ok(left: Before, right: Right) -> AfterJoin:
        return left.join_as_model(right, AfterJoin, on="id")
    """
    proc = _run_ty_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
