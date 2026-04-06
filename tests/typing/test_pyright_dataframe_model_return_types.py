# pyright: reportMissingModuleSource=false
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests._support.paths import repo_root


def _run_pyright_snippet(tmp_path: Path, code: str) -> subprocess.CompletedProcess[str]:
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


def test_pyright_sees_full_public_exports_from_init_stub(tmp_path: Path) -> None:
    pytest.importorskip("pyright")
    code = """
    import pydantable
    from pydantable import DataFrame, Expr, Schema, DataFrameModel

    reveal_type(pydantable.DataFrame)
    reveal_type(pydantable.Expr)
    reveal_type(pydantable.Schema)
    reveal_type(pydantable.DataFrameModel)

    # Smoke check that these names exist (the root stub used to hide them).
    _ = (DataFrame, Expr, Schema, DataFrameModel)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_sees_as_model_variants(tmp_path: Path) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int

    class After(DataFrameModel):
        id: int

    def f(df: Before) -> After:
        # These are primarily ergonomics for pyright users.
        out1 = df.assert_model(After)
        out2 = df.try_as_model(After)
        if out2 is None:
            return out1
        return out2
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_sees_facade_modules(tmp_path: Path) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import pandas, pyspark

    # Smoke check key facade exports exist for editors.
    _ = pandas.DataFrame
    _ = pandas.DataFrameModel
    _ = pyspark.DataFrame
    _ = pyspark.DataFrameModel
    _ = pyspark.sql.functions.col
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_accepts_literal_ip_wkb_annotated_model_and_as_model(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
    code = """
    from __future__ import annotations

    import ipaddress
    from typing import Annotated, Literal

    from pydantic import HttpUrl

    from pydantable import DataFrameModel, WKB

    class Before(DataFrameModel):
        mode: Literal["dev", "prod"]
        addr: ipaddress.IPv4Address
        g: WKB
        link: Annotated[str, HttpUrl]

    class After(DataFrameModel):
        mode: Literal["dev", "prod"]
        ip: ipaddress.IPv4Address
        g: WKB
        dup_mode: Literal["dev", "prod"]

    def narrowed(df: Before) -> Before:
        return df.filter(df.mode == "dev")

    def reshaped(df: Before) -> After:
        out = df.rename({"addr": "ip"}).drop("link")
        # Stubs type chained methods as DataFrameModel[Any]; use `df` for Expr refs.
        step = out.with_columns(dup_mode=df.mode).select("mode", "ip", "g", "dup_mode")
        return step.as_model(After)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_sees_dataframe_model_io_and_model_helpers(tmp_path: Path) -> None:
    """Hand ``dataframe_model.pyi`` exposes I/O classmethods and helpers for Pyright."""
    pytest.importorskip("pyright")
    code = """
    from typing import Any

    from pydantable import DataFrameModel

    class U(DataFrameModel):
        id: int

    # Lazy readers / async shims (surface only; no runtime I/O).
    _: object = U.read_parquet
    _: object = U.read_ndjson
    _: object = U.iter_ndjson
    _: object = U.aread_json
    _: object = U.write_parquet_batches
    _: object = U.row_model
    _: object = U.schema_model
    _: object = U.concat

    def narrowed(df: U) -> U:
        return df.filter(df.id == 1).distinct()

    def typed(df: U) -> DataFrameModel[Any]:
        return df.with_columns_cast("id", int)
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_keeps_concrete_model_type_through_schema_preserving_chains(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrameModel

    class U(DataFrameModel):
        id: int
        v: int | None

    def f(df: U) -> U:
        # These should preserve the model type (no schema changes).
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
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_groupby_agg_as_model_helpers_return_target_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
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
        out = grouped.agg(total=("sum", "v"))
        # `agg(...)` itself is schema-changing; these helpers are the typed escape
        # hatch.
        return grouped.agg_as_model(After, total=("sum", "v"))
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_rolling_agg_as_model_helpers_return_target_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        ts: str
        v: int

    class After(DataFrameModel):
        ts: str
        roll: int

    def f(df: Before) -> After:
        # rolling_agg is schema-changing; these helpers provide the typed escape hatch.
        return df.rolling_agg_as_model(
            After,
            on="ts",
            column="v",
            window_size=3,
            op="sum",
            out_name="roll",
        )
    """
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_pyright_melt_unpivot_join_as_model_helpers_return_target_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pyright")
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
    proc = _run_pyright_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)
