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
    """Schema-changing transforms can flow directly into a declared after-model."""
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
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_accepts_select_drop_rename_return_model_without_materialize(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int
        city: str

    class After(DataFrameModel):
        id: int
        years: int

    def transform(df: Before) -> After:
        out = df.rename({"age": "years"}).drop("city").select("id", "years")
        return out
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_accepts_select_drop_with_list_literals(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int
        city: str

    class After(DataFrameModel):
        id: int

    def transform(df: Before) -> After:
        return df.drop(["age", "city"]).select(["id"])
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_with_columns_infers_literal_and_arithmetic_types(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class After(DataFrameModel):
        id: int
        age: int
        age2: int
        flag: bool
        label: str

    def transform(df: Before) -> After:
        out = df.with_columns(
            age2=df.age * 2,
            flag=True,
            label="x",
        )
        return out
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_with_columns_rejects_type_mismatch_when_inferred(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class WrongAfter(DataFrameModel):
        id: int
        age: int
        age2: str

    def transform(df: Before) -> WrongAfter:
        out = df.with_columns(age2=df.age * 2)
        return out
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_accepts_join_and_groupby_agg_return_model_without_materialize(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class Cities(DataFrameModel):
        id: int
        city: str

    class Joined(DataFrameModel):
        id: int
        age: int
        city: str

    class AggOut(DataFrameModel):
        id: int
        age_mean: float

    def join_transform(users: Users, cities: Cities) -> Joined:
        return users.join(cities, on="id")

    def agg_transform(users: Users) -> AggOut:
        return users.group_by("id").agg(age_mean=("mean", "age"))
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_schema_preserving_transform_still_checks_model_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class Orders(DataFrameModel):
        order_id: int

    def bad_transform(df: Users) -> Orders:
        return df.filter(df.age > 0)
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_schema_preserving_chain_accepts_same_model_type(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    def stable(df: Users) -> Users:
        return (
            df.filter(df.age > 0)
            .sort("age")
            .slice(0, 2)
            .head(2)
            .tail(1)
        )
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_schema_preserving_fill_null_and_drop_nulls_preserve_model_type(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class Orders(DataFrameModel):
        order_id: int

    def ok(df: Users) -> Users:
        return df.fill_null(0).drop_nulls()

    def bad(df: Users) -> Orders:
        return df.fill_null(0).drop_nulls()
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_accepts_melt_unpivot_and_rolling_agg_return_model_without_materialize(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int
        city: str

    class Melted(DataFrameModel):
        id: int
        variable: str
        value: int

    class Unpivoted(DataFrameModel):
        id: int
        var: str
        val: int

    class Rolled(DataFrameModel):
        id: int
        age: int
        city: str
        age_mean: float

    def melt_it(df: Before) -> Melted:
        return df.melt(id_vars=["id"], value_vars=["age"])

    def unpivot_it(df: Before) -> Unpivoted:
        return df.unpivot(
            index=["id"], on=["age"], variable_name="var", value_name="val"
        )

    def roll_it(df: Before) -> Rolled:
        return df.rolling_agg(
            on="id", column="age", window_size=2, op="mean", out_name="age_mean"
        )
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_melt_requires_literal_id_vars_for_refinement(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class Melted(DataFrameModel):
        id: int
        variable: str
        value: int

    def ok(df: Before) -> Before:
        cols = ["id"]
        # Non-literal id_vars: plugin should not refine; this must stay Before.
        return df.melt(id_vars=cols, value_vars=["age"])

    def bad(df: Before) -> Melted:
        cols = ["id"]
        return df.melt(id_vars=cols, value_vars=["age"])
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_unpivot_requires_literal_index_for_refinement(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class Unpivoted(DataFrameModel):
        id: int
        variable: str
        value: int

    def ok(df: Before) -> Before:
        idx = ["id"]
        return df.unpivot(index=idx, on=["age"])

    def bad(df: Before) -> Unpivoted:
        idx = ["id"]
        return df.unpivot(index=idx, on=["age"])
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_rolling_agg_count_maps_to_int(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class After(DataFrameModel):
        id: int
        age: int
        age_count: int

    def roll(df: Before) -> After:
        return df.rolling_agg(
            on="id", column="age", window_size=2, op="count", out_name="age_count"
        )
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode == 0, (proc.stdout, proc.stderr)


def test_mypy_still_rejects_wrong_model_on_schema_preserving_chain(
    tmp_path: Path,
) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class Orders(DataFrameModel):
        order_id: int

    def bad(df: Users) -> Orders:
        return df.filter(df.age > 0).sort("age").head(1)
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_rejects_wrong_after_model_for_schema_changing_chain(
    tmp_path: Path,
) -> None:
    """Schema-changing chain should not type-check against an unrelated model."""
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Before(DataFrameModel):
        id: int
        age: int

    class WrongAfter(DataFrameModel):
        nope: int

    def bad(df: Before) -> WrongAfter:
        return df.with_columns(age2=df.age * 2).select("id", "age2")
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_rejects_wrong_after_model_for_join(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class Cities(DataFrameModel):
        id: int
        city: str

    class WrongJoinOut(DataFrameModel):
        id: int
        missing: int

    def bad(users: Users, cities: Cities) -> WrongJoinOut:
        return users.join(cities, on="id")
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout


def test_mypy_rejects_wrong_after_model_for_groupby_agg(tmp_path: Path) -> None:
    pytest.importorskip("mypy")
    code = """
    from pydantable import DataFrameModel

    class Users(DataFrameModel):
        id: int
        age: int

    class WrongAggOut(DataFrameModel):
        id: int
        age_max: int

    def bad(users: Users) -> WrongAggOut:
        return users.group_by("id").agg(age_mean=("mean", "age"))
    """
    proc = _run_mypy_snippet(tmp_path, code)
    assert proc.returncode != 0
    assert "Incompatible return value type" in proc.stdout
