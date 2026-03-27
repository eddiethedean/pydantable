from __future__ import annotations

import argparse
import ast
from pathlib import Path


_STUB_DATAFRAME_MODEL = """\
from __future__ import annotations

from collections.abc import Mapping, Sequence
from concurrent.futures import Executor
from typing import Any, Generic, Literal, TypeVar

from typing_extensions import Self

AfterModelT = TypeVar("AfterModelT", bound="DataFrameModel")
GroupedModelT = TypeVar("GroupedModelT", bound="DataFrameModel")


class DataFrameModel:
    _df: Any

    @classmethod
    def _from_dataframe(cls, df: Any) -> Self: ...

    def __init__(
        self,
        data: Any,
        *,
        trusted_mode: Literal["off", "shape_only", "strict"] | None = None,
        fill_missing_optional: bool = True,
        ignore_errors: bool = False,
        on_validation_errors: Any | None = None,
    ) -> None: ...

    def schema_fields(self) -> dict[str, Any]: ...

    def as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT: ...

    def try_as_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT | None: ...

    def assert_model(
        self,
        model: type[AfterModelT],
        *,
        validate_schema: bool = True,
    ) -> AfterModelT: ...

    def select(self, *cols: Any) -> DataFrameModel: ...
    def with_columns(self, **new_columns: Any) -> DataFrameModel: ...
    def drop(self, *columns: Any) -> DataFrameModel: ...
    def rename(self, columns: Mapping[str, str]) -> DataFrameModel: ...
    def join(
        self,
        other: DataFrameModel,
        *,
        on: str | Sequence[str] | None = None,
        left_on: Any = None,
        right_on: Any = None,
        how: str = "inner",
        suffix: str = "_right",
    ) -> DataFrameModel: ...

    def fill_null(
        self,
        value: Any = None,
        *,
        strategy: str | None = None,
        subset: Sequence[str] | None = None,
    ) -> DataFrameModel: ...
    def drop_nulls(self, subset: Sequence[str] | None = None) -> DataFrameModel: ...
    def melt(
        self,
        *,
        id_vars: Sequence[str] | None = None,
        value_vars: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel: ...
    def unpivot(
        self,
        *,
        index: Sequence[str] | None = None,
        on: Sequence[str] | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> DataFrameModel: ...
    def rolling_agg(
        self,
        *,
        on: str,
        column: str,
        window_size: int | str,
        op: str,
        out_name: str,
        by: Sequence[str] | None = None,
        min_periods: int = 1,
    ) -> DataFrameModel: ...
    def explode(self, columns: str | Sequence[str]) -> DataFrameModel: ...
    def unnest(self, columns: str | Sequence[str]) -> DataFrameModel: ...

    def to_dict(self, *, streaming: bool | None = None) -> dict[str, list[Any]]: ...
    def collect(
        self,
        *,
        as_lists: bool = False,
        as_numpy: bool = False,
        as_polars: bool | None = None,
        streaming: bool | None = None,
    ) -> Any: ...
    async def ato_dict(
        self,
        *,
        streaming: bool | None = None,
        executor: Executor | None = None,
    ) -> dict[str, list[Any]]: ...

    def filter(self, condition: Any) -> Self: ...
    def sort(self, *by: Any, descending: bool | Sequence[bool] = False) -> Self: ...
    def slice(self, offset: int, length: int) -> Self: ...
    def head(self, n: int = 5) -> Self: ...
    def tail(self, n: int = 5) -> Self: ...

    def group_by(self, *keys: Any) -> GroupedDataFrameModel[Self]: ...
    def group_by_dynamic(
        self,
        index_column: str,
        *,
        every: str,
        period: str | None = None,
        by: Sequence[str] | None = None,
    ) -> DynamicGroupedDataFrameModel[Self]: ...


class GroupedDataFrameModel(Generic[GroupedModelT]):
    _grouped_df: Any
    _model_type: type[GroupedModelT]

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None: ...
    def agg(self, **aggregations: Any) -> DataFrameModel: ...


class DynamicGroupedDataFrameModel(Generic[GroupedModelT]):
    _grouped_df: Any
    _model_type: type[GroupedModelT]

    def __init__(self, grouped_df: Any, model_type: type[GroupedModelT]) -> None: ...
    def agg(self, **aggregations: Any) -> DataFrameModel: ...
"""


def _write_if_changed(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    if existing == content:
        return
    path.write_text(content, encoding="utf-8")


def _differs(path: Path, content: str) -> bool:
    existing = path.read_text(encoding="utf-8") if path.exists() else None
    return existing != content


def _render_init_stub(init_py: Path) -> str:
    """
    Generate `__init__.pyi` from runtime `__init__.py`.

    This is intentionally conservative: we mirror *only* imports/exports that define
    the public API (`__all__`) to avoid drift and reduce pyright/Pylance noise.
    """
    src = init_py.read_text(encoding="utf-8")
    mod = ast.parse(src, filename=str(init_py))

    all_names: list[str] | None = None
    version_value: str | None = None
    import_nodes: list[ast.stmt] = []

    for node in mod.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "__future__"
                and any(alias.name == "annotations" for alias in node.names)
            ):
                continue
            import_nodes.append(node)
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "__all__":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, list) and all(isinstance(x, str) for x in value):
                    all_names = list(value)
            if isinstance(target, ast.Name) and target.id == "__version__":
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, str):
                    version_value = value

    if all_names is None:
        raise SystemExit(f"Could not statically parse __all__ from {init_py}")

    imports_src = "\n".join(ast.unparse(n) for n in import_nodes).strip()
    # Emit `__all__` deterministically without relying on synthesized AST locations.
    all_src = f"__all__ = {all_names!r}"

    lines: list[str] = ["from __future__ import annotations", ""]
    if imports_src:
        lines.append(imports_src)
        lines.append("")
    if version_value is not None:
        lines.append(f'__version__ = {version_value!r}')
        lines.append("")
    # Keep explicit __all__ for editor/typing tooling parity.
    lines.append(all_src)
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """
    Keep committed typing artifacts in sync.

    Typing artifacts are committed and generated deterministically to prevent drift.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Fail if generated artifacts would change (do not write).",
    )
    args = parser.parse_args(argv)

    repo = Path(__file__).resolve().parents[1]
    pkg = repo / "python" / "pydantable"
    stub_pkg = repo / "typings" / "pydantable"

    init_stub = _render_init_stub(pkg / "__init__.py")
    targets: list[tuple[Path, str]] = [
        (pkg / "__init__.pyi", init_stub),
        (pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL),
        (stub_pkg / "__init__.pyi", init_stub),
        (stub_pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL),
    ]

    if args.check:
        changed = [str(p.relative_to(repo)) for (p, c) in targets if _differs(p, c)]
        if changed:
            print("Typing artifacts are out of date. Re-run:")
            print("  python scripts/generate_typing_artifacts.py")
            print("Changed:")
            for p in changed:
                print(f"  - {p}")
            return 1
    else:
        _write_if_changed(pkg / "__init__.pyi", init_stub)
        _write_if_changed(pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL)
    (pkg / "py.typed").parent.mkdir(parents=True, exist_ok=True)
    (pkg / "py.typed").touch(exist_ok=True)

    if not args.check:
        _write_if_changed(stub_pkg / "__init__.pyi", init_stub)
        _write_if_changed(stub_pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

