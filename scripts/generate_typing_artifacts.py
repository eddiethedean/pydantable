from __future__ import annotations

from pathlib import Path


_STUB_INIT = """\
from __future__ import annotations

from .dataframe_model import DataFrameModel

__all__ = ["DataFrameModel"]
"""


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

    def to_dict(self, *, streaming: bool | None = None) -> dict[str, list[Any]]: ...
    def collect(self, *, streaming: bool | None = None) -> Any: ...
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


def main() -> int:
    """
    Keep committed typing artifacts in sync.

    For now, artifacts are small and maintained as templates to ensure pyright/Pylance
    has a stable API surface (not full schema-evolution typing).
    """
    repo = Path(__file__).resolve().parents[1]
    pkg = repo / "python" / "pydantable"
    stub_pkg = repo / "typings" / "pydantable"

    _write_if_changed(pkg / "__init__.pyi", _STUB_INIT)
    _write_if_changed(pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL)
    (pkg / "py.typed").parent.mkdir(parents=True, exist_ok=True)
    (pkg / "py.typed").touch(exist_ok=True)

    _write_if_changed(stub_pkg / "__init__.pyi", _STUB_INIT)
    _write_if_changed(stub_pkg / "dataframe_model.pyi", _STUB_DATAFRAME_MODEL)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

