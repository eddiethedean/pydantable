"""pandas-like method names on the core :class:`DataFrame` and :class:`DataFrameModel`.

``merge``/``assign``/``query`` mirror familiar pandas entry points where supported;
execution remains the Rust engine. Import ``DataFrame`` from this module for the
pandas-shaped API.
"""

from __future__ import annotations

from typing import Any

from .dataframe import DataFrame as CoreDataFrame
from .dataframe import GroupedDataFrame as CoreGroupedDataFrame
from .dataframe_model import DataFrameModel as CoreDataFrameModel
from .dataframe_model import GroupedDataFrameModel as CoreGroupedDataFrameModel
from .expressions import Expr
from .rust_engine import _require_rust_core
from .schema import Schema, _is_polars_dataframe


def _is_pandas_series(value: object) -> bool:
    return type(value).__name__ == "Series" and type(value).__module__.startswith(
        "pandas."
    )


class PandasDataFrame(CoreDataFrame):
    """``assign``, ``merge``, ``query``, ``columns``, ``shape``, and related."""

    def assign(self, **kwargs: Any) -> CoreDataFrame:
        for name, value in kwargs.items():
            if callable(value) and not isinstance(value, Expr):
                raise TypeError(
                    f"assign({name!r}=...): callable values are not supported; "
                    "use Expr expressions or literals."
                )
            if _is_pandas_series(value):
                raise TypeError(
                    f"assign({name!r}=...): pandas Series is not supported; "
                    "use column expressions or literals."
                )
        return self.with_columns(**kwargs)

    def merge(
        self,
        other: CoreDataFrame,
        *,
        how: str = "inner",
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
        suffixes: tuple[str, str] = ("_x", "_y"),
        indicator: bool = False,
        validate: str | None = None,
        **kw: Any,
    ) -> CoreDataFrame:
        if kw:
            raise TypeError(
                f"merge() got unsupported keyword arguments: {sorted(kw)!r}"
            )
        if left_on is not None or right_on is not None:
            raise NotImplementedError(
                "merge(left_on=..., right_on=...) is not supported; use join(on=...)."
            )
        if indicator:
            raise NotImplementedError(
                "merge(indicator=...) is not supported in the pydantable pandas UI."
            )
        if validate is not None:
            raise NotImplementedError(
                "merge(validate=...) is not supported in the pydantable pandas UI."
            )
        if on is None:
            raise TypeError("merge(...) requires on=...")
        suffix = suffixes[1] if suffixes and len(suffixes) >= 2 else "_right"
        return self.join(other, on=on, how=how, suffix=suffix)

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrame:
        raise NotImplementedError(
            "String query() is not supported; use filter(Expr) with typed column "
            "expressions (see pydantable.expressions)."
        )

    @property
    def columns(self) -> list[str]:
        return list(self._current_field_types.keys())

    @property
    def shape(self) -> tuple[int, int]:
        if not self._root_data:
            return (0, len(self._current_field_types))
        rd = self._root_data
        if _is_polars_dataframe(rd):
            return (len(rd), len(self._current_field_types))
        first = next(iter(rd.values()))
        return (len(first), len(self._current_field_types))

    @property
    def empty(self) -> bool:
        return self.shape[0] == 0

    @property
    def dtypes(self) -> dict[str, Any]:
        return dict(self._current_field_types)

    def head(self, n: int = 5) -> CoreDataFrame:
        """
        Return the first ``n`` rows after materializing the current logical plan.

        This is an eager, convenience API (not a zero-copy lazy slice).
        """
        data = self.collect(as_lists=True)
        sliced: dict[str, list[Any]]
        if not data:
            sliced = {name: [] for name in self._current_field_types}
        else:
            nrows = len(next(iter(data.values())))
            take = max(0, min(int(n), nrows))
            sliced = {k: v[:take] for k, v in data.items()}
        rust = _require_rust_core().make_plan(self.schema_fields())
        return self._from_plan(
            root_data=sliced,
            root_schema_type=self._current_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust,
        )

    def tail(self, n: int = 5) -> CoreDataFrame:
        """
        Return the last ``n`` rows after materializing the current logical plan.

        Eager; see :meth:`head`.
        """
        data = self.collect(as_lists=True)
        sliced: dict[str, list[Any]]
        if not data:
            sliced = {name: [] for name in self._current_field_types}
        else:
            nrows = len(next(iter(data.values())))
            take = max(0, min(int(n), nrows))
            start = max(0, nrows - take)
            sliced = {k: v[start:] for k, v in data.items()}
        rust = _require_rust_core().make_plan(self.schema_fields())
        return self._from_plan(
            root_data=sliced,
            root_schema_type=self._current_schema_type,
            current_schema_type=self._current_schema_type,
            rust_plan=rust,
        )

    def __getitem__(self, key: str | list[str]) -> Any:
        if isinstance(key, str):
            return self.col(key)
        if isinstance(key, list):
            if not key:
                raise ValueError("Column list must be non-empty.")
            return self.select(*key)
        raise TypeError(
            "DataFrame indexing supports a single column name (str) or list[str]."
        )

    def group_by(self, *keys: Any) -> PandasGroupedDataFrame:
        inner = super().group_by(*keys)
        return PandasGroupedDataFrame(inner._df, inner._keys)


class PandasGroupedDataFrame(CoreGroupedDataFrame):
    """Grouped frame with shorthand ``sum`` / ``mean`` / ``count`` over columns."""

    def sum(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("sum() requires at least one column name.")
        return self.agg(**{f"{c}_sum": ("sum", c) for c in columns})

    def mean(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("mean() requires at least one column name.")
        return self.agg(**{f"{c}_mean": ("mean", c) for c in columns})

    def count(self, *columns: str) -> CoreDataFrame:
        if not columns:
            raise TypeError("count() requires at least one column name.")
        return self.agg(**{f"{c}_count": ("count", c) for c in columns})


class PandasDataFrameModel(CoreDataFrameModel):
    """:class:`DataFrameModel` using :class:`PandasDataFrame` under the hood."""

    def assign(self, **kwargs: Any) -> CoreDataFrameModel:
        return type(self)._from_dataframe(self._df.assign(**kwargs))

    def merge(self, other: CoreDataFrameModel, **kwargs: Any) -> CoreDataFrameModel:
        if not isinstance(other, CoreDataFrameModel):
            raise TypeError("merge(other=...) expects another DataFrameModel instance.")
        return type(self)._from_dataframe(self._df.merge(other._df, **kwargs))

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrameModel:
        raise NotImplementedError(
            "String query() is not supported; use filter(Expr) with typed column "
            "expressions (see pydantable.expressions)."
        )

    @property
    def columns(self) -> list[str]:
        return list(self._df.columns)

    @property
    def shape(self) -> tuple[int, int]:
        return self._df.shape

    @property
    def empty(self) -> bool:
        return self._df.empty

    @property
    def dtypes(self) -> dict[str, Any]:
        return self._df.dtypes

    def head(self, n: int = 5) -> CoreDataFrameModel:
        return type(self)._from_dataframe(self._df.head(n))

    def tail(self, n: int = 5) -> CoreDataFrameModel:
        return type(self)._from_dataframe(self._df.tail(n))

    def __getitem__(self, key: str | list[str]) -> Any:
        return self._df[key]  # type: ignore[index]

    def group_by(self, *keys: Any) -> PandasGroupedDataFrameModel:
        g = self._df.group_by(*keys)
        return PandasGroupedDataFrameModel(g, type(self))


class PandasGroupedDataFrameModel(CoreGroupedDataFrameModel):
    """Model-level grouped aggregations with pandas naming."""

    def sum(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.sum(*columns))

    def mean(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.mean(*columns))

    def count(self, *columns: str) -> CoreDataFrameModel:
        return self._model_type._from_dataframe(self._grouped_df.count(*columns))


class DataFrame(PandasDataFrame):
    """Default export: pandas-flavored typed ``DataFrame``."""


class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame


__all__ = ["DataFrame", "DataFrameModel", "Expr", "Schema"]
