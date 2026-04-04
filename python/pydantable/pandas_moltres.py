"""Moltres SQL execution with :mod:`pydantable.pandas`-style method names.

Install with ``pip install "pydantable[moltres]"``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantable.dataframe_model import DataFrameModel as CoreDataFrameModel
from pydantable.pandas import PandasDataFrame, PandasDataFrameModel
from pydantable.sql_moltres import (
    SqlDataFrame as CoreSqlDataFrame,
)
from pydantable.sql_moltres import (
    SqlDataFrameModel as CoreSqlDataFrameModel,
)


class SqlDataFrame(CoreSqlDataFrame, PandasDataFrame):
    """Moltres SQL backend plus pandas-shaped API (``merge``, ``assign``, …)."""


class SqlDataFrameModel(CoreSqlDataFrameModel, PandasDataFrameModel):
    """Moltres SQL backend plus pandas-shaped :class:`DataFrameModel` methods."""

    _dataframe_cls = SqlDataFrame

    @classmethod
    def concat(
        cls,
        dfs: Sequence[CoreDataFrameModel],
        /,
        *,
        how: str | None = None,
        axis: int = 0,
        join: str = "outer",
        ignore_index: bool = False,
        keys: Any = None,
        levels: Any = None,
        names: Any = None,
        verify_integrity: Any = None,
        sort: Any = None,
        copy: Any = None,
        streaming: bool | None = None,
    ) -> CoreDataFrameModel:
        if len(dfs) < 2:
            raise ValueError("concat() requires at least two DataFrameModel inputs.")
        if not all(isinstance(df, CoreDataFrameModel) for df in dfs):
            raise TypeError("concat() expects a sequence of DataFrameModel objects.")
        out = SqlDataFrame.concat(
            [df._df for df in dfs],
            how=how,
            axis=axis,
            join=join,
            ignore_index=ignore_index,
            keys=keys,
            levels=levels,
            names=names,
            verify_integrity=verify_integrity,
            sort=sort,
            copy=copy,
            streaming=streaming,
        )
        return cls._from_dataframe(out)


__all__ = ["SqlDataFrame", "SqlDataFrameModel"]
