from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from typing_extensions import Self

from .dataframe import DataFrame as CoreDataFrame
from .dataframe import GroupedDataFrame as CoreGroupedDataFrame
from .dataframe_model import DataFrameModel as CoreDataFrameModel
from .dataframe_model import GroupedDataFrameModel as CoreGroupedDataFrameModel
from .expressions import ColumnRef, Expr
from .pandas_sql_dataframe import SqlDataFrame, SqlDataFrameModel
from .schema import Schema
from .selectors import Selector

def wide_to_long(
    df: CoreDataFrame,
    stubnames: str | list[str],
    i: str | list[str],
    j: str,
    *,
    sep: str = "_",
    suffix: str = "\\d+",
    value_name: str | None = None,
) -> CoreDataFrame: ...

class PandasDataFrame(CoreDataFrame):
    @classmethod
    def concat(
        cls,
        objs: Sequence[CoreDataFrame],
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
    ) -> CoreDataFrame: ...
    def assign(self, **kwargs: Any) -> CoreDataFrame: ...
    def merge(
        self,
        other: CoreDataFrame,
        *,
        how: str = "inner",
        on: str | list[str] | None = None,
        left_on: str | list[str] | None = None,
        right_on: str | list[str] | None = None,
        left_by: str | list[str] | None = None,
        right_by: str | list[str] | None = None,
        left_index: bool = False,
        right_index: bool = False,
        suffixes: tuple[str, str] = ("_x", "_y"),
        sort: bool = False,
        copy: bool | None = None,
        indicator: bool = False,
        validate: str | None = None,
        **kw: Any,
    ) -> CoreDataFrame: ...
    def query(
        self,
        expr: str,
        *,
        local_dict: dict[str, object] | None = None,
        global_dict: dict[str, object] | None = None,
        engine: str = "python",
        inplace: bool = False,
        **kwargs: Any,
    ) -> CoreDataFrame: ...
    def sort_values(
        self,
        by: str | list[str],
        *,
        ascending: bool | list[bool] = True,
        kind: str | None = None,
        na_position: str | None = None,
        ignore_index: bool = False,
        key: Any = None,
    ) -> CoreDataFrame: ...
    def drop(self, *args: Any, **kwargs: Any) -> CoreDataFrame: ...
    def rename(self, *args: Any, **kwargs: Any) -> CoreDataFrame: ...
    def fillna(
        self,
        value: Any = None,
        *,
        method: str | None = None,
        axis: Any = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: Any = None,
        subset: str | list[str] | None = None,
    ) -> CoreDataFrame: ...
    def astype(
        self, dtype: Any, *, copy: bool | None = None, errors: str = "raise"
    ) -> CoreDataFrame: ...
    def to_pandas(self) -> Any: ...
    def head(self, n: int = 5) -> CoreDataFrame: ...
    def tail(self, n: int = 5) -> CoreDataFrame: ...
    def __getitem__(self, key: str | list[str]) -> Any: ...

    class _ILoc:
        def __init__(self, df: PandasDataFrame): ...
        def __getitem__(self, key: int | slice) -> CoreDataFrame: ...
        def _nrows_or_none(self) -> int | None: ...

    @property
    def iloc(self) -> _ILoc: ...

    class _Loc:
        def __init__(self, df: PandasDataFrame): ...
        def __getitem__(self, key: object) -> CoreDataFrame: ...

    @property
    def loc(self) -> _Loc: ...
    def group_by(
        self,
        *keys: Any,
        maintain_order: bool = False,
        drop_nulls: bool = True,
        dropna: Any = None,
        as_index: Any = None,
        sort: Any = None,
        observed: Any = None,
    ) -> PandasGroupedDataFrame: ...
    def drop_duplicates(
        self,
        subset: str | list[str] | None = None,
        *,
        keep: str | bool = "first",
        inplace: bool = False,
        ignore_index: bool = False,
    ) -> CoreDataFrame: ...
    def duplicated(
        self, subset: Sequence[str] | None = None, *, keep: str | bool = "first"
    ) -> CoreDataFrame: ...
    def isna(self) -> CoreDataFrame: ...
    def isnull(self) -> CoreDataFrame: ...
    def notna(self) -> CoreDataFrame: ...
    def notnull(self) -> CoreDataFrame: ...
    def dropna(
        self,
        *,
        axis: int = 0,
        how: str = "any",
        subset: str | list[str] | None = None,
        inplace: Any = None,
        thresh: Any = None,
    ) -> CoreDataFrame: ...
    def get_dummies(
        self,
        columns: list[str],
        *,
        prefix: str | Mapping[str, str] | None = None,
        prefix_sep: str = "_",
        drop_first: bool = False,
        dummy_na: bool = False,
        dtype: str = "bool",
        max_categories: int = 512,
    ) -> CoreDataFrame: ...
    def pivot(
        self,
        *,
        index: str | Sequence[str] | Selector,
        columns: str | Selector | ColumnRef,
        values: str | Sequence[str] | Selector,
        aggregate_function: str = "first",
        pivot_values: Sequence[Any] | None = None,
        sort_columns: bool = False,
        separator: str = "_",
        streaming: bool | None = None,
    ) -> CoreDataFrame: ...
    def factorize_column(self, column: str) -> tuple[list[int], list[Any]]: ...
    def cut(
        self,
        column: str,
        bins: Any,
        *,
        new_column: str | None = None,
        labels: Any = None,
        right: bool = True,
        include_lowest: bool = False,
        duplicates: str = "raise",
    ) -> CoreDataFrame: ...
    def qcut(
        self,
        column: str,
        q: Any,
        *,
        new_column: str | None = None,
        duplicates: str = "raise",
    ) -> CoreDataFrame: ...
    def melt(
        self,
        *,
        id_vars: str | Sequence[str] | Selector | None = None,
        value_vars: str | Sequence[str] | Selector | None = None,
        variable_name: str = "variable",
        value_name: str = "value",
        streaming: bool | None = None,
        var_name: str | None = None,
    ) -> CoreDataFrame: ...
    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any] | list[dict[str, Any]],
        orient: str = "columns",
        *,
        columns: list[str] | None = None,
    ) -> Any: ...
    def wide_to_long(
        self,
        stubnames: str | list[str],
        i: str | list[str],
        j: str,
        *,
        sep: str = "_",
        suffix: str = "\\d+",
        value_name: str | None = None,
    ) -> CoreDataFrame: ...
    def stack(
        self,
        *,
        id_vars: str | list[str],
        value_vars: str | list[str] | None = None,
        var_name: str = "variable",
        value_name: str = "value",
    ) -> CoreDataFrame: ...
    def unstack(
        self,
        *,
        index: str | list[str],
        columns: str,
        values: str | list[str],
        aggregate_function: str = "first",
        streaming: bool | None = None,
    ) -> CoreDataFrame: ...
    def where(self, cond: Expr, other: Any | None = None) -> CoreDataFrame: ...
    def mask(self, cond: Expr, other: Any | None = None) -> CoreDataFrame: ...
    def rank(
        self,
        *,
        axis: int = 0,
        method: str = "average",
        ascending: bool = True,
        na_option: str = "keep",
        pct: bool = False,
    ) -> CoreDataFrame: ...
    def sample(
        self,
        n: int | None = None,
        frac: float | None = None,
        *,
        fraction: float | None = None,
        seed: int | None = None,
        with_replacement: bool = False,
        replace: bool = False,
        random_state: int | None = None,
        axis: Any = 0,
    ) -> CoreDataFrame: ...
    def take(self, indices): ...
    def sort_index(self, *args: Any, **kwargs: Any) -> CoreDataFrame: ...
    def combine_first(
        self, other: CoreDataFrame, *, on: list[str]
    ) -> CoreDataFrame: ...
    def update(self, other: CoreDataFrame, *, on: list[str]) -> CoreDataFrame: ...
    def compare(
        self, other: CoreDataFrame, *, rtol: float = 1e-05, atol: float = 0.0
    ) -> CoreDataFrame: ...
    def corr(self, method: str = "pearson", min_periods: int = 1): ...
    def cov(self, min_periods: int = 1): ...
    def reindex(
        self, other: CoreDataFrame, *, on: str | list[str], **join_kw: Any
    ) -> CoreDataFrame: ...
    def reindex_like(self, other: CoreDataFrame, **join_kw: Any) -> CoreDataFrame: ...
    def align(
        self, other: CoreDataFrame, *, on: list[str], join: str = "outer"
    ) -> tuple[CoreDataFrame, CoreDataFrame]: ...
    def set_index(
        self,
        keys: str | list[str],
        *,
        drop: bool = True,
        append: bool = False,
        inplace: bool = False,
    ) -> CoreDataFrame: ...
    def reset_index(
        self, level: Any = None, *, drop: bool = False, inplace: bool = False
    ) -> CoreDataFrame: ...
    def eval(
        self, expr: str, *, local_dict: Any = None, global_dict: Any = None, **kw: Any
    ) -> CoreDataFrame: ...
    @property
    def T(self) -> CoreDataFrame: ...
    def transpose(self, *args: Any, **kwargs: Any) -> CoreDataFrame: ...
    def dot(self, other: CoreDataFrame) -> CoreDataFrame: ...
    def insert(
        self, loc: int, column: str, value: Any, allow_duplicates: bool = False
    ) -> CoreDataFrame: ...
    def pop(self, item: str) -> tuple[Expr, CoreDataFrame]: ...
    def interpolate(
        self,
        *,
        method: str = "linear",
        axis: int = 0,
        limit_direction: str = "forward",
        **kwargs: Any,
    ) -> CoreDataFrame: ...

    class _Ewm:
        __slots__ = ("_adjust", "_alpha", "_com", "_df", "_min_periods", "_span")

        def __init__(
            self,
            df: PandasDataFrame,
            *,
            com: float | None,
            span: float | None,
            alpha: float | None,
            adjust: bool,
            min_periods: int,
        ) -> None: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...

    class _Expanding:
        __slots__ = ("_df",)

        def __init__(self, df: PandasDataFrame): ...
        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...
        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...

    def expanding(self, min_periods: int = 1) -> _Expanding: ...
    def ewm(
        self,
        *,
        com: float | None = None,
        span: float | None = None,
        alpha: float | None = None,
        adjust: bool = True,
        min_periods: int = 0,
    ) -> PandasDataFrame._Ewm: ...
    def nlargest(
        self, n: int, columns: str | list[str], *, keep: str = "all"
    ) -> CoreDataFrame: ...
    def nsmallest(
        self, n: int, columns: str | list[str], *, keep: str = "all"
    ) -> CoreDataFrame: ...
    def isin(self, values: Any) -> CoreDataFrame: ...
    def explode(self, *args: Any, **kwargs: Any) -> CoreDataFrame: ...
    def copy(self, deep: bool = False) -> CoreDataFrame: ...
    def pipe(self, fn: Any, *args: Any, **kwargs: Any) -> Any: ...
    def filter(
        self,
        *args: Any,
        items: list[str] | None = None,
        like: str | None = None,
        regex: str | None = None,
        axis: Any = 0,
    ) -> CoreDataFrame: ...

    class _Rolling:
        def __init__(
            self,
            df: PandasDataFrame,
            *,
            window: int,
            min_periods: int,
            partition_by: list[str] | None = None,
        ): ...
        def _apply(
            self, op: str, column: str, out_name: str | None
        ) -> CoreDataFrame: ...
        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...
        def min(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def max(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...

    def rolling(self, *, window: int, min_periods: int = 1) -> _Rolling: ...

class PandasGroupedDataFrame(CoreGroupedDataFrame):
    class _Rolling:
        __slots__ = ("_inner",)

        def __init__(
            self, gdf: PandasGroupedDataFrame, *, window: int, min_periods: int
        ): ...
        def sum(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...
        def min(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def max(self, column: str, *, out_name: str | None = None) -> CoreDataFrame: ...
        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrame: ...

    def rolling(self, *, window: int, min_periods: int = 1) -> _Rolling: ...
    def size(self) -> CoreDataFrame: ...
    def sum(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame: ...
    def mean(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame: ...
    def count(self, *columns: str, streaming: bool | None = None) -> CoreDataFrame: ...
    def nunique(self, *columns: str) -> CoreDataFrame: ...
    def first(self, *columns: str) -> CoreDataFrame: ...
    def last(self, *columns: str) -> CoreDataFrame: ...
    def median(self, *columns: str) -> CoreDataFrame: ...
    def std(self, *columns: str) -> CoreDataFrame: ...
    def var(self, *columns: str) -> CoreDataFrame: ...
    def agg_multi(self, **spec: list[str]) -> CoreDataFrame: ...

class PandasDataFrameModel(CoreDataFrameModel):
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
    ) -> CoreDataFrameModel: ...
    def assign(self, **kwargs: Any) -> CoreDataFrameModel: ...
    def merge(self, other: CoreDataFrameModel, **kwargs: Any) -> CoreDataFrameModel: ...
    def query(self, expr: str, **kwargs: Any) -> CoreDataFrameModel: ...
    def head(self, n: int = 5) -> Self: ...
    def tail(self, n: int = 5) -> Self: ...
    def sort_values(self, by: str | list[str], **kwargs: Any) -> Self: ...
    def drop_duplicates(self, *args: Any, **kwargs: Any) -> Self: ...
    def duplicated(self, *args: Any, **kwargs: Any) -> Self: ...
    def drop(self, *args: Any, **kwargs: Any) -> Self: ...
    def rename(self, *args: Any, **kwargs: Any) -> Self: ...
    def fillna(self, *args: Any, **kwargs: Any) -> Self: ...
    def astype(self, *args: Any, **kwargs: Any) -> Self: ...
    def nlargest(self, *args: Any, **kwargs: Any) -> Self: ...
    def nsmallest(self, *args: Any, **kwargs: Any) -> Self: ...
    def isin(self, values: Any) -> Self: ...
    def explode(self, *args: Any, **kwargs: Any) -> Self: ...
    def copy(self, *args: Any, **kwargs: Any) -> Self: ...
    def pipe(self, fn: Any, *args: Any, **kwargs: Any) -> Any: ...
    def filter(self, *args: Any, **kwargs: Any) -> Self: ...

    class _ModelILoc:
        __slots__ = ("_m",)

        def __init__(self, m: PandasDataFrameModel): ...
        def __getitem__(self, key: int | slice) -> CoreDataFrameModel: ...

    class _ModelLoc:
        __slots__ = ("_m",)

        def __init__(self, m: PandasDataFrameModel): ...
        def __getitem__(self, key: object) -> CoreDataFrameModel: ...

    @property
    def iloc(self) -> _ModelILoc: ...
    @property
    def loc(self) -> _ModelLoc: ...
    def isna(self) -> Self: ...
    def isnull(self) -> Self: ...
    def notna(self) -> Self: ...
    def notnull(self) -> Self: ...
    def dropna(self, *args: Any, **kwargs: Any) -> Self: ...
    def melt(self, *args: Any, **kwargs: Any) -> Self: ...

    class _ModelRolling:
        __slots__ = ("_inner", "_m")

        def __init__(
            self, m: PandasDataFrameModel, *, window: int, min_periods: int
        ): ...
        def sum(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def min(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def max(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...

    def rolling(self, *, window: int, min_periods: int = 1) -> _ModelRolling: ...
    def __getitem__(self, key: str | list[str]) -> Any: ...
    def group_by(self, *keys: Any, **kwargs: Any) -> PandasGroupedDataFrameModel: ...

class PandasGroupedDataFrameModel(CoreGroupedDataFrameModel):
    class _ModelGroupedRolling:
        __slots__ = ("_inner", "_mt")

        def __init__(self, mt: type, inner: Any): ...
        def sum(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def mean(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def min(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def max(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...
        def count(
            self, column: str, *, out_name: str | None = None
        ) -> CoreDataFrameModel: ...

    def rolling(self, *, window: int, min_periods: int = 1) -> _ModelGroupedRolling: ...
    def sum(
        self, *columns: str, streaming: bool | None = None
    ) -> CoreDataFrameModel: ...
    def mean(
        self, *columns: str, streaming: bool | None = None
    ) -> CoreDataFrameModel: ...
    def count(
        self, *columns: str, streaming: bool | None = None
    ) -> CoreDataFrameModel: ...
    def size(self) -> CoreDataFrameModel: ...
    def nunique(self, *columns: str) -> CoreDataFrameModel: ...
    def first(self, *columns: str) -> CoreDataFrameModel: ...
    def last(self, *columns: str) -> CoreDataFrameModel: ...
    def median(self, *columns: str) -> CoreDataFrameModel: ...
    def std(self, *columns: str) -> CoreDataFrameModel: ...
    def var(self, *columns: str) -> CoreDataFrameModel: ...
    def agg_multi(self, **spec: list[str]) -> CoreDataFrameModel: ...

class DataFrame(PandasDataFrame): ...

class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame

__all__ = [
    "DataFrame",
    "DataFrameModel",
    "Expr",
    "Schema",
    "SqlDataFrame",
    "SqlDataFrameModel",
]
