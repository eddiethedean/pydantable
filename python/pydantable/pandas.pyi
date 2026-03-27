from __future__ import annotations

from typing import Any
from typing_extensions import Self
from .dataframe import DataFrame as CoreDataFrame
from .dataframe import GroupedDataFrame as CoreGroupedDataFrame
from .dataframe_model import DataFrameModel as CoreDataFrameModel
from .dataframe_model import GroupedDataFrameModel as CoreGroupedDataFrameModel
from .expressions import Expr
from .rust_engine import _require_rust_core
from .schema import Schema

class PandasDataFrame(CoreDataFrame):

    def assign(self, **kwargs: Any) -> CoreDataFrame:
        ...

    def merge(self, other: CoreDataFrame, *, how: str='inner', on: str | list[str] | None=None, left_on: str | list[str] | None=None, right_on: str | list[str] | None=None, suffixes: tuple[str, str]=('_x', '_y'), indicator: bool=False, validate: str | None=None, **kw: Any) -> CoreDataFrame:
        ...

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrame:
        ...

    def head(self, n: int=5) -> CoreDataFrame:
        ...

    def tail(self, n: int=5) -> CoreDataFrame:
        ...

    def __getitem__(self, key: str | list[str]) -> Any:
        ...

    def group_by(self, *keys: Any) -> PandasGroupedDataFrame:
        ...

class PandasGroupedDataFrame(CoreGroupedDataFrame):

    def sum(self, *columns: str) -> CoreDataFrame:
        ...

    def mean(self, *columns: str) -> CoreDataFrame:
        ...

    def count(self, *columns: str) -> CoreDataFrame:
        ...

class PandasDataFrameModel(CoreDataFrameModel):

    def assign(self, **kwargs: Any) -> CoreDataFrameModel:
        ...

    def merge(self, other: CoreDataFrameModel, **kwargs: Any) -> CoreDataFrameModel:
        ...

    def query(self, expr: str, **kwargs: Any) -> CoreDataFrameModel:
        ...

    def head(self, n: int=5) -> Self:
        ...

    def tail(self, n: int=5) -> Self:
        ...

    def __getitem__(self, key: str | list[str]) -> Any:
        ...

    def group_by(self, *keys: Any) -> PandasGroupedDataFrameModel:
        ...

class PandasGroupedDataFrameModel(CoreGroupedDataFrameModel):

    def sum(self, *columns: str) -> CoreDataFrameModel:
        ...

    def mean(self, *columns: str) -> CoreDataFrameModel:
        ...

    def count(self, *columns: str) -> CoreDataFrameModel:
        ...

class DataFrame(PandasDataFrame):
    ...

class DataFrameModel(PandasDataFrameModel):
    _dataframe_cls = DataFrame

__all__ = ['DataFrame', 'DataFrameModel', 'Expr', 'Schema']
