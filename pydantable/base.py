from __future__ import annotations
import typing as _t
import pydantic as _pydantic

from pydantable.generators.models.pipes import injectors
from pydantable.generators.models.pipes import readers
from pydantable.results import dataframe


class BaseTableModel(_pydantic.BaseModel):
    @classmethod
    def csv_reader(
        cls,
        f: _t.IO
    ) -> readers.CSVModelReaderPipe:
        return injectors.csv_reader(f, cls)
    
    @classmethod
    def dicts_reader(
        cls,
        data: _t.Iterable[_t.Mapping]
    ) -> readers.MappingModelReaderPipe:
        return injectors.dicts_reader(iter(data), cls)
    
    @classmethod
    def tuples_reader(
        cls,
        data: _t.Iterable[_t.Sequence],
        column_names: _t.Sequence[str]
    ) -> readers.TuplesModelReaderPipe:
        return injectors.tuples_reader(iter(data), column_names, cls)
    
    @classmethod
    def df_reader(
        cls,
        df: dataframe.DataFrame,
    ) -> readers.DataFrameModelReaderPipe:
        return injectors.df_reader(df, cls)