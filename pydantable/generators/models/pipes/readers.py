import typing as _t

from pydantable.generators.models.pipes import base
from pydantable.generators.models.readers import csv as csv_readers
from pydantable.generators.models.readers import dicts as dicts_readers
from pydantable.generators.models.readers import tuples as tuples_readers
from pydantable.generators.models.readers import dataframe as df_readers

import pydantic


class CSVModelReaderPipe(base.ModelPipe, csv_readers.CSVModelReader):
    def __init__(self,
        f: _t.IO,
        model: pydantic.BaseModel,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        csv_readers.CSVModelReader.__init__(self, f, model)


class MappingModelReaderPipe(base.ModelPipe, dicts_readers.MappingModelReader):
    def __init__(self,
        data: _t.Iterator[_t.Mapping],
        model: pydantic.BaseModel,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        dicts_readers.MappingModelReader.__init__(self, data, model)


class TuplesModelReaderPipe(base.ModelPipe, tuples_readers.TuplesModelReader):
    def __init__(self,
        data: _t.Iterator[_t.Sequence],
        column_names: _t.Sequence[str],
        model: pydantic.BaseModel,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        tuples_readers.TuplesModelReader.__init__(self, data, column_names, model)


class DataFrameModelReaderPipe(base.ModelPipe, df_readers.DataFrameModelReader):
    def __init__(self,
        data: _t.Generator[tuple[int, dict], None, None],
        model: pydantic.BaseModel,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        df_readers.DataFrameModelReader.__init__(self, data, model)