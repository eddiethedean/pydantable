import typing as _t

import tinytim as tt
import pydantic

from pydantable.results import dataframe as df
from . import readers
from . import writers


def csv_reader(
    f: _t.IO,
    model: pydantic.BaseModel
) -> readers.CSVModelReaderPipe:
    return readers.CSVModelReaderPipe(
        f,
        model,
        writers.CSVModelWriterPipe
    )


def dicts_reader(
    data: _t.Iterator[_t.Mapping],
    model: pydantic.BaseModel
) -> readers.MappingModelReaderPipe:
    return readers.MappingModelReaderPipe(
        data,
        model,
        writers.CSVModelWriterPipe
    )


def df_reader(
    df: df.DataFrame,
    model: pydantic.BaseModel
) -> readers.DataFrameModelReaderPipe:
    data: _t.Generator[tuple[int, dict], None, None] = tt.rows.iterrows(df)
    return readers.DataFrameModelReaderPipe(
        data,
        model,
        writers.CSVModelWriterPipe
    )


def tuples_reader(
    data: _t.Iterator[_t.Sequence],
    column_names: _t.Sequence[str],
    model: pydantic.BaseModel
) -> readers.TuplesModelReaderPipe:
    return readers.TuplesModelReaderPipe(
        data,
        column_names,
        model,
        writers.CSVModelWriterPipe
    )


def csv_writer(
    data: _t.Iterator[pydantic.BaseModel],
    f: _t.IO
) -> writers.CSVModelWriterPipe:
    return writers.CSVModelWriterPipe(
        data,
        f,
        writers.CSVModelWriterPipe
    )


