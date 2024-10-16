import typing as _t

from . import readers
from . import writers

import pydantic


def csv_reader(
    f: _t.IO,
    model: pydantic.BaseModel
) -> readers.CSVModelReaderPipe:
    return readers.CSVModelReaderPipe(
        f,
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