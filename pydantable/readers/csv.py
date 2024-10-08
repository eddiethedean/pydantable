import typing as _t
import csv

import pydantic

from pydantable.results.dictionary import DictResult
from pydantable.writers import csv as csv_writers
from pydantable.validators import dictionary


class CSVDictReader:
    def __init__(
        self,
        csv_file: _t.Iterable[str],
        fieldnames: _t.Sequence[str] | None = None,
        restkey: str | None = None,
        restval: str | None = None,
        dialect: str = "excel",
        *,
        delimiter: str = ",",
        quotechar: str | None = '"',
        escapechar: str | None = None,
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: str = "\r\n",
        quoting: int = 0,
        strict: bool = False
    ) -> None:
        self.reader = csv.DictReader(
            csv_file,
            fieldnames,
            restkey,
            restval,
            dialect,
            delimiter=delimiter,
            quotechar=quotechar,
            escapechar=escapechar,
            doublequote=doublequote,
            skipinitialspace=skipinitialspace,
            lineterminator=lineterminator,
            quoting=quoting,
            strict=strict
        )
    
    def __iter__(self) -> _t.Self:
        return self

    def __next__(self) -> DictResult:
        return self.read()

    def read(self) -> DictResult:
        try:
            row: dict = next(self.reader)
        except StopIteration as se:
            raise se
        except Exception as e:
            return e
        return row

    def validator(self, model: pydantic.BaseModel) -> dictionary.DictValidator:
        return dictionary.DictValidator(self, model)

    def writer(self, csv_file: 'SupportsWrite[str]') -> csv_writers.CSVDictWriter:
        return csv_writers.CSVDictWriter(self, csv_file)