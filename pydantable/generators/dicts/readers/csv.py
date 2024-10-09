import typing as _t
import csv

from pydantable.generators.dicts.base import chain
from pydantable.generators.dicts.validators import dicts as validators
from pydantable.results import dicts as dict_results
from pydantable.generators.dicts.base import iter


class CSVDictReader(iter.BaseIter, chain.ChainBase, validators.ValidatorMixin):
    def __init__(
        self,
        csv_file: _t.Iterable[str],
        fieldnames: _t.Sequence[str] | None = None,
        restkey: str | None = None,
        restval: str | None = None,
        dialect: str = "excel",
        mode: iter.ReturnMode = iter.ReturnMode.PASSIVE,
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
        super().__init__(self.reader, mode)
    
    def __iter__(self) -> _t.Self:
        return self

    def __next__(self) -> dict_results.MappingResult:
        return self.read()

    def read(self) -> dict_results.MappingResult:
        try:
            row: dict = next(self.reader)
        except StopIteration as se:
            raise se
        except Exception as e:
            return self.filter_error(e)
        return row