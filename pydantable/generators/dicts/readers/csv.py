import typing as _t
import csv

from pydantable.results import dicts as results


class CSVDictReader(results.DictResultsGenerator):
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
        self.data = csv.DictReader(
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

    def process(self, result: dict) -> dict:
        return result