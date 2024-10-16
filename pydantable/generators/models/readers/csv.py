import typing as _t
import csv

import pydantic

from pydantable.results import models as results


class CSVModelReader(results.DictToModelResultsGenerator):
    def __init__(
        self,
        csv_file: _t.Iterable[str],
        model: pydantic.BaseModel,
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
        self.model: pydantic.BaseModel = model

    def process(self, result: dict) -> pydantic.BaseModel:
        validated_model: pydantic.BaseModel = self.model.model_validate(result)
        return validated_model