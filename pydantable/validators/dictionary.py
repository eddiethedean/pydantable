import typing as _t
from collections.abc import Mapping

import pydantic

from pydantable.writers import csv as csv_writers
from pydantable.results.dictionary import DictResult, MappingResult


class DictValidator:
    def __init__(
        self,
        data: _t.Iterator[MappingResult],
        model: pydantic.BaseModel
    ) -> None:
        self.model: pydantic.BaseModel = model
        self.data: _t.Iterator[MappingResult] = data

    def __iter__(self) -> _t.Self:
        return self

    def __next__(self) -> DictResult:
        result: MappingResult = next(self.data)
        if isinstance(result, Mapping):
            return self.validate(result)
        if isinstance(result, Exception):
            return result

    def validate(self, row: MappingResult) -> DictResult:
        try:
            validated_model: pydantic.BaseModel = self.model.model_validate(row)
        except pydantic.ValidationError as e:
            return e
        return validated_model.model_dump()

    def writer(self, csv_file: 'SupportsWrite[str]') -> csv_writers.CSVDictWriter:
        return csv_writers.CSVDictWriter(self, csv_file)