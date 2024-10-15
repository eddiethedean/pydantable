import typing as _t

import pydantic

from pydantable.results import dicts as results


class DictValidator(results.MappingResultsGenerator):
    def __init__(
        self,
        data: _t.Iterator[results.MappingResult],
        model: pydantic.BaseModel
    ) -> None:
        self.data: _t.Iterator[results.MappingResult] = data
        self.model: pydantic.BaseModel = model

    def process(self, result: results.MappingResult) -> dict:
        validated_model: pydantic.BaseModel = self.model.model_validate(result)
        return validated_model.model_dump()