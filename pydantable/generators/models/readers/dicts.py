import typing as _t

import pydantic

from pydantable.results import models as results


class MappingModelReader(results.MappingToModelResultsGenerator):
    def __init__(
        self,
        data: _t.Iterator[_t.Mapping],
        model: pydantic.BaseModel
    ) -> None:
        self.data = data
        self.model: pydantic.BaseModel = model

    def process(self, result: _t.Mapping) -> pydantic.BaseModel:
        validated_model: pydantic.BaseModel = self.model.model_validate(result)
        return validated_model