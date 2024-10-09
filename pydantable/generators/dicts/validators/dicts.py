import typing as _t
from collections.abc import Mapping
from abc import ABC, abstractmethod

import pydantic

from pydantable.generators.dicts.base import chain
from pydantable.generators.dicts.base import iter
from pydantable.results import dicts as dict_results




class DictValidator(iter.BaseIter, chain.ChainBase):
    def __init__(
        self,
        data: _t.Iterator[dict_results.MappingResult],
        model: pydantic.BaseModel,
        mode: iter.ReturnMode = iter.ReturnMode.PASSIVE
    ) -> None:
        super().__init__(data, mode)
        self.model: pydantic.BaseModel = model
        self.mode: iter.ReturnMode = mode

    def __next__(self) -> dict_results.MappingResult:
        result: dict_results.MappingResult = super().__next__()
        if isinstance(result, Mapping):
            return self.validate(result)
        if isinstance(result, Exception):
            return self.filter_error(result)
  
    def validate(
        self,
        row: dict_results.MappingResult
    ) -> dict_results.MappingResult:
        try:
            validated_model: pydantic.BaseModel = self.model.model_validate(row)
        except pydantic.ValidationError as e:
            return self.filter_error(e)
        return validated_model.model_dump()


class ValidatorMixin(ABC):
    @abstractmethod
    def __iter__(self) -> _t.Self:
        ...

    @abstractmethod
    def __next__(self) -> dict_results.MappingResult:
        ...

    def validator(
        self,
        model: pydantic.BaseModel,
        mode: iter.ReturnMode = iter.ReturnMode.PASSIVE
    ) -> DictValidator:
        return DictValidator(self, model, mode)