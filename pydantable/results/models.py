import typing as _t
from collections.abc import Generator
from abc import abstractmethod

import pydantic

from pydantable.results import dicts

ModelResult = pydantic.BaseModel | Exception


class ModelResultsGenerator(Generator):
    data: _t.Iterator[ModelResult]

    def send(self, ignored_arg) -> ModelResult:
        try:
            return self.next()
        except StopIteration as si:
            raise si
        except Exception as e:
            return e
        
    def throw(self, type=None, value=None, traceback=None) -> _t.NoReturn:
        raise StopIteration
    
    def next(self) -> ModelResult:
        result: ModelResult = next(self.data)
        if isinstance(result, pydantic.BaseModel):
            return self.process(result)
        if isinstance(result, Exception):
            return result

    @abstractmethod
    def process(self, result: pydantic.BaseModel) -> pydantic.BaseModel:
        ...


class DictToModelResultsGenerator(Generator):
    data: _t.Iterator[dicts.DictResult]

    def send(self, ignored_arg) -> ModelResult:
        try:
            return self.next()
        except StopIteration as si:
            raise si
        except Exception as e:
            return e
        
    def throw(self, type=None, value=None, traceback=None) -> _t.NoReturn:
        raise StopIteration
    
    def next(self) -> ModelResult:
        result: dicts.DictResult = next(self.data)
        if isinstance(result, _t.Mapping):
            return self.process(result)
        if isinstance(result, Exception):
            return result

    @abstractmethod
    def process(self, result: dict) -> pydantic.BaseModel:
        ...