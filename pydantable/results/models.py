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


class MappingToModelResultsGenerator(Generator):
    data: _t.Iterator[_t.Mapping]

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
        result: dicts.MappingResult = next(self.data)
        if isinstance(result, _t.Mapping):
            return self.process(result)
        if isinstance(result, Exception):
            return result

    @abstractmethod
    def process(self, result: _t.Mapping) -> pydantic.BaseModel:
        ...


class TuplesToModelResultsGenerator(Generator):
    data: _t.Iterator[_t.Sequence]
    column_names: _t.Sequence[str]

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
        values: _t.Sequence = next(self.data)
        row: dict = dict(zip(self.column_names, values))
        if isinstance(row, _t.Mapping):
            return self.process(row)
        if isinstance(row, Exception):
            return row

    @abstractmethod
    def process(self, result: _t.Mapping) -> pydantic.BaseModel:
        ...