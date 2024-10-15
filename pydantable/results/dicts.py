import typing as _t
from collections.abc import Mapping, Generator
from abc import abstractmethod


DictResult = dict | Exception
MappingResult = Mapping | Exception


class MappingResultsGenerator(Generator):
    data: _t.Iterator[MappingResult]

    def send(self, ignored_arg) -> MappingResult:
        try:
            return self.next()
        except StopIteration as si:
            raise si
        except Exception as e:
            return e
        
    def throw(self, type=None, value=None, traceback=None) -> _t.NoReturn:
        raise StopIteration
    
    def next(self) -> MappingResult:
        result: MappingResult = next(self.data)
        if isinstance(result, _t.Mapping):
            return self.process(result)
        if isinstance(result, Exception):
            return result

    @abstractmethod
    def process(self, result: Mapping) -> Mapping:
        ...
        

class DictResultsGenerator(Generator):
    data: _t.Iterator[DictResult]

    def send(self, ignored_arg) -> DictResult:
        try:
            return self.next()
        except StopIteration as si:
            raise si
        except Exception as e:
            return e
        
    def throw(self, type=None, value=None, traceback=None) -> _t.NoReturn:
        raise StopIteration
    
    def next(self) -> DictResult:
        result: DictResult = next(self.data)
        if isinstance(result, _t.Mapping):
            return self.process(result)
        if isinstance(result, Exception):
            return result

    @abstractmethod
    def process(self, result: dict) -> dict:
        ...