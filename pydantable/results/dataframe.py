from abc import abstractmethod
import typing as _t

import pydantic
import tinytim as tt

from pydantable.results import models as results


class DataFrame(_t.Protocol):
    def keys(self) -> _t.Iterator[str]:
        ...

    def __iter__(self) -> _t.Iterable[str]:
         ...

    def __getitem__(self, key: str) -> _t.Collection:
        ...


class DataFrameModelResultsGenerator(_t.Generator):
    data: _t.Generator[tuple[int, dict], None, None]

    def send(self, ignored_arg) -> results.ModelResult:
        try:
            return self.next()
        except StopIteration as si:
            raise si
        except Exception as e:
            return e
        
    def throw(self, type=None, value=None, traceback=None) -> _t.NoReturn:
        raise StopIteration
    
    def next(self) -> results.ModelResult:
        i, result = next(self.data)
        return self.process(result)


    @abstractmethod
    def process(self, result: dict) -> pydantic.BaseModel:
        ...