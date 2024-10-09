from collections.abc import Mapping
import copy
import typing as _t
from abc import ABC, abstractmethod

from pydantable.results import dicts as dict_results


def suppressed_results(
        data: _t.Iterator[dict_results.MappingResult]
) -> _t.Generator[Mapping, None, None]:
    for result in data:
        if isinstance(result, Mapping):
            yield result


def raised_results(
        data: _t.Iterator[dict_results.MappingResult]
) -> _t.Generator[Mapping, None, None]:
    for result in data:
        if isinstance(result, Mapping):
            yield result
        if isinstance(result, Exception):
            raise result


class ChainBase(ABC):
    @abstractmethod
    def __init__(
        self,
        data: _t.Iterator[dict_results.MappingResult]
    ) -> None:
        self.data = data

    @abstractmethod
    def __iter__(self) -> _t.Self:
        ...

    @abstractmethod
    def __next__(self) -> dict_results.MappingResult:
        ...

    def suppressor(self) -> _t.Self:
        out = copy.copy(self)
        out.data = suppressed_results(self)
        return out

    def raiser(self) -> _t.Self:
        out = copy.copy(self)
        out.data = raised_results(self)
        return out
