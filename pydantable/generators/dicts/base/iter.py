import typing as _t
from enum import StrEnum, auto

from pydantable.results import dicts as dict_results


class ReturnMode(StrEnum):
    PASSIVE = auto()
    SUPPRESS = auto()
    RAISE = auto()


class BaseIter:
    def __init__(
        self,
        data: _t.Iterator[dict_results.MappingResult],
        mode: ReturnMode = ReturnMode.PASSIVE
    ) -> None:
        self.data: _t.Iterator[dict_results.MappingResult] = data
        self.mode: ReturnMode = mode
        self.start_iter()

    def __iter__(self) -> _t.Self:
        self.start_iter()
        return self
    
    def start_iter(self) -> None:
        self._iter: _t.Iterator[dict_results.MappingResult] = iter(self.data)

    def __next__(self) -> dict_results.MappingResult:
        return next(self._iter)
    
    def filter_error(
            self,
            error: Exception
        ) -> dict_results.MappingResult:
        match self.mode:
            case ReturnMode.PASSIVE:
                return error
            case ReturnMode.SUPPRESS:
                return next(self)
            case ReturnMode.RAISE:
                raise error
            case _:
                raise ValueError('mode must be "passive", "suppress" or "raise"')