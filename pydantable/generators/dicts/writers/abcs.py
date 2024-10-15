import typing as _t

from abc import abstractmethod
from collections.abc import Generator

from pydantable.results import dicts as dict_results


class CSVDictWriter(Generator):
    @abstractmethod
    def __init__(
        self,
        data: _t.Iterable[dict_results.MappingResult],
        file: _t.IO,
        mode: str = 'passive'
    ) -> None:
        ...