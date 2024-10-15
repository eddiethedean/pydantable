import typing as _t

import pydantic

from pydantable.results import dicts as dict_results

from abc import abstractmethod
from collections.abc import Generator


class DictValidator(Generator):
    @abstractmethod
    def __init__(
        self,
        data: _t.Iterable[dict_results.MappingResult],
        model: pydantic.BaseModel,
        mode: str = 'passive'
    ) -> None:
        ...