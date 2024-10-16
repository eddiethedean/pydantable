import typing as _t
from dataclasses import dataclass
from collections import defaultdict

import pydantic


@dataclass
class ModelPipe(_t.Generator):
    writer_class: _t.Type

    def csv_writer(self, f):
        return self.writer_class(
            self,
            f,
            self.writer_class
        )
    
    def dict_results(self) -> _t.Generator[dict | Exception, None, None]:
        for result in self:
            if isinstance(result, pydantic.BaseModel):
                yield result.model_dump()
            if isinstance(result, Exception):
                yield result
    
    def tolist(self) -> list[pydantic.BaseModel]:
        return list(self)
    
    def todict(self) -> dict[str, list]:
        out = defaultdict(list)
        for result in self:
            if isinstance(result, pydantic.BaseModel):
                d: dict = result.model_dump()
                for key, value in d.items():
                    out[key].append(value)
        return dict(out)
    
    def tuple_results(self) -> _t.Generator[tuple | Exception, None, None]:
        for result in self:
            if isinstance(result, pydantic.BaseModel):
                d: dict = result.model_dump()
                yield tuple(d.values())
            if isinstance(result, Exception):
                yield result