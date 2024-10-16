import typing as _t
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class DictPipe(_t.Generator):
    transformer_class: _t.Type
    validator_class: _t.Type
    writer_class: _t.Type
        
    def transformer(self, transfomer_function):
        return self.transformer_class(
            self,
            transfomer_function,
            self.transformer_class,
            self.validator_class,
            self.writer_class
        )

    def validator(self, model):
        return self.validator_class(
            self,
            model,
            self.transformer_class,
            self.validator_class,
            self.writer_class
        )

    def csv_writer(self, f):
        return self.writer_class(
            self,
            f,
            self.transformer_class,
            self.validator_class,
            self.writer_class
        )
    
    def tolist(self) -> list[dict]:
        return list(self)
    
    def todict(self) -> dict[str, list]:
        out = defaultdict(list)
        for result in self:
            if isinstance(result, _t.Mapping):
                for key, value in result.items():
                    out[key].append(value)
        return dict(out)
    
    def totuples(self) -> list[tuple]:
        out = []
        for result in self:
            if isinstance(result, _t.Mapping):
                out.append(tuple(result.values()))
        return out