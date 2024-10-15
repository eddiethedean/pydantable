import typing as _t
from dataclasses import dataclass


@dataclass
class DictPipe:
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