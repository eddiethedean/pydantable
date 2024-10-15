from pydantable.generators.dicts.pipes import base
from pydantable.generators.dicts.validators import dicts as validators


class DictValidatorPipe(base.DictPipe, validators.DictValidator):
    def __init__(
        self,
        data,
        model,
        transformer_class,
        validator_class,
        writer_class
    ) -> None:
        base.DictPipe.__init__(
            self,
            transformer_class,
            validator_class,
            writer_class
        )
        validators.DictValidator.__init__(self, data, model)