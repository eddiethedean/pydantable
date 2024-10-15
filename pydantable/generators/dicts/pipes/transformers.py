from pydantable.generators.dicts.pipes import base
from pydantable.generators.dicts.transformers import dicts as transformers


class DictTransformerPipe(base.DictPipe, transformers.DictTransformer):
    def __init__(
        self,
        data,
        transfomer_function,
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
        transformers.DictTransformer.__init__(self, data, transfomer_function)