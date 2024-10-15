from pydantable.generators.dicts.pipes import base
from pydantable.generators.dicts.writers import csv as writers


class CSVDictWriterPipe(base.DictPipe, writers.CSVDictWriter):
    def __init__(
        self,
        data,
        f,
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
        writers.CSVDictWriter.__init__(self, data, f)