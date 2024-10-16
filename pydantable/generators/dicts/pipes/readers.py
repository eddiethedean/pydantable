from pydantable.generators.dicts.pipes import base
from pydantable.generators.dicts.readers import csv as readers


class CSVDictReaderPipe(base.DictPipe, readers.CSVDictReader):
    def __init__(self,
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
        readers.CSVDictReader.__init__(self, f)