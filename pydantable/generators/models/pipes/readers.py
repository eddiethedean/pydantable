import typing as _t

from pydantable.generators.models.pipes import base
from pydantable.generators.models.readers import csv as readers

import pydantic


class CSVModelReaderPipe(base.ModelPipe, readers.CSVModelReader):
    def __init__(self,
        f: _t.IO,
        model: pydantic.BaseModel,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        readers.CSVModelReader.__init__(self, f, model)