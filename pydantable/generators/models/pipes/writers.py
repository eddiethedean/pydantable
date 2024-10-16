from pydantable.generators.models.pipes import base
from pydantable.generators.models.writers import csv as writers


class CSVModelWriterPipe(base.ModelPipe, writers.CSVModelWriter):
    def __init__(
        self,
        data,
        f,
        writer_class
    ) -> None:
        base.ModelPipe.__init__(
            self,
            writer_class
        )
        writers.CSVModelWriter.__init__(self, data, f)