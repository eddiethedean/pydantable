import typing as _t
import csv

import pydantic

from pydantable.results import models as results


class CSVModelWriter(results.ModelResultsGenerator):
    def __init__(
        self,
        data: _t.Iterator[results.ModelResult],
        csv_file: _t.IO,
        add_headers: bool = True
    ) -> None:
        self.data: _t.Iterator[results.ModelResult] = data
        self.file: _t.IO = csv_file
        self._writer: csv.DictWriter | None = None
        self.add_headers: bool = add_headers

    def process(self, result: pydantic.BaseModel) -> pydantic.BaseModel:
        d: dict = result.model_dump()
        if self._writer is None:
            self._writer = csv.DictWriter(self.file, d)
            if self.add_headers:
                self._writer.writeheader()
        self._writer.writerow(d)
        return result
        