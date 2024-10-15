import typing as _t
import csv

from pydantable.results import dicts as results


class CSVDictWriter(results.MappingResultsGenerator):
    def __init__(
        self,
        data: _t.Iterator[results.MappingResult],
        csv_file: _t.IO,
        add_headers: bool = True
    ) -> None:
        self.data: _t.Iterator[results.MappingResult] = data
        self.file: _t.IO = csv_file
        self._writer: csv.DictWriter | None = None
        self.add_headers: bool = add_headers

    def process(self, result: _t.Mapping) -> _t.Mapping:
        if self._writer is None:
            self._writer = csv.DictWriter(self.file, result)
            if self.add_headers:
                self._writer.writeheader()
        self._writer.writerow(result)
        return result
        