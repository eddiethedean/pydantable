from collections.abc import Mapping
import typing as _t
import csv


from pydantable.results.dictionary import MappingResult


class CSVDictWriter:
    def __init__(
        self,
        data: _t.Iterator[MappingResult],
        csv_file: 'SupportsWrite[str]',
        add_headers: bool = True
    ) -> None:
        self.data: _t.Iterator[MappingResult] = data
        self.file: 'SupportsWrite[str]' = csv_file
        self.writer: csv.DictWriter | None = None
        self.add_headers: bool = add_headers

    def __iter__(self) -> _t.Self:
        return self

    def __next__(self) -> MappingResult:
        result: MappingResult = next(self.data)
        if isinstance(result, Mapping):
            if self.writer is None:
                    self._start_writer(result)
            return self.write(result)
        
        if isinstance(result, Exception):
            return result

    def _start_writer(self, row: Mapping) -> None:
        self.writer = csv.DictWriter(self.file, row.keys())
        if self.add_headers:
            self.writer.writeheader()
            
    def write(self, row: Mapping) -> MappingResult:
        if self.writer is None:
            raise ValueError('writer is not initialized')
        try:
            self.writer.writerow(row)
        except Exception as e:
            return e
        return row